package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	directv1 "github.com/k8sstormcenter/nodeagent-direct-api/nodeagent/direct/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type fakeProfileServer struct {
	directv1.UnimplementedAlertStreamServer
	mu        sync.Mutex
	epoch     string
	snapshots []*directv1.ProfileSnapshot
	acks      []*directv1.AckRequest
	dials     atomic.Int32
}

func (f *fakeProfileServer) SubscribeProfiles(req *directv1.SubscribeRequest, stream directv1.AlertStream_SubscribeProfilesServer) error {
	f.dials.Add(1)
	f.mu.Lock()
	snaps, epoch := f.snapshots, f.epoch
	f.mu.Unlock()
	since := req.GetSinceSeq()
	if req.GetProducerEpoch() != "" && req.GetProducerEpoch() != epoch {
		since = 0
	}
	batch := &directv1.ProfileSnapshotBatch{ProducerEpoch: epoch, SentAtNs: time.Now().UnixNano(), OldestSeq: 1}
	for _, s := range snaps {
		if s.GetSeq() > since {
			batch.Snapshots = append(batch.Snapshots, s)
		}
	}
	if err := stream.Send(batch); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func (f *fakeProfileServer) AckSnapshots(_ context.Context, req *directv1.AckRequest) (*directv1.AckResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, req)
	return &directv1.AckResponse{AckedUptoSeq: req.GetUptoSeq(), ProducerEpoch: req.GetProducerEpoch()}, nil
}

func (f *fakeProfileServer) lastAck() *directv1.AckRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.acks) == 0 {
		return nil
	}
	return f.acks[len(f.acks)-1]
}

type memProfileSink struct {
	mu    sync.Mutex
	seqs  []uint64
	Fail  func(*directv1.ProfileSnapshotBatch) error
	calls int
}

func (s *memProfileSink) Write(_ context.Context, _ string, b *directv1.ProfileSnapshotBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.Fail != nil {
		if err := s.Fail(b); err != nil {
			return err
		}
	}
	for _, sn := range b.GetSnapshots() {
		s.seqs = append(s.seqs, sn.GetSeq())
	}
	return nil
}

func (s *memProfileSink) Seqs() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.seqs...)
}

func mkSnaps(seqs ...uint64) []*directv1.ProfileSnapshot {
	out := make([]*directv1.ProfileSnapshot, 0, len(seqs))
	for _, s := range seqs {
		out = append(out, &directv1.ProfileSnapshot{Seq: s, ProfileName: "node-a-host-1111-2222", ChunkName: "node-a-host-1111-2222-c", ProfileKind: "node", Spec: []byte(`{"opens":[]}`)})
	}
	return out
}

func startProfiles(t *testing.T, f *fakeProfileServer, sink *memProfileSink, cur *MemoryCursor) *ProfileConsumer {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	directv1.RegisterAlertStreamServer(srv, f)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	c, err := NewProfileConsumer(ProfileConfig{
		Addr: "passthrough:///buf", Hostname: "node-a", Tokens: StaticToken("tok"), Cursor: cur, Sink: sink,
		MinBackoff: 20 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestProfileConsumer_WritesThenSavesThenAcks(t *testing.T) {
	f := &fakeProfileServer{epoch: "e1", snapshots: mkSnaps(1, 2, 3)}
	sink, cur := &memProfileSink{}, NewMemoryCursor()
	c := startProfiles(t, f, sink, cur)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	waitFor(t, func() bool { return c.Stats.Acked.Load() == 3 })
	if got := sink.Seqs(); len(got) != 3 {
		t.Fatalf("sink %v", got)
	}
	ep, seq, _ := cur.Load(ctx, "node-a/profiles")
	if ep != "e1" || seq != 3 {
		t.Fatalf("cursor %s/%d", ep, seq)
	}
	ack := f.lastAck()
	if ack == nil || ack.GetUptoSeq() != 3 || ack.GetProducerEpoch() != "e1" || ack.GetHostname() != "node-a" {
		t.Fatalf("ack %v", ack)
	}
}

func TestProfileConsumer_NoAckWhenSinkFails(t *testing.T) {
	f := &fakeProfileServer{epoch: "e1", snapshots: mkSnaps(1, 2)}
	sink, cur := &memProfileSink{}, NewMemoryCursor()
	var n atomic.Int32
	sink.Fail = func(*directv1.ProfileSnapshotBatch) error {
		if n.Add(1) <= 2 {
			return errors.New("clickhouse down")
		}
		return nil
	}
	c := startProfiles(t, f, sink, cur)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	waitFor(t, func() bool { return c.Stats.SinkFailures.Load() == 2 })
	if f.lastAck() != nil {
		t.Fatal("ack sent before any write succeeded")
	}
	if _, seq, _ := cur.Load(ctx, "node-a/profiles"); seq != 0 {
		t.Fatalf("cursor advanced to %d", seq)
	}
	waitFor(t, func() bool { return c.Stats.Acked.Load() == 2 })
	if cur.Saves != 1 {
		t.Fatalf("saves %d", cur.Saves)
	}
}

func TestProfileConsumer_EpochChangeAndSeparateCursorKey(t *testing.T) {
	f := &fakeProfileServer{epoch: "e2", snapshots: mkSnaps(1)}
	sink, cur := &memProfileSink{}, NewMemoryCursor()
	cur.Save(context.Background(), "node-a/profiles", "e1", 50)
	cur.Save(context.Background(), "node-a", "e1", 999)
	c := startProfiles(t, f, sink, cur)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	waitFor(t, func() bool { return c.Stats.Acked.Load() == 1 })
	if c.Stats.ProducerRestarts.Load() != 1 {
		t.Fatalf("restarts %d", c.Stats.ProducerRestarts.Load())
	}
	if _, seq, _ := cur.Load(ctx, "node-a"); seq != 999 {
		t.Fatal("the alert cursor must not be touched by the profile consumer")
	}
}
