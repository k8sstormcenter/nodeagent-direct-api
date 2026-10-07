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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeServer struct {
	directv1.UnimplementedAlertStreamServer
	mu         sync.Mutex
	epoch      string
	alerts     []*directv1.Alert
	oldest     uint64
	wantTok    string
	subs       []*directv1.SubscribeRequest
	closeAfter int
	dials      atomic.Int32
}

func (f *fakeServer) Subscribe(req *directv1.SubscribeRequest, stream directv1.AlertStream_SubscribeServer) error {
	f.dials.Add(1)
	if f.wantTok != "" {
		md, _ := metadata.FromIncomingContext(stream.Context())
		if v := md.Get("authorization"); len(v) != 1 || v[0] != "Bearer "+f.wantTok {
			return status.Error(codes.Unauthenticated, "bad token")
		}
	}
	f.mu.Lock()
	f.subs = append(f.subs, req)
	alerts, oldest, epoch := f.alerts, f.oldest, f.epoch
	f.mu.Unlock()
	since := req.GetSinceSeq()
	if req.GetProducerEpoch() != "" && req.GetProducerEpoch() != epoch {
		since = 0
	}
	batch := &directv1.AlertBatch{ProducerEpoch: epoch, SentAtNs: time.Now().UnixNano(), OldestSeq: oldest}
	if since+1 < oldest {
		batch.Gap = true
		batch.DroppedSinceLast = oldest - since - 1
	}
	sent := 0
	for _, a := range alerts {
		if a.GetSeq() > since {
			batch.Alerts = append(batch.Alerts, a)
			sent++
			if f.closeAfter > 0 && sent >= f.closeAfter {
				break
			}
		}
	}
	if err := stream.Send(batch); err != nil {
		return err
	}
	if f.closeAfter > 0 {
		return nil
	}
	<-stream.Context().Done()
	return nil
}

func mk(seqs ...uint64) []*directv1.Alert {
	out := make([]*directv1.Alert, 0, len(seqs))
	for _, s := range seqs {
		out = append(out, &directv1.Alert{Seq: s, RuleId: "R1017", EventTimeNs: 1790619389554466071})
	}
	return out
}

func start(t *testing.T, f *fakeServer) *Consumer {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	directv1.RegisterAlertStreamServer(srv, f)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	c, err := New(Config{
		Addr: "passthrough:///buf", Hostname: "node-a", Tokens: StaticToken("tok"),
		Cursor: NewMemoryCursor(), Sink: &MemorySink{},
		MinBackoff: 20 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func TestNewRequiresDeps(t *testing.T) {
	if _, err := New(Config{Addr: "x"}); !errors.Is(err, ErrConfig) {
		t.Fatalf("got %v", err)
	}
}

func TestSaveOnlyAfterWrite(t *testing.T) {
	f := &fakeServer{epoch: "e1", alerts: mk(1, 2, 3), oldest: 1}
	c := start(t, f)
	sink := c.cfg.Sink.(*MemorySink)
	cur := c.cfg.Cursor.(*MemoryCursor)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	waitFor(t, func() bool { return c.Stats.LastSeq.Load() == 3 })
	if got := sink.Seqs(); len(got) != 3 {
		t.Fatalf("sink got %v", got)
	}
	ep, seq, _ := cur.Load(ctx, "node-a")
	if ep != "e1" || seq != 3 {
		t.Fatalf("cursor %s/%d", ep, seq)
	}
}

func TestSinkFailureBlocksCursor(t *testing.T) {
	f := &fakeServer{epoch: "e1", alerts: mk(1, 2), oldest: 1}
	c := start(t, f)
	sink := c.cfg.Sink.(*MemorySink)
	cur := c.cfg.Cursor.(*MemoryCursor)
	var fails atomic.Int32
	sink.Fail = func(*directv1.AlertBatch) error {
		if fails.Add(1) <= 2 {
			return errors.New("ch down")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	waitFor(t, func() bool { return c.Stats.SinkFailures.Load() == 2 })
	if _, seq, _ := cur.Load(ctx, "node-a"); seq != 0 {
		t.Fatalf("cursor advanced to %d on sink failure", seq)
	}
	waitFor(t, func() bool { return c.Stats.LastSeq.Load() == 2 })
	if cur.Saves != 1 {
		t.Fatalf("saves %d", cur.Saves)
	}
	if c.Stats.Reconnects.Load() < 2 {
		t.Fatalf("reconnects %d", c.Stats.Reconnects.Load())
	}
}

func TestResumeFromCursor(t *testing.T) {
	f := &fakeServer{epoch: "e1", alerts: mk(1, 2, 3, 4), oldest: 1}
	c := start(t, f)
	c.cfg.Cursor.Save(context.Background(), "node-a", "e1", 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	waitFor(t, func() bool { return c.Stats.LastSeq.Load() == 4 })
	if got := c.cfg.Sink.(*MemorySink).Seqs(); len(got) != 2 || got[0] != 3 {
		t.Fatalf("sink got %v", got)
	}
	if f.subs[0].GetSinceSeq() != 2 || f.subs[0].GetProducerEpoch() != "e1" {
		t.Fatalf("subscribe %v", f.subs[0])
	}
}

func TestGapAndDroppedCounted(t *testing.T) {
	f := &fakeServer{epoch: "e1", alerts: mk(50, 51), oldest: 50}
	c := start(t, f)
	c.cfg.Cursor.Save(context.Background(), "node-a", "e1", 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	waitFor(t, func() bool { return c.Stats.LastSeq.Load() == 51 })
	if c.Stats.Gaps.Load() != 1 || c.Stats.Dropped.Load() != 39 {
		t.Fatalf("gaps %d dropped %d", c.Stats.Gaps.Load(), c.Stats.Dropped.Load())
	}
}

func TestEpochChangeCounted(t *testing.T) {
	f := &fakeServer{epoch: "e2", alerts: mk(1), oldest: 1}
	c := start(t, f)
	c.cfg.Cursor.Save(context.Background(), "node-a", "e1", 900)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	waitFor(t, func() bool { return c.Stats.LastSeq.Load() == 1 })
	if c.Stats.ProducerRestarts.Load() != 1 {
		t.Fatalf("restarts %d", c.Stats.ProducerRestarts.Load())
	}
	if ep, _ := c.Cursor(); ep != "e2" {
		t.Fatalf("epoch %s", ep)
	}
}

func TestUnauthenticatedCountedAndRetried(t *testing.T) {
	f := &fakeServer{epoch: "e1", alerts: mk(1), oldest: 1, wantTok: "right"}
	c := start(t, f)
	var n atomic.Int32
	c.cfg.Tokens = TokenFunc(func(context.Context) (string, error) {
		if n.Add(1) < 3 {
			return "wrong", nil
		}
		return "right", nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	waitFor(t, func() bool { return c.Stats.LastSeq.Load() == 1 })
	if c.Stats.AuthFailures.Load() != 2 {
		t.Fatalf("auth failures %d", c.Stats.AuthFailures.Load())
	}
}

func TestReconnectResumesAfterServerClose(t *testing.T) {
	f := &fakeServer{epoch: "e1", alerts: mk(1, 2, 3), oldest: 1, closeAfter: 1}
	c := start(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	waitFor(t, func() bool { return c.Stats.LastSeq.Load() == 3 })
	if got := c.cfg.Sink.(*MemorySink).Seqs(); len(got) != 3 {
		t.Fatalf("sink got %v", got)
	}
	if f.dials.Load() < 3 {
		t.Fatalf("dials %d", f.dials.Load())
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	f := &fakeServer{epoch: "e1", oldest: 0}
	c := start(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	waitFor(t, func() bool { return c.Stats.Batches.Load() == 1 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return")
	}
}
