package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	directv1 "github.com/k8sstormcenter/nodeagent-direct-api/nodeagent/direct/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ProfileSink interface {
	Write(ctx context.Context, hostname string, batch *directv1.ProfileSnapshotBatch) error
}

type ProfileConfig struct {
	Addr          string
	Hostname      string
	Tokens        TokenSource
	Cursor        CursorStore
	Sink          ProfileSink
	MaxBatch      uint32
	MinBackoff    time.Duration
	MaxBackoff    time.Duration
	TLSSkipVerify bool
	DialOptions   []grpc.DialOption
	Log           Logger
	CursorKey     string
}

type ProfileStats struct {
	Batches          atomic.Uint64
	Snapshots        atomic.Uint64
	Gaps             atomic.Uint64
	Dropped          atomic.Uint64
	ProducerRestarts atomic.Uint64
	Reconnects       atomic.Uint64
	AuthFailures     atomic.Uint64
	SinkFailures     atomic.Uint64
	AckFailures      atomic.Uint64
	Acked            atomic.Uint64
	LastSeq          atomic.Uint64
}

type ProfileConsumer struct {
	cfg   ProfileConfig
	Stats ProfileStats
	mu    sync.Mutex
	epoch string
	seq   uint64
}

var ErrProfileConfig = errors.New("nodeagent-direct profile client: Addr, Hostname, Tokens, Cursor and Sink are required")

const defaultProfileCursorSuffix = "/profiles"

func NewProfileConsumer(cfg ProfileConfig) (*ProfileConsumer, error) {
	if cfg.Addr == "" || cfg.Hostname == "" || cfg.Tokens == nil || cfg.Cursor == nil || cfg.Sink == nil {
		return nil, ErrProfileConfig
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = func(string, ...any) {}
	}
	if cfg.CursorKey == "" {
		cfg.CursorKey = cfg.Hostname + defaultProfileCursorSuffix
	}
	return &ProfileConsumer{cfg: cfg}, nil
}

func (c *ProfileConsumer) Cursor() (epoch string, seq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch, c.seq
}

func (c *ProfileConsumer) Run(ctx context.Context) error {
	epoch, seq, err := c.cfg.Cursor.Load(ctx, c.cfg.CursorKey)
	if err != nil {
		return fmt.Errorf("cursor load: %w", err)
	}
	c.mu.Lock()
	c.epoch, c.seq = epoch, seq
	c.mu.Unlock()
	backoff := c.cfg.MinBackoff
	for {
		err := c.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if status.Code(err) == codes.Unauthenticated {
			c.Stats.AuthFailures.Add(1)
		}
		c.Stats.Reconnects.Add(1)
		c.cfg.Log("nodeagent-direct profiles: stream ended (%v); reconnecting in %s", err, backoff)
		select {
		case <-time.After(jitter(backoff)):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff *= 2
		if backoff > c.cfg.MaxBackoff {
			backoff = c.cfg.MaxBackoff
		}
	}
}

func (c *ProfileConsumer) runOnce(ctx context.Context) error {
	tok, err := c.cfg.Tokens.Token(ctx)
	if err != nil {
		return fmt.Errorf("token: %w", err)
	}
	conn, err := dial(c.cfg.Addr, c.cfg.TLSSkipVerify, c.cfg.DialOptions)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	epoch, seq := c.Cursor()
	sctx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
	cl := directv1.NewAlertStreamClient(conn)
	stream, err := cl.SubscribeProfiles(sctx, &directv1.SubscribeRequest{SinceSeq: seq, ProducerEpoch: epoch, MaxBatch: c.cfg.MaxBatch})
	if err != nil {
		return err
	}
	for {
		batch, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := c.handle(ctx, batch); err != nil {
			return err
		}
		if e, s := c.Cursor(); s > 0 {
			if _, err := cl.AckSnapshots(sctx, &directv1.AckRequest{Hostname: c.cfg.Hostname, ProducerEpoch: e, UptoSeq: s}); err != nil {
				c.Stats.AckFailures.Add(1)
				c.cfg.Log("nodeagent-direct profiles: ack to %d failed: %v", s, err)
			} else {
				c.Stats.Acked.Store(s)
			}
		}
	}
}

func (c *ProfileConsumer) handle(ctx context.Context, batch *directv1.ProfileSnapshotBatch) error {
	c.Stats.Batches.Add(1)
	epoch, seq := c.Cursor()
	if epoch != "" && batch.GetProducerEpoch() != epoch {
		c.Stats.ProducerRestarts.Add(1)
		c.cfg.Log("nodeagent-direct profiles: producer restarted (epoch %s -> %s); snapshots of the old epoch after seq %d are unobservable", epoch, batch.GetProducerEpoch(), seq)
	}
	if batch.GetGap() {
		c.Stats.Gaps.Add(1)
	}
	if batch.GetDroppedSinceLast() > 0 {
		c.Stats.Dropped.Add(batch.GetDroppedSinceLast())
		c.cfg.Log("nodeagent-direct profiles: %d snapshots evicted before delivery (oldest retained %d)", batch.GetDroppedSinceLast(), batch.GetOldestSeq())
	}
	last := seq
	if n := len(batch.GetSnapshots()); n > 0 {
		if err := c.cfg.Sink.Write(ctx, c.cfg.Hostname, batch); err != nil {
			c.Stats.SinkFailures.Add(1)
			return fmt.Errorf("sink: %w", err)
		}
		c.Stats.Snapshots.Add(uint64(n))
		last = batch.GetSnapshots()[n-1].GetSeq()
	} else if batch.GetGap() && batch.GetOldestSeq() > 0 {
		last = batch.GetOldestSeq() - 1
	}
	if err := c.cfg.Cursor.Save(ctx, c.cfg.CursorKey, batch.GetProducerEpoch(), last); err != nil {
		return fmt.Errorf("cursor save: %w", err)
	}
	c.mu.Lock()
	c.epoch, c.seq = batch.GetProducerEpoch(), last
	c.mu.Unlock()
	c.Stats.LastSeq.Store(last)
	return nil
}
