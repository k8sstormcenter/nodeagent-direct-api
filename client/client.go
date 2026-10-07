package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	directv1 "github.com/k8sstormcenter/nodeagent-direct-api/nodeagent/direct/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

type CursorStore interface {
	Load(ctx context.Context, hostname string) (epoch string, seq uint64, err error)
	Save(ctx context.Context, hostname, epoch string, seq uint64) error
}

type Sink interface {
	Write(ctx context.Context, hostname string, batch *directv1.AlertBatch) error
}

type Logger func(format string, args ...any)

type Config struct {
	Addr          string
	Hostname      string
	Tokens        TokenSource
	Cursor        CursorStore
	Sink          Sink
	MaxBatch      uint32
	MinBackoff    time.Duration
	MaxBackoff    time.Duration
	TLSSkipVerify bool
	DialOptions   []grpc.DialOption
	Log           Logger
}

type Stats struct {
	Batches          atomic.Uint64
	Alerts           atomic.Uint64
	Gaps             atomic.Uint64
	Dropped          atomic.Uint64
	ProducerRestarts atomic.Uint64
	Reconnects       atomic.Uint64
	AuthFailures     atomic.Uint64
	SinkFailures     atomic.Uint64
	LastSeq          atomic.Uint64
	LastSentAtNs     atomic.Int64
}

type Consumer struct {
	cfg   Config
	Stats Stats
	mu    sync.Mutex
	epoch string
	seq   uint64
}

var ErrConfig = errors.New("nodeagent-direct client: Addr, Hostname, Tokens, Cursor and Sink are required")

func New(cfg Config) (*Consumer, error) {
	if cfg.Addr == "" || cfg.Hostname == "" || cfg.Tokens == nil || cfg.Cursor == nil || cfg.Sink == nil {
		return nil, ErrConfig
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
	return &Consumer{cfg: cfg}, nil
}

func (c *Consumer) Cursor() (epoch string, seq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch, c.seq
}

func (c *Consumer) Run(ctx context.Context) error {
	epoch, seq, err := c.cfg.Cursor.Load(ctx, c.cfg.Hostname)
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
		c.cfg.Log("nodeagent-direct: stream ended (%v); reconnecting in %s", err, backoff)
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

func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

func (c *Consumer) dial() (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: c.cfg.TLSSkipVerify, MinVersion: tls.VersionTLS12}))}
	opts = append(opts, c.cfg.DialOptions...)
	return grpc.NewClient(c.cfg.Addr, opts...)
}

func (c *Consumer) runOnce(ctx context.Context) error {
	tok, err := c.cfg.Tokens.Token(ctx)
	if err != nil {
		return fmt.Errorf("token: %w", err)
	}
	conn, err := c.dial()
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	epoch, seq := c.Cursor()
	sctx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
	stream, err := directv1.NewAlertStreamClient(conn).Subscribe(sctx, &directv1.SubscribeRequest{SinceSeq: seq, ProducerEpoch: epoch, MaxBatch: c.cfg.MaxBatch})
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
	}
}

func (c *Consumer) handle(ctx context.Context, batch *directv1.AlertBatch) error {
	c.Stats.Batches.Add(1)
	c.Stats.LastSentAtNs.Store(batch.GetSentAtNs())
	epoch, seq := c.Cursor()
	if epoch != "" && batch.GetProducerEpoch() != epoch {
		c.Stats.ProducerRestarts.Add(1)
		c.cfg.Log("nodeagent-direct: producer restarted (epoch %s -> %s); alerts of the old epoch after seq %d are unobservable", epoch, batch.GetProducerEpoch(), seq)
	}
	if batch.GetGap() {
		c.Stats.Gaps.Add(1)
	}
	if batch.GetDroppedSinceLast() > 0 {
		c.Stats.Dropped.Add(batch.GetDroppedSinceLast())
		c.cfg.Log("nodeagent-direct: %d alerts evicted before delivery (oldest retained %d)", batch.GetDroppedSinceLast(), batch.GetOldestSeq())
	}
	last := seq
	if n := len(batch.GetAlerts()); n > 0 {
		if err := c.cfg.Sink.Write(ctx, c.cfg.Hostname, batch); err != nil {
			c.Stats.SinkFailures.Add(1)
			return fmt.Errorf("sink: %w", err)
		}
		c.Stats.Alerts.Add(uint64(n))
		last = batch.GetAlerts()[n-1].GetSeq()
	} else if batch.GetGap() && batch.GetOldestSeq() > 0 {
		last = batch.GetOldestSeq() - 1
	}
	if err := c.cfg.Cursor.Save(ctx, c.cfg.Hostname, batch.GetProducerEpoch(), last); err != nil {
		return fmt.Errorf("cursor save: %w", err)
	}
	c.mu.Lock()
	c.epoch, c.seq = batch.GetProducerEpoch(), last
	c.mu.Unlock()
	c.Stats.LastSeq.Store(last)
	return nil
}
