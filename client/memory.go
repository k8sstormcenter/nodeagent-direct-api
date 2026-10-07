package client

import (
	"context"
	"sync"

	directv1 "github.com/k8sstormcenter/nodeagent-direct-api/nodeagent/direct/v1"
)

type StaticToken string

func (s StaticToken) Token(context.Context) (string, error) { return string(s), nil }

type TokenFunc func(ctx context.Context) (string, error)

func (f TokenFunc) Token(ctx context.Context) (string, error) { return f(ctx) }

type MemoryCursor struct {
	mu    sync.Mutex
	epoch map[string]string
	seq   map[string]uint64
	Saves int
}

func NewMemoryCursor() *MemoryCursor {
	return &MemoryCursor{epoch: map[string]string{}, seq: map[string]uint64{}}
}

func (m *MemoryCursor) Load(_ context.Context, hostname string) (string, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.epoch[hostname], m.seq[hostname], nil
}

func (m *MemoryCursor) Save(_ context.Context, hostname, epoch string, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch[hostname], m.seq[hostname] = epoch, seq
	m.Saves++
	return nil
}

type MemorySink struct {
	mu      sync.Mutex
	Alerts  []*directv1.Alert
	Batches int
	Fail    func(batch *directv1.AlertBatch) error
}

func (s *MemorySink) Write(_ context.Context, _ string, batch *directv1.AlertBatch) error {
	if s.Fail != nil {
		if err := s.Fail(batch); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Batches++
	s.Alerts = append(s.Alerts, batch.GetAlerts()...)
	return nil
}

func (s *MemorySink) Seqs() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint64, 0, len(s.Alerts))
	for _, a := range s.Alerts {
		out = append(out, a.GetSeq())
	}
	return out
}
