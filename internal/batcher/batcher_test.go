package batcher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
)

type mockSender struct {
	mu        sync.Mutex
	payloads  [][]bulk_transformer.Data
	err       error
	failTimes int
	calls     int
}

func (m *mockSender) Bulk(ctx context.Context, payload bulk_transformer.DataPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calls++

	if m.err != nil && (m.failTimes == 0 || m.calls <= m.failTimes) {
		return m.err
	}

	cp := make([]bulk_transformer.Data, len(payload))
	copy(cp, payload)
	m.payloads = append(m.payloads, cp)

	return nil
}

type fakeMonitoring struct {
	mu       sync.Mutex
	retries  int
	failures int
}

func (f *fakeMonitoring) IncFlushRetry() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retries++
}

func (f *fakeMonitoring) IncFlushFailure() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures++
}

func data(v string) bulk_transformer.Data {
	return bulk_transformer.Data{
		ID:     v,
		Action: bulk_transformer.Index,
		Body:   []byte(v),
	}
}

func TestBatcher_AddCommitFlush(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Commit()
	b.Commit()
	_ = b.Flush()

	if len(sender.payloads) != 1 {
		t.Fatalf("expected 1 bulk call, got %d", len(sender.payloads))
	}

	got := sender.payloads[0]
	if len(got) != 2 {
		t.Fatalf("expected 2 items, got %d", len(got))
	}
}

func TestBatcher_FlushWithoutCommit(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("a"))
	_ = b.Flush()

	if len(sender.payloads) != 0 {
		t.Fatalf("expected no flush")
	}
}

func TestBatcher_PartialCommitFlush(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Add(data("c"))

	b.Commit()
	b.Commit()

	_ = b.Flush()

	if len(sender.payloads) != 1 {
		t.Fatalf("expected 1 flush")
	}

	if len(sender.payloads[0]) != 2 {
		t.Fatalf("expected 2 committed items")
	}

	if len(b.buffer) != 1 {
		t.Fatalf("expected 1 remaining item, got %d", len(b.buffer))
	}
}

func TestBatcher_RollbackLastUncommitted(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Rollback()

	if len(b.buffer) != 1 {
		t.Fatalf("expected 1 item after rollback, got %d", len(b.buffer))
	}
}

func TestBatcher_RollbackCommittedDoesNothing(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("a"))
	b.Commit()
	b.Rollback()

	if len(b.buffer) != 1 {
		t.Fatalf("committed item must not rollback")
	}
}

func TestBatcher_FlushFailureKeepsBuffer(t *testing.T) {
	sender := &mockSender{
		err: errors.New("bulk failed"),
	}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("a"))
	b.Commit()

	if err := b.Flush(); err == nil {
		t.Fatalf("expected error from failed flush")
	}

	if len(b.buffer) != 1 {
		t.Fatalf("buffer should remain on failed flush")
	}

	if b.lastCommitted != 0 {
		t.Fatalf("commit pointer should remain")
	}
}

func TestBatcher_FlushRetriesAndSucceeds(t *testing.T) {
	sender := &mockSender{err: errors.New("timeout"), failTimes: 2}
	monitoring := &fakeMonitoring{}
	b := NewBatcher(context.Background(), Config{
		BufferSize:   10,
		MaxRetries:   3,
		RetryTimeout: 10 * time.Millisecond,
	}, monitoring, nil, sender)

	b.Add(data("a"))
	b.Commit()

	if err := b.Flush(); err != nil {
		t.Fatalf("expected flush to succeed after retries, got: %v", err)
	}

	if len(sender.payloads) != 1 {
		t.Fatalf("expected 1 successful bulk call, got %d", len(sender.payloads))
	}

	if sender.calls != 3 {
		t.Fatalf("expected 3 attempts (2 failures + 1 success), got %d", sender.calls)
	}

	monitoring.mu.Lock()
	retries := monitoring.retries
	failures := monitoring.failures
	monitoring.mu.Unlock()

	if retries != 2 {
		t.Fatalf("expected 2 retries reported, got %d", retries)
	}
	if failures != 0 {
		t.Fatalf("expected no failure reported after eventual success, got %d", failures)
	}

	if len(b.buffer) != 0 {
		t.Fatalf("buffer should be drained after successful flush")
	}
}

func TestBatcher_FlushExhaustsRetries(t *testing.T) {
	sender := &mockSender{err: errors.New("timeout"), failTimes: 100}
	monitoring := &fakeMonitoring{}
	b := NewBatcher(context.Background(), Config{
		BufferSize:   10,
		MaxRetries:   2,
		RetryTimeout: 5 * time.Millisecond,
	}, monitoring, nil, sender)

	b.Add(data("a"))
	b.Commit()

	if err := b.Flush(); err == nil {
		t.Fatalf("expected error after exhausting retries")
	}

	if sender.calls != 3 {
		t.Fatalf("expected 3 attempts (1 + 2 retries), got %d", sender.calls)
	}

	if len(b.buffer) != 1 {
		t.Fatalf("buffer should remain after exhausted retries")
	}

	monitoring.mu.Lock()
	retries := monitoring.retries
	failures := monitoring.failures
	monitoring.mu.Unlock()

	if retries != 2 {
		t.Fatalf("expected 2 retries reported, got %d", retries)
	}
	if failures != 1 {
		t.Fatalf("expected 1 failure reported, got %d", failures)
	}
}

func TestBatcher_FlushNoRetriesByDefault(t *testing.T) {
	sender := &mockSender{err: errors.New("timeout"), failTimes: 100}
	b := NewBatcher(context.Background(), Config{BufferSize: 10}, nil, nil, sender)

	b.Add(data("a"))
	b.Commit()

	if err := b.Flush(); err == nil {
		t.Fatalf("expected error")
	}

	if sender.calls != 1 {
		t.Fatalf("expected a single attempt when MaxRetries is 0, got %d", sender.calls)
	}
}

func TestBatcher_FlushRetryStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sender := &mockSender{err: errors.New("timeout"), failTimes: 100}
	b := NewBatcher(ctx, Config{
		BufferSize:   10,
		MaxRetries:   5,
		RetryTimeout: 200 * time.Millisecond,
	}, nil, nil, sender)

	b.Add(data("a"))
	b.Commit()

	done := make(chan error, 1)
	go func() {
		done <- b.Flush()
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected error after context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatalf("Flush did not return after context cancellation")
	}
}

func TestBatcher_AutoFlushOnBufferFull(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 2, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("a"))
	b.Commit()

	b.Add(data("b"))
	b.Commit()

	b.Add(data("c"))

	if len(sender.payloads) != 1 {
		t.Fatalf("expected auto flush")
	}

	if len(sender.payloads[0]) != 2 {
		t.Fatalf("expected 2 flushed items")
	}

	if len(b.buffer) != 1 {
		t.Fatalf("expected c to remain in buffer")
	}
}

func TestBatcher_MultipleFlushes(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Commit()
	b.Commit()
	_ = b.Flush()

	b.Add(data("c"))
	b.Commit()
	_ = b.Flush()

	if len(sender.payloads) != 2 {
		t.Fatalf("expected 2 flushes")
	}

	if len(sender.payloads[0]) != 2 {
		t.Fatalf("first flush wrong")
	}

	if len(sender.payloads[1]) != 1 {
		t.Fatalf("second flush wrong")
	}
}

func TestBatcher_CommitMoreThanBuffer(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("a"))
	b.Commit()
	b.Commit()
	b.Commit()

	_ = b.Flush()

	if len(sender.payloads) != 1 {
		t.Fatalf("expected single flush")
	}

	if len(sender.payloads[0]) != 1 {
		t.Fatalf("must flush only existing item")
	}
}

func TestBatcher_FlushEmpty(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	_ = b.Flush()

	if len(sender.payloads) != 0 {
		t.Fatalf("expected no payload")
	}
}

func TestBatcher_RollbackEmpty(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	b.Rollback()

	if len(b.buffer) != 0 {
		t.Fatalf("buffer should remain empty")
	}
}

func TestBatcher_AutoFlushOnBufferFull_VerifyData(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 2, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("x"))
	b.Commit()
	b.Add(data("y"))
	b.Commit()
	b.Add(data("z"))

	if len(sender.payloads) != 1 {
		t.Fatalf("expected 1 auto flush, got %d", len(sender.payloads))
	}
	got := sender.payloads[0]
	if len(got) != 2 {
		t.Fatalf("expected 2 flushed items, got %d", len(got))
	}
	if got[0].ID != "x" || got[1].ID != "y" {
		t.Fatalf("unexpected flushed items: %v", got)
	}
	if len(b.buffer) != 1 || b.buffer[0].ID != "z" {
		t.Fatalf("expected z to remain in buffer")
	}
}

func TestBatcher_AutoFlushOnTimeout(t *testing.T) {
	sender := &mockSender{}
	ctx := context.Background()
	b := NewBatcher(ctx, Config{BufferSize: 100, FlushTimeout: 50 * time.Millisecond}, nil, nil, sender)

	b.Add(data("a"))
	b.Commit()

	time.Sleep(400 * time.Millisecond)

	sender.mu.Lock()
	n := len(sender.payloads)
	sender.mu.Unlock()

	if n != 1 {
		t.Fatalf("expected 1 flush by timeout, got %d", n)
	}
	if sender.payloads[0][0].ID != "a" {
		t.Fatalf("unexpected payload: %v", sender.payloads[0])
	}
}

func TestBatcher_TimeoutResetAfterSizeFlush(t *testing.T) {
	sender := &mockSender{}
	ctx := context.Background()
	b := NewBatcher(ctx, Config{BufferSize: 2, FlushTimeout: 300 * time.Millisecond}, nil, nil, sender)

	time.Sleep(100 * time.Millisecond)

	b.Add(data("p"))
	b.Commit()
	b.Add(data("q"))
	b.Commit()
	b.Add(data("r"))
	b.Commit()

	if len(sender.payloads) != 1 {
		t.Fatalf("expected size flush, got %d", len(sender.payloads))
	}

	time.Sleep(250 * time.Millisecond)

	sender.mu.Lock()
	n := len(sender.payloads)
	sender.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected no timer flush shortly after size flush, got %d", n)
	}

	time.Sleep(350 * time.Millisecond)

	sender.mu.Lock()
	n = len(sender.payloads)
	sender.mu.Unlock()
	if n != 2 {
		t.Fatalf("expected timer flush after reset interval, got %d", n)
	}
	if sender.payloads[1][0].ID != "r" {
		t.Fatalf("unexpected payload in timer flush: %v", sender.payloads[1])
	}
}

func TestBatcher_CommitNRollbackN(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Add(data("c"))

	b.CommitN(2)
	b.RollbackN(1)
	_ = b.Flush()

	if len(sender.payloads) != 1 {
		t.Fatalf("expected 1 flush, got %d", len(sender.payloads))
	}
	got := sender.payloads[0]
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("unexpected flushed items: %v", got)
	}
}

func TestBatcher_OrderPreserved(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), Config{BufferSize: 10, FlushTimeout: 0}, nil, nil, sender)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Add(data("c"))

	b.Commit()
	b.Commit()
	b.Commit()

	_ = b.Flush()

	got := sender.payloads[0]

	if got[0].ID != "a" ||
		got[1].ID != "b" ||
		got[2].ID != "c" {
		t.Fatalf("order broken")
	}
}
