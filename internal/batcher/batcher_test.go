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
	mu       sync.Mutex
	payloads [][]bulk_transformer.Data
	err      error
}

func (m *mockSender) Bulk(ctx context.Context, payload bulk_transformer.DataPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return m.err
	}

	cp := make([]bulk_transformer.Data, len(payload))
	copy(cp, payload)
	m.payloads = append(m.payloads, cp)

	return nil
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

	b := NewBatcher(context.Background(), 10, 0, sender, nil)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Commit()
	b.Commit()
	b.Flush()

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

	b := NewBatcher(context.Background(), 10, 0, sender, nil)

	b.Add(data("a"))
	b.Flush()

	if len(sender.payloads) != 0 {
		t.Fatalf("expected no flush")
	}
}

func TestBatcher_PartialCommitFlush(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), 10, 0, sender, nil)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Add(data("c"))

	b.Commit()
	b.Commit()

	b.Flush()

	if len(sender.payloads) != 1 {
		t.Fatalf("expected 1 flush")
	}

	if len(sender.payloads[0]) != 2 {
		t.Fatalf("expected 2 committed items")
	}

	// remaining item should stay buffered
	if len(b.buffer) != 1 {
		t.Fatalf("expected 1 remaining item, got %d", len(b.buffer))
	}
}

func TestBatcher_RollbackLastUncommitted(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), 10, 0, sender, nil)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Rollback()

	if len(b.buffer) != 1 {
		t.Fatalf("expected 1 item after rollback, got %d", len(b.buffer))
	}
}

func TestBatcher_RollbackCommittedDoesNothing(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), 10, 0, sender, nil)

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
	b := NewBatcher(context.Background(), 10, 0, sender, nil)

	b.Add(data("a"))
	b.Commit()
	b.Flush()

	if len(b.buffer) != 1 {
		t.Fatalf("buffer should remain on failed flush")
	}

	if b.lastCommitted != 0 {
		t.Fatalf("commit pointer should remain")
	}
}

func TestBatcher_AutoFlushOnBufferFull(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), 2, 0, sender, nil)

	b.Add(data("a"))
	b.Commit()

	b.Add(data("b"))
	b.Commit()

	// next add should trigger auto flush
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
	b := NewBatcher(context.Background(), 10, 0, sender, nil)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Commit()
	b.Commit()
	b.Flush()

	b.Add(data("c"))
	b.Commit()
	b.Flush()

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
	b := NewBatcher(context.Background(), 10, 0, sender, nil)

	b.Add(data("a"))
	b.Commit()
	b.Commit()
	b.Commit()

	b.Flush()

	if len(sender.payloads) != 1 {
		t.Fatalf("expected single flush")
	}

	if len(sender.payloads[0]) != 1 {
		t.Fatalf("must flush only existing item")
	}
}

func TestBatcher_FlushEmpty(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), 10, 0, sender, nil)

	b.Flush()

	if len(sender.payloads) != 0 {
		t.Fatalf("expected no payload")
	}
}

func TestBatcher_RollbackEmpty(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), 10, 0, sender, nil)

	b.Rollback()

	if len(b.buffer) != 0 {
		t.Fatalf("buffer should remain empty")
	}
}

func TestBatcher_AutoFlushOnBufferFull_VerifyData(t *testing.T) {
	sender := &mockSender{}
	b := NewBatcher(context.Background(), 2, 0, sender, nil)

	b.Add(data("x"))
	b.Commit()
	b.Add(data("y"))
	b.Commit()
	b.Add(data("z")) // triggers auto flush of x, y

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
	b := NewBatcher(ctx, 100, 50*time.Millisecond, sender, nil)

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

// TestBatcher_TimeoutResetAfterSizeFlush verifies that a size-triggered flush
// resets the idle timer so the timeout flush does not fire again immediately.
func TestBatcher_TimeoutResetAfterSizeFlush(t *testing.T) {
	sender := &mockSender{}
	ctx := context.Background()
	b := NewBatcher(ctx, 2, 300*time.Millisecond, sender, nil)

	// Start the loop with a 300ms interval, then wait 100ms so the first tick
	// is 200ms away when the size flush happens below.
	time.Sleep(100 * time.Millisecond)

	// Trigger a size flush at ~t=100ms.
	b.Add(data("p"))
	b.Commit()
	b.Add(data("q"))
	b.Commit()
	b.Add(data("r")) // size flush of p, q
	b.Commit()       // commit r so it will be sent on the next flush

	if len(sender.payloads) != 1 {
		t.Fatalf("expected size flush, got %d", len(sender.payloads))
	}

	// The ticker fires at t=300ms. Since the size flush happened at ~t=100ms,
	// time.Since(lastFlushTime) ≈ 200ms < 300ms → no timer flush.
	time.Sleep(250 * time.Millisecond) // now at ~t=350ms; tick fired but was suppressed

	sender.mu.Lock()
	n := len(sender.payloads)
	sender.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected no timer flush shortly after size flush, got %d", n)
	}

	// Next tick at t=600ms: time.Since(100ms) ≈ 500ms >= 300ms → flush r.
	time.Sleep(350 * time.Millisecond) // now at ~t=700ms

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
	b := NewBatcher(context.Background(), 10, 0, sender, nil)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Add(data("c"))

	b.CommitN(2)
	b.RollbackN(1)
	b.Flush()

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
	b := NewBatcher(context.Background(), 10, 0, sender, nil)

	b.Add(data("a"))
	b.Add(data("b"))
	b.Add(data("c"))

	b.Commit()
	b.Commit()
	b.Commit()

	b.Flush()

	got := sender.payloads[0]

	if got[0].ID != "a" ||
		got[1].ID != "b" ||
		got[2].ID != "c" {
		t.Fatalf("order broken")
	}
}
