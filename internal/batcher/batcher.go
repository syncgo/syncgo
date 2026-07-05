package batcher

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/romanchechyotkin/syncgo/internal/bulk_transformer"
)

//go:generate go tool mockgen -source=batcher.go -destination=./mocks/bulk_sender_mock.go -package=mocks
type BulkSender interface {
	Bulk(ctx context.Context, data bulk_transformer.DataPayload) error
}

type Batcher struct {
	mu sync.Mutex

	buffer       []bulk_transformer.Data
	bufferSize   int
	flushTimeout time.Duration

	// lastCommitted is the index of the last committed item in buffer.
	// -1 means nothing committed yet.
	lastCommitted int

	// lastFlushTime is updated after every successful flush; used by StartFlushLoop
	// to avoid firing a timeout flush too soon after a size-triggered flush.
	lastFlushTime time.Time

	sender BulkSender
	ctx    context.Context
}

func NewBatcher(ctx context.Context, bufferSize int, flushTimeout time.Duration, sender BulkSender) *Batcher {
	batcher := &Batcher{
		ctx:           ctx,
		buffer:        make([]bulk_transformer.Data, 0, bufferSize),
		bufferSize:    bufferSize,
		flushTimeout:  flushTimeout,
		lastCommitted: -1,
		sender:        sender,
		lastFlushTime: time.Now(),
	}

	if flushTimeout > 0 {
		go batcher.startFlushLoop()
	}

	return batcher
}

func (b *Batcher) startFlushLoop() {
	ticker := time.NewTicker(b.flushTimeout)
	defer ticker.Stop()

	for b.ctx.Err() == nil {
		select {
		case <-b.ctx.Done():
			slog.Info("batcher context done")
			return
		case <-ticker.C:
			b.mu.Lock()
			idle := time.Since(b.lastFlushTime)
			shouldFlush := b.flushTimeout > 0 && idle >= b.flushTimeout
			if shouldFlush {
				b.flushLocked()
			}
			b.mu.Unlock()
		}
	}
}

// Add appends a new item into the batch.
// If the buffer is full, committed items are flushed first.
func (b *Batcher) Add(item bulk_transformer.Data) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buffer) >= b.bufferSize {
		b.flushLocked()
	}

	b.buffer = append(b.buffer, item)
}

// CommitN calls Commit method n times
// Commit order must match Add order.
func (b *Batcher) CommitN(n int) {
	for i := 0; i < n; i++ {
		b.Commit()
	}
}

// Commit marks the next item in order as committed.
// Commit order must match Add order.
func (b *Batcher) Commit() {
	b.mu.Lock()
	defer b.mu.Unlock()

	nextCommit := b.lastCommitted + 1
	if nextCommit >= len(b.buffer) {
		return
	}

	b.lastCommitted = nextCommit
}

// RollbackN calls Rollback n times.
func (b *Batcher) RollbackN(n int) {
	for i := 0; i < n; i++ {
		b.Rollback()
	}
}

// Rollback removes the latest uncommitted item.
func (b *Batcher) Rollback() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buffer) == 0 {
		return
	}

	lastIndex := len(b.buffer) - 1

	// Cannot rollback committed data
	if lastIndex <= b.lastCommitted {
		return
	}

	b.buffer = b.buffer[:lastIndex]
}

// Flush sends only committed items.
func (b *Batcher) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.flushLocked()
}

// flushLocked flushes committed prefix.
// Caller must hold mutex.
func (b *Batcher) flushLocked() {
	if b.lastCommitted < 0 {
		return
	}

	flushLen := b.lastCommitted + 1
	payload := bulk_transformer.DataPayload(b.buffer[:flushLen])

	if err := b.sender.Bulk(b.ctx, payload); err != nil {
		slog.Error("batcher: bulk send failed",
			slog.Int("last_committed", b.lastCommitted),
			slog.Int("flush_len", flushLen),
			slog.String("error", err.Error()),
		)
		return
	}

	// Keep only uncommitted tail
	b.buffer = b.buffer[flushLen:]

	// Reset commit pointer relative to new buffer
	b.lastCommitted = -1
	b.lastFlushTime = time.Now()
}
