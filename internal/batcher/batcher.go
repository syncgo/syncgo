package batcher

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
)

//go:generate go tool mockgen -source=batcher.go -destination=./mocks/bulk_sender_mock.go -package=mocks
type BulkSender interface {
	Bulk(ctx context.Context, data bulk_transformer.DataPayload) error
}

//go:generate go tool mockgen -source=batcher.go -destination=./mocks/metrics_mock.go -package=mocks
type Metrics interface {
	SetBatcherBufferSize(n int)
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

	sender  BulkSender
	metrics Metrics
	ctx     context.Context
}

func NewBatcher(ctx context.Context, bufferSize int, flushTimeout time.Duration, sender BulkSender, metrics Metrics) *Batcher {
	batcher := &Batcher{
		ctx:           ctx,
		buffer:        make([]bulk_transformer.Data, 0, bufferSize),
		bufferSize:    bufferSize,
		flushTimeout:  flushTimeout,
		lastCommitted: -1,
		sender:        sender,
		metrics:       metrics,
		lastFlushTime: time.Now(),
	}

	if flushTimeout > 0 {
		go batcher.startFlushLoop()
	}

	return batcher
}

func (b *Batcher) reportBufferSize() {
	if b.metrics == nil {
		return
	}
	b.metrics.SetBatcherBufferSize(len(b.buffer))
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
			b.withLock(func() {
				idle := time.Since(b.lastFlushTime)
				shouldFlush := b.flushTimeout > 0 && idle >= b.flushTimeout
				if shouldFlush {
					b.flush()
				}
			})
		}
	}
}

// Add appends a new item into the batch.
// If the buffer is full, committed items are flushed first.
func (b *Batcher) Add(item bulk_transformer.Data) {
	b.withLock(func() {
		b.add(item)
	})
}

func (b *Batcher) add(item bulk_transformer.Data) {
	if len(b.buffer) >= b.bufferSize {
		b.flush()
	}

	b.buffer = append(b.buffer, item)
	b.reportBufferSize()
}

// CommitN calls Commit method n times
// Commit order must match Add order.
func (b *Batcher) CommitN(n int) {
	b.withLock(func() {
		for i := 0; i < n; i++ {
			b.commit()
		}
	})

}

// Commit marks the next item in order as committed.
// Commit order must match Add order.
func (b *Batcher) Commit() {
	b.withLock(b.commit)
}

func (b *Batcher) commit() {
	nextCommit := b.lastCommitted + 1
	if nextCommit >= len(b.buffer) {
		return
	}

	b.lastCommitted = nextCommit
}

// RollbackN calls rollback n times.
func (b *Batcher) RollbackN(n int) {
	b.withLock(
		func() {
			for i := 0; i < n; i++ {
				b.rollback()
			}
		})
}

// Rollback removes the latest uncommitted item.
func (b *Batcher) Rollback() {
	b.withLock(b.rollback)
}

func (b *Batcher) rollback() {
	if len(b.buffer) == 0 {
		return
	}

	lastIndex := len(b.buffer) - 1

	// Cannot rollback committed data
	if lastIndex <= b.lastCommitted {
		return
	}

	b.buffer = b.buffer[:lastIndex]
	b.reportBufferSize()
}

// Flush sends only committed items.
func (b *Batcher) Flush() {
	b.withLock(b.flush)
}

// flush flushes committed prefix.
// Caller must hold mutex.
func (b *Batcher) flush() {
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
	b.reportBufferSize()

	// Reset commit pointer relative to new buffer
	b.lastCommitted = -1
	b.lastFlushTime = time.Now()
}

func (b *Batcher) withLock(f func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	f()
}
