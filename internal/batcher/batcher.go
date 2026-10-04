//go:generate go tool mockgen -source=batcher.go -destination=./mocks/mocks.go -package=mocks
package batcher

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
)

type BulkSender interface {
	Bulk(ctx context.Context, data bulk_transformer.DataPayload) error
}

type FlushMonitoring interface {
	IncFlushRetry()
	IncFlushFailure()
}

type Metrics interface {
	SetBatcherBufferSize(n int)
}

type Config struct {
	BufferSize   int
	FlushTimeout time.Duration
	MaxRetries   int
	RetryTimeout time.Duration
}

type Batcher struct {
	mu sync.Mutex

	buffer       []bulk_transformer.Data
	bufferSize   int
	flushTimeout time.Duration
	maxRetries   int
	retryTimeout time.Duration

	lastCommitted int

	lastFlushTime time.Time

	sender     BulkSender
	monitoring FlushMonitoring
	metrics    Metrics
	ctx        context.Context
}

func NewBatcher(ctx context.Context, cfg Config, monitoring FlushMonitoring, metrics Metrics, sender BulkSender) *Batcher {
	batcher := &Batcher{
		ctx:           ctx,
		buffer:        make([]bulk_transformer.Data, 0, cfg.BufferSize),
		bufferSize:    cfg.BufferSize,
		flushTimeout:  cfg.FlushTimeout,
		maxRetries:    cfg.MaxRetries,
		retryTimeout:  cfg.RetryTimeout,
		lastCommitted: -1,
		sender:        sender,
		monitoring:    monitoring,
		metrics:       metrics,
		lastFlushTime: time.Now(),
	}

	if cfg.FlushTimeout > 0 {
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
					_ = b.flush()
				}
			})
		}
	}
}

func (b *Batcher) Add(item bulk_transformer.Data) {
	b.withLock(func() {
		b.add(item)
	})
}

func (b *Batcher) add(item bulk_transformer.Data) {
	if len(b.buffer) >= b.bufferSize {
		_ = b.flush()
	}

	b.buffer = append(b.buffer, item)
	b.reportBufferSize()
}

func (b *Batcher) CommitN(n int) {
	b.withLock(func() {
		for i := 0; i < n; i++ {
			b.commit()
		}
	})
}

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

func (b *Batcher) RollbackN(n int) {
	b.withLock(func() {
		for i := 0; i < n; i++ {
			b.rollback()
		}
	})
}

func (b *Batcher) Rollback() {
	b.withLock(b.rollback)
}

func (b *Batcher) rollback() {
	if len(b.buffer) == 0 {
		return
	}

	lastIndex := len(b.buffer) - 1

	if lastIndex <= b.lastCommitted {
		return
	}

	b.buffer = b.buffer[:lastIndex]
	b.reportBufferSize()
}

func (b *Batcher) Flush() error {
	var err error
	b.withLock(func() {
		err = b.flush()
	})
	return err
}

func (b *Batcher) flush() error {
	if b.lastCommitted < 0 {
		return nil
	}

	flushLen := b.lastCommitted + 1
	payload := bulk_transformer.DataPayload(b.buffer[:flushLen])

	var err error
	for attempt := 1; attempt <= b.maxRetries+1; attempt++ {
		err = b.sender.Bulk(b.ctx, payload)
		if err == nil {
			break
		}

		slog.Error("batcher: bulk send failed",
			slog.Int("attempt", attempt),
			slog.Int("max_attempts", b.maxRetries+1),
			slog.Int("last_committed", b.lastCommitted),
			slog.Int("flush_len", flushLen),
			slog.String("error", err.Error()),
		)

		if attempt > b.maxRetries {
			break
		}

		if b.monitoring != nil {
			b.monitoring.IncFlushRetry()
		}

		select {
		case <-b.ctx.Done():
			err = b.ctx.Err()
			if b.monitoring != nil {
				b.monitoring.IncFlushFailure()
			}
			return err
		case <-time.After(b.retryTimeout):
		}
	}

	if err != nil {
		slog.Error("batcher: flush failed, giving up after retries",
			slog.Int("attempts", b.maxRetries+1),
			slog.Int("flush_len", flushLen),
			slog.String("error", err.Error()),
		)
		if b.monitoring != nil {
			b.monitoring.IncFlushFailure()
		}
		return err
	}

	b.buffer = b.buffer[flushLen:]
	b.reportBufferSize()

	b.lastCommitted = -1
	b.lastFlushTime = time.Now()

	return nil
}

func (b *Batcher) withLock(f func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	f()
}
