package asynctask

import (
	"context"
	"time"
)

type AsyncTask struct {
	cancel context.CancelFunc
	done   chan struct{}

	timeout time.Duration
	job     func(context.Context)
}

func New(timeout time.Duration, job func(context.Context)) *AsyncTask {
	return &AsyncTask{
		timeout: timeout,
		done:    make(chan struct{}),
		job:     job,
	}
}

func (at *AsyncTask) Start(ctx context.Context) {
	if at.cancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	go at.runBackgroundLoop(ctx)

	at.cancel = cancel
}

func (at *AsyncTask) runBackgroundLoop(ctx context.Context) {
	ticker := time.NewTicker(at.timeout)
	defer func() {
		ticker.Stop()
		close(at.done)
	}()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			at.job(ctx)
		}
	}
}

func (at *AsyncTask) Stop() {
	if at.cancel == nil {
		return
	}

	at.cancel()
	<-at.done

	at.done = nil
	at.cancel = nil
}
