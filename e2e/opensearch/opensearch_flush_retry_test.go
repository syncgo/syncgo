//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/syncgo/syncgo/internal/batcher"
	"github.com/syncgo/syncgo/internal/bulk_transformer"
	"github.com/syncgo/syncgo/pkg/search_engine_client/mocks"

	"github.com/google/uuid"
	gomock "go.uber.org/mock/gomock"
)

type flushMonitoringSpy struct {
	retries  int
	failures int
}

func (s *flushMonitoringSpy) IncFlushRetry()   { s.retries++ }
func (s *flushMonitoringSpy) IncFlushFailure() { s.failures++ }

// TestBatcherWithOpensearch_FlushRetriesThenSucceeds updates a document that
// does not exist yet. OpenSearch reports a document_missing_exception on the
// first attempt; the document is created while the batcher is waiting for
// its retry, so the retried flush succeeds against the real cluster.
func TestBatcherWithOpensearch_FlushRetriesThenSucceeds(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(2)
	monitoring.EXPECT().ObserveSearchBulkDuration(gomock.Any(), gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MinTimes(1)

	client := newOpensearchClient(t, ctrl, monitoring)

	id := uuid.New().String()

	spy := &flushMonitoringSpy{}
	b := batcher.NewBatcher(ctx, batcher.Config{
		BufferSize:   10,
		MaxRetries:   3,
		RetryTimeout: 300 * time.Millisecond,
	}, spy, nil, client)

	b.Add(bulk_transformer.Data{ID: id, Action: bulk_transformer.Update, Body: []byte(`{"title":"retried"}`)})
	b.Commit()

	flushErr := make(chan error, 1)
	go func() {
		flushErr <- b.Flush()
	}()

	// Let the first attempt fail, then create the document so the retry succeeds.
	time.Sleep(150 * time.Millisecond)
	createPayload := bulk_transformer.DataPayload{
		{ID: id, Action: bulk_transformer.Create, Body: []byte(`{"title":"original"}`)},
	}
	if err := client.Bulk(ctx, createPayload); err != nil {
		t.Fatal("e2e; opensearch; failed to create document for retry scenario:", err)
	}

	select {
	case err := <-flushErr:
		if err != nil {
			t.Fatalf("expected flush to eventually succeed, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not complete in time")
	}

	if spy.retries == 0 {
		t.Fatal("expected at least 1 retry to be reported")
	}
	if spy.failures != 0 {
		t.Fatalf("expected no final failure to be reported, got %d", spy.failures)
	}

	exists, err := client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; opensearch; failed to check document existence:", err)
	}
	if !exists {
		t.Fatal("e2e; opensearch; expected document to exist after retried update")
	}
}

// TestBatcherWithOpensearch_FlushExhaustsRetriesOnPersistentFailure updates a
// document that is never created, so every attempt fails against the real
// cluster. Flush must retry exactly MaxRetries times and then return the error.
func TestBatcherWithOpensearch_FlushExhaustsRetriesOnPersistentFailure(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().ObserveSearchBulkDuration(gomock.Any(), gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MinTimes(1)

	client := newOpensearchClient(t, ctrl, monitoring)

	id := uuid.New().String() // never created

	spy := &flushMonitoringSpy{}
	b := batcher.NewBatcher(ctx, batcher.Config{
		BufferSize:   10,
		MaxRetries:   2,
		RetryTimeout: 100 * time.Millisecond,
	}, spy, nil, client)

	b.Add(bulk_transformer.Data{ID: id, Action: bulk_transformer.Update, Body: []byte(`{"title":"never"}`)})
	b.Commit()

	start := time.Now()
	err := b.Flush()
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("e2e; opensearch; expected flush to fail after exhausting retries")
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("expected flush to wait through retries, elapsed only %v", elapsed)
	}
	if spy.retries != 2 {
		t.Fatalf("expected 2 retries, got %d", spy.retries)
	}
	if spy.failures != 1 {
		t.Fatalf("expected 1 reported failure, got %d", spy.failures)
	}
}
