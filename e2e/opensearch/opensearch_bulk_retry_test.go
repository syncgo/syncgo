//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/search_engine_client"

	"github.com/google/uuid"
)

func newOpensearchRetryClient(t *testing.T, maxRetries int, retryTimeout time.Duration) *search_engine_client.Client {
	t.Helper()

	cfg, err := config.LoadFromYAML("config_sync_opensearch.yaml")
	if err != nil {
		t.Fatal("e2e; opensearch; failed to load config; error: ", err)
	}

	client, err := search_engine_client.New(context.Background(), search_engine_client.Config{
		Name:              cfg.SearchEngine.Name,
		Address:           cfg.SearchEngine.Address,
		Username:          cfg.SearchEngine.Username,
		Password:          cfg.SearchEngine.Password,
		Index:             cfg.SearchEngine.Index,
		ConnectionTimeout: cfg.SearchEngine.ConnectionTimeout,
		GzipCompression:   cfg.SearchEngine.GzipCompression,
		MaxRetries:        maxRetries,
		RetryTimeout:      retryTimeout,
	}, metrics.DefaultMetrics)
	if err != nil {
		t.Fatal("e2e; opensearch; failed to init opensearch client; error: ", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestOpensearchClient_BulkRetriesThenSucceeds(t *testing.T) {
	ctx := context.Background()

	client := newOpensearchRetryClient(t, 3, 300*time.Millisecond)

	id := uuid.New().String()

	updatePayload := bulk_transformer.DataPayload{
		{ID: id, Action: bulk_transformer.Update, Body: []byte(`{"title":"retried"}`)},
	}

	bulkErr := make(chan error, 1)
	go func() {
		bulkErr <- client.Bulk(ctx, updatePayload)
	}()

	time.Sleep(150 * time.Millisecond)
	createPayload := bulk_transformer.DataPayload{
		{ID: id, Action: bulk_transformer.Create, Body: []byte(`{"title":"original"}`)},
	}
	if err := client.Bulk(ctx, createPayload); err != nil {
		t.Fatal("e2e; opensearch; failed to create document for retry scenario:", err)
	}

	select {
	case err := <-bulkErr:
		if err != nil {
			t.Fatalf("expected bulk to eventually succeed, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bulk did not complete in time")
	}

	exists, err := client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; opensearch; failed to check document existence:", err)
	}
	if !exists {
		t.Fatal("e2e; opensearch; expected document to exist after retried update")
	}
}

func TestOpensearchClient_BulkExhaustsRetries(t *testing.T) {
	ctx := context.Background()

	client := newOpensearchRetryClient(t, 2, 100*time.Millisecond)

	id := uuid.New().String()

	updatePayload := bulk_transformer.DataPayload{
		{ID: id, Action: bulk_transformer.Update, Body: []byte(`{"title":"never"}`)},
	}

	start := time.Now()
	err := client.Bulk(ctx, updatePayload)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("e2e; opensearch; expected bulk to fail after exhausting retries")
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("expected bulk to wait through retries, elapsed only %v", elapsed)
	}
}
