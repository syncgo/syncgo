//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/syncgo/syncgo/e2e/pkg/utils"
	"github.com/syncgo/syncgo/internal/batcher"
	"github.com/syncgo/syncgo/internal/bulk_transformer"
	"github.com/syncgo/syncgo/pkg/metrics"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

func TestBatcherWithOpensearch_FlushOnSize(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())

	client := utils.NewSearchEngineClient(t, monitoring, "config_opensearch.yaml", "opensearch")

	id1 := uuid.New().String()
	id2 := uuid.New().String()

	b := batcher.NewBatcher(ctx, 2, 0, client, nil)

	b.Add(bulk_transformer.Data{ID: id1, Action: bulk_transformer.Create, Body: []byte(`{"title":"Batcher Size E2E 1"}`)})
	b.Commit()
	b.Add(bulk_transformer.Data{ID: id2, Action: bulk_transformer.Create, Body: []byte(`{"title":"Batcher Size E2E 2"}`)})
	b.Commit()
	// third Add triggers auto flush of id1 and id2 (buffer size reached)
	b.Add(bulk_transformer.Data{ID: uuid.New().String(), Action: bulk_transformer.Create, Body: []byte(`{"title":"trigger"}`)})

	exists, err := client.DocumentExists(ctx, id1)
	if err != nil {
		t.Fatal("e2e; opensearch; batcher; failed to check document existence:", err)
	}
	if !exists {
		t.Fatal("e2e; opensearch; batcher; expected id1 to exist after flush on buffer full")
	}

	exists, err = client.DocumentExists(ctx, id2)
	if err != nil {
		t.Fatal("e2e; opensearch; batcher; failed to check document existence:", err)
	}
	if !exists {
		t.Fatal("e2e; opensearch; batcher; expected id2 to exist after flush on buffer full")
	}
}

func TestBatcherWithOpensearch_Flush(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())

	client := utils.NewSearchEngineClient(t, monitoring, "config_opensearch.yaml", "opensearch")

	id := uuid.New().String()

	b := batcher.NewBatcher(ctx, 100, 0, client, nil)
	b.Add(bulk_transformer.Data{ID: id, Action: bulk_transformer.Create, Body: []byte(`{"title":"Batcher FlushNow E2E"}`)})
	b.Commit()
	b.Flush()

	exists, err := client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; opensearch; batcher; failed to check document existence:", err)
	}
	if !exists {
		t.Fatal("e2e; opensearch; batcher; expected document to exist after explicit Flush")
	}
}

func TestBatcherWithOpensearch_DeleteAfterCreate(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())

	client := utils.NewSearchEngineClient(t, monitoring, "config_opensearch.yaml", "opensearch")

	id := uuid.New().String()

	createBatcher := batcher.NewBatcher(ctx, 100, 0, client, nil)
	createBatcher.Add(bulk_transformer.Data{ID: id, Action: bulk_transformer.Create, Body: []byte(`{"title":"to be deleted"}`)})
	createBatcher.Commit()
	createBatcher.Flush()

	exists, err := client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; opensearch; batcher; failed to check document existence after create:", err)
	}
	if !exists {
		t.Fatal("e2e; opensearch; batcher; expected document to exist after create flush")
	}

	deleteBatcher := batcher.NewBatcher(ctx, 100, 0, client, nil)
	deleteBatcher.Add(bulk_transformer.Data{ID: id, Action: bulk_transformer.Delete})
	deleteBatcher.Commit()
	deleteBatcher.Flush()

	exists, err = client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; opensearch; batcher; failed to check document existence after delete:", err)
	}
	if exists {
		t.Fatal("e2e; opensearch; batcher; expected document to be gone after delete flush")
	}
}
