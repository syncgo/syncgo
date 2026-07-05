//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/romanchechyotkin/syncgo/internal/batcher"
	"github.com/romanchechyotkin/syncgo/internal/bulk_transformer"
	"github.com/romanchechyotkin/syncgo/pkg/search_engine_client/mocks"

	"github.com/google/uuid"
	gomock "go.uber.org/mock/gomock"
)

func TestBatcherWithElasticsearch_FlushOnSize(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MaxTimes(1)

	client := newElasticsearchClient(t, ctrl, monitoring)

	id1 := uuid.New().String()
	id2 := uuid.New().String()

	b := batcher.NewBatcher(ctx, 2, 0, client)

	b.Add(bulk_transformer.Data{ID: id1, Action: bulk_transformer.Create, Body: []byte(`{"title":"Batcher Size E2E 1"}`)})
	b.Commit()
	b.Add(bulk_transformer.Data{ID: id2, Action: bulk_transformer.Create, Body: []byte(`{"title":"Batcher Size E2E 2"}`)})
	b.Commit()
	// third Add triggers auto flush of id1 and id2 (buffer size reached)
	b.Add(bulk_transformer.Data{ID: uuid.New().String(), Action: bulk_transformer.Create, Body: []byte(`{"title":"trigger"}`)})

	exists, err := client.DocumentExists(ctx, id1)
	if err != nil {
		t.Fatal("e2e; elasticsearch; batcher; failed to check document existence:", err)
	}
	if !exists {
		t.Fatal("e2e; elasticsearch; batcher; expected id1 to exist after flush on buffer full")
	}

	exists, err = client.DocumentExists(ctx, id2)
	if err != nil {
		t.Fatal("e2e; elasticsearch; batcher; failed to check document existence:", err)
	}
	if !exists {
		t.Fatal("e2e; elasticsearch; batcher; expected id2 to exist after flush on buffer full")
	}
}

func TestBatcherWithElasticsearch_Flush(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MaxTimes(1)

	client := newElasticsearchClient(t, ctrl, monitoring)

	id := uuid.New().String()

	b := batcher.NewBatcher(ctx, 100, 0, client)
	b.Add(bulk_transformer.Data{ID: id, Action: bulk_transformer.Create, Body: []byte(`{"title":"Batcher FlushNow E2E"}`)})
	b.Commit()
	b.Flush()

	exists, err := client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; elasticsearch; batcher; failed to check document existence:", err)
	}
	if !exists {
		t.Fatal("e2e; elasticsearch; batcher; expected document to exist after explicit Flush")
	}
}

func TestBatcherWithElasticsearch_DeleteAfterCreate(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(2)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MaxTimes(2)

	client := newElasticsearchClient(t, ctrl, monitoring)

	id := uuid.New().String()

	createBatcher := batcher.NewBatcher(ctx, 100, 0, client)
	createBatcher.Add(bulk_transformer.Data{ID: id, Action: bulk_transformer.Create, Body: []byte(`{"title":"to be deleted"}`)})
	createBatcher.Commit()
	createBatcher.Flush()

	exists, err := client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; elasticsearch; batcher; failed to check document existence after create:", err)
	}
	if !exists {
		t.Fatal("e2e; elasticsearch; batcher; expected document to exist after create flush")
	}

	deleteBatcher := batcher.NewBatcher(ctx, 100, 0, client)
	deleteBatcher.Add(bulk_transformer.Data{ID: id, Action: bulk_transformer.Delete})
	deleteBatcher.Commit()
	deleteBatcher.Flush()

	exists, err = client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; elasticsearch; batcher; failed to check document existence after delete:", err)
	}
	if exists {
		t.Fatal("e2e; elasticsearch; batcher; expected document to be gone after delete flush")
	}
}
