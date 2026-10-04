//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/syncgo/syncgo/e2e/pkg/utils"
	"github.com/syncgo/syncgo/internal/bulk_transformer"
	"github.com/syncgo/syncgo/pkg/metrics"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

func TestElasticsearchClient_Create(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())

	client := utils.NewSearchEngineClient(t, monitoring, "config_elasticsearch.yaml", "elasticsearch")

	test_uuid := uuid.New().String()

	payload := bulk_transformer.DataPayload{
		{
			ID:     test_uuid,
			Action: bulk_transformer.Create,
			Body:   []byte(`{"title":"Prisoners 2","year":2013}`),
		},
	}

	err := client.Bulk(ctx, payload)
	if err != nil {
		t.Fatal("e2e; elasticsearch; failed to create index & document through bulk api; error: ", err)
	}
}

func TestElasticsearchClient_Delete(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())

	client := utils.NewSearchEngineClient(t, monitoring, "config_elasticsearch.yaml", "elasticsearch")

	test_uuid := uuid.New().String()

	createPayload := bulk_transformer.DataPayload{
		{
			ID:     test_uuid,
			Action: bulk_transformer.Create,
			Body:   []byte(`{"title":"Prisoners 2","year":2013}`),
		},
	}

	err := client.Bulk(ctx, createPayload)
	if err != nil {
		t.Fatal("e2e; elasticsearch; failed to create index & document through bulk api; error: ", err)
	}

	deletePayload := bulk_transformer.DataPayload{
		{
			ID:     test_uuid,
			Action: bulk_transformer.Delete,
		},
	}

	err = client.Bulk(ctx, deletePayload)
	if err != nil {
		t.Fatal("e2e; elasticsearch; failed to delete document through bulk api; error: ", err)
	}
}

func TestElasticsearchClient_Index(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())

	client := utils.NewSearchEngineClient(t, monitoring, "config_elasticsearch.yaml", "elasticsearch")

	test_uuid := uuid.New().String()

	createPayload := bulk_transformer.DataPayload{
		{
			ID:     test_uuid,
			Action: bulk_transformer.Create,
			Body:   []byte(`{"title":"Prisoners 2","year":2013}`),
		},
	}

	err := client.Bulk(ctx, createPayload)
	if err != nil {
		t.Fatal("e2e; elasticsearch; failed to create index & document through bulk api; error: ", err)
	}

	indexPayload := bulk_transformer.DataPayload{
		{
			ID:     test_uuid,
			Action: bulk_transformer.Index,
			Body:   []byte(`{"title":"Rush","year":2013}`),
		},
	}

	err = client.Bulk(ctx, indexPayload)
	if err != nil {
		t.Fatal("e2e; elasticsearch; failed to index document through bulk api; error: ", err)
	}
}

func TestElasticsearchClient_Update(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())

	client := utils.NewSearchEngineClient(t, monitoring, "config_elasticsearch.yaml", "elasticsearch")

	test_uuid := uuid.New().String()

	createPayload := bulk_transformer.DataPayload{
		{
			ID:     test_uuid,
			Action: bulk_transformer.Create,
			Body:   []byte(`{"title":"Prisoners 2","year":2013}`),
		},
	}

	err := client.Bulk(ctx, createPayload)
	if err != nil {
		t.Fatal("e2e; elasticsearch; failed to create index & document through bulk api; error: ", err)
	}

	updatePayload := bulk_transformer.DataPayload{
		{
			ID:     test_uuid,
			Action: bulk_transformer.Update,
			Body:   []byte(`{"title":"World War Z"}`),
		},
	}

	err = client.Bulk(ctx, updatePayload)
	if err != nil {
		t.Fatal("e2e; elasticsearch; failed to update document through bulk api; error: ", err)
	}
}

func TestElasticsearchClient_CreateIndex(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())

	client := utils.NewSearchEngineClient(t, monitoring, "config_elasticsearch.yaml", "elasticsearch")

	indexName := "test_" + uuid.New().String()

	if err := client.CreateIndex(ctx, indexName, nil); err != nil {
		t.Fatal("e2e; elasticsearch; failed to create index; error: ", err)
	}
}

func TestElasticsearchClient_IndexExists(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())

	client := utils.NewSearchEngineClient(t, monitoring, "config_elasticsearch.yaml", "elasticsearch")

	indexName := "test_" + uuid.New().String()

	exists, err := client.IndexExists(ctx, indexName)
	if err != nil {
		t.Fatal("e2e; elasticsearch; failed to check index existence; error: ", err)
	}
	if exists {
		t.Fatal("e2e; elasticsearch; index should not exist yet")
	}

	if err = client.CreateIndex(ctx, indexName, nil); err != nil {
		t.Fatal("e2e; elasticsearch; failed to create index; error: ", err)
	}

	exists, err = client.IndexExists(ctx, indexName)
	if err != nil {
		t.Fatal("e2e; elasticsearch; failed to check index existence after create; error: ", err)
	}
	if !exists {
		t.Fatal("e2e; elasticsearch; index should exist after creation")
	}
}
