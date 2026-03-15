//go:build e2e

package e2e

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/romanchechyotkin/syncgo/internal/bulk_transformer"
	"github.com/romanchechyotkin/syncgo/pkg/config"
	"github.com/romanchechyotkin/syncgo/pkg/search_engine_client"
	"github.com/romanchechyotkin/syncgo/pkg/search_engine_client/mocks"

	"github.com/google/uuid"
	gomock "go.uber.org/mock/gomock"
)

func newElasticsearchClient(t *testing.T, ctrl *gomock.Controller, monitoring *mocks.MockMonitoring) *search_engine_client.Client {
	t.Helper()

	cfg, err := config.LoadFromYAML("config_elasticsearch.yaml")
	if err != nil {
		slog.Error("failed to load config", slog.String("err", err.Error()))
		os.Exit(1)
	}

	client, err := search_engine_client.New(context.Background(), search_engine_client.Config{
		Name:              cfg.SearchEngine.Name,
		Addresses:         cfg.SearchEngine.Addresses,
		Username:          cfg.SearchEngine.Username,
		Password:          cfg.SearchEngine.Password,
		Index:             cfg.SearchEngine.Index,
		ConnectionTimeout: cfg.SearchEngine.ConnectionTimeout,
		GzipCompression:   cfg.SearchEngine.GzipCompression,
	}, monitoring)
	if err != nil {
		t.Fatal("e2e; elasticsearch; failed to init elasticsearch client; error: ", err)
	}

	return client
}

func TestElasticsearchClient_Create(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MaxTimes(1)

	client := newElasticsearchClient(t, ctrl, monitoring)

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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MaxTimes(1)

	client := newElasticsearchClient(t, ctrl, monitoring)

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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MaxTimes(1)

	client := newElasticsearchClient(t, ctrl, monitoring)

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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MaxTimes(1)

	client := newElasticsearchClient(t, ctrl, monitoring)

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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)

	client := newElasticsearchClient(t, ctrl, monitoring)

	indexName := "test_" + uuid.New().String()

	if err := client.CreateIndex(ctx, indexName, nil); err != nil {
		t.Fatal("e2e; elasticsearch; failed to create index; error: ", err)
	}
}

func TestElasticsearchClient_IndexExists(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)

	client := newElasticsearchClient(t, ctrl, monitoring)

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
