//go:build e2e

package e2e

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/romanchechyotkin/syncgo/pkg/config"
	"github.com/romanchechyotkin/syncgo/pkg/search_engine_client"
	"github.com/romanchechyotkin/syncgo/pkg/search_engine_client/mocks"

	"github.com/google/uuid"
	gomock "go.uber.org/mock/gomock"
)

func newOpensearchClient(t *testing.T, ctrl *gomock.Controller, monitoring *mocks.MockMonitoring) *search_engine_client.Client {
	t.Helper()

	cfg, err := config.LoadFromYAML("config_opensearch.yaml")
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
		t.Fatal("e2e; opensearch; failed to init opensearch client; error: ", err)
	}

	return client
}

func TestOpensearchClient_Create(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MaxTimes(1)

	client := newOpensearchClient(t, ctrl, monitoring)

	test_uuid := uuid.New().String()

	if err := client.Bulk(ctx, []byte("{ \"create\": { \"_index\": \"test_index\", \"_id\": \""+test_uuid+"\" } }\n  { \"title\": \"Prisoners 2\", \"year\": 2013 }\n")); err != nil {
		t.Fatal("e2e; opensearch; failed to create index & document through bulk api; error: ", err)
	}
}

func TestOpensearchClient_Delete(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MaxTimes(1)

	client := newOpensearchClient(t, ctrl, monitoring)

	test_uuid := uuid.New().String()

	if err := client.Bulk(ctx, []byte("{ \"create\": { \"_index\": \"test_index\", \"_id\": \""+test_uuid+"\" } }\n  { \"title\": \"Prisoners 2\", \"year\": 2013 }\n")); err != nil {
		t.Fatal("e2e; opensearch; failed to create index & document through bulk api; error: ", err)
	}

	if err := client.Bulk(ctx, []byte("{ \"delete\": { \"_index\": \"test_index\", \"_id\": \""+test_uuid+"\" } }\n")); err != nil {
		t.Fatal("e2e; opensearch; failed to delete document through bulk api; error: ", err)
	}
}

func TestOpensearchClient_Index(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MaxTimes(1)

	client := newOpensearchClient(t, ctrl, monitoring)

	test_uuid := uuid.New().String()

	if err := client.Bulk(ctx, []byte("{ \"create\": { \"_index\": \"test_index\", \"_id\": \""+test_uuid+"\" } }\n { \"title\": \"Prisoners 2\", \"year\": 2013 }\n")); err != nil {
		t.Fatal("e2e; opensearch; failed to create index & document through bulk api; error: ", err)
	}

	if err := client.Bulk(ctx, []byte("{ \"index\": { \"_index\": \"test_index\", \"_id\": \""+test_uuid+"\" } }\n { \"title\": \"Rush\", \"year\": 2013}\n")); err != nil {
		t.Fatal("e2e; opensearch; failed to index document through bulk api; error: ", err)
	}
}

func TestOpensearchClient_Update(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)
	monitoring.EXPECT().IncSearchRequests(gomock.Any(), gomock.Any()).MinTimes(1)
	monitoring.EXPECT().AddSearchErrors(gomock.Any(), gomock.Any()).MaxTimes(1)

	client := newOpensearchClient(t, ctrl, monitoring)

	test_uuid := uuid.New().String()

	if err := client.Bulk(ctx, []byte("{ \"create\": { \"_index\": \"test_index\", \"_id\": \""+test_uuid+"\" } }\n { \"title\": \"Prisoners 2\", \"year\": 2013 }\n")); err != nil {
		t.Fatal("e2e; opensearch; failed to create index & document through bulk api; error: ", err)
	}

	if err := client.Bulk(ctx, []byte("{ \"update\": { \"_index\": \"test_index\", \"_id\": \""+test_uuid+"\" } }\n { \"doc\" : { \"title\": \"World War Z\" } }\n")); err != nil {
		t.Fatal("e2e; opensearch; failed to update document through bulk api; error: ", err)
	}
}

func TestOpensearchClient_CreateIndex(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)

	client := newOpensearchClient(t, ctrl, monitoring)

	indexName := "test_" + uuid.New().String()

	if err := client.CreateIndex(ctx, indexName, nil); err != nil {
		t.Fatal("e2e; opensearch; failed to create index; error: ", err)
	}
}

func TestOpensearchClient_IndexExists(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)

	client := newOpensearchClient(t, ctrl, monitoring)

	indexName := "test_" + uuid.New().String()

	exists, err := client.IndexExists(ctx, indexName)
	if err != nil {
		t.Fatal("e2e; opensearch; failed to check index existence; error: ", err)
	}
	if exists {
		t.Fatal("e2e; opensearch; index should not exist yet")
	}

	if err = client.CreateIndex(ctx, indexName, nil); err != nil {
		t.Fatal("e2e; opensearch; failed to create index; error: ", err)
	}

	exists, err = client.IndexExists(ctx, indexName)
	if err != nil {
		t.Fatal("e2e; opensearch; failed to check index existence after create; error: ", err)
	}
	if !exists {
		t.Fatal("e2e; opensearch; index should exist after creation")
	}
}
