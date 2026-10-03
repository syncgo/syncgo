//go:build e2e

package e2e

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/search_engine_client"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestGRPC(t *testing.T) {
	ctx := context.Background()

	cfg, err := config.LoadFromYAML("config_opensearch_grpc.yaml")
	if err != nil {
		slog.Error("failed to load config", slog.String("err", err.Error()))
		os.Exit(1)
	}

	monitoring := metrics.New(prometheus.NewRegistry())

	client, err := search_engine_client.New(ctx, search_engine_client.Config{
		Name:              cfg.SearchEngine.Name,
		Address:           cfg.SearchEngine.Address,
		Username:          cfg.SearchEngine.Username,
		Password:          cfg.SearchEngine.Password,
		Index:             cfg.SearchEngine.Index,
		ConnectionTimeout: cfg.SearchEngine.ConnectionTimeout,
		GzipCompression:   cfg.SearchEngine.GzipCompression,
		GRPC: &search_engine_client.GRPCConfig{
			Host: cfg.SearchEngine.GRPC.Host,
			Port: cfg.SearchEngine.GRPC.Port,
		},
	}, monitoring)
	if err != nil {
		t.Fatal("e2e; opensearch; failed to init opensearch client; error: ", err)
	}

	assert.True(t, client.GrpcIsConnected())
}
