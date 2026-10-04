//go:build e2e

package utils

import (
	"context"
	"testing"

	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/search_engine_client"
)

// NewSearchEngineClient loads configPath and builds a search engine client
// from it, failing the test on error. engine is used only to label errors
// (e.g. "elasticsearch", "opensearch").
func NewSearchEngineClient(t *testing.T, monitoring search_engine_client.Monitoring, configPath, engine string) *search_engine_client.Client {
	t.Helper()

	cfg, err := config.LoadFromYAML(configPath)
	if err != nil {
		t.Fatalf("e2e; %s; failed to load config; error: %v", engine, err)
	}

	client, err := search_engine_client.New(context.Background(), search_engine_client.Config{
		Name:              cfg.SearchEngine.Name,
		Address:           cfg.SearchEngine.Address,
		Username:          cfg.SearchEngine.Username,
		Password:          cfg.SearchEngine.Password,
		Index:             cfg.SearchEngine.Index,
		ConnectionTimeout: cfg.SearchEngine.ConnectionTimeout,
		GzipCompression:   cfg.SearchEngine.GzipCompression,
	}, monitoring)
	if err != nil {
		t.Fatalf("e2e; %s; failed to init %s client; error: %v", engine, engine, err)
	}

	return client
}
