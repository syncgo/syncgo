package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/romanchechyotkin/syncgo/internal/replication"
	"github.com/romanchechyotkin/syncgo/pkg/elasticsearch"
	_ "github.com/romanchechyotkin/syncgo/pkg/logger"
	"github.com/romanchechyotkin/syncgo/pkg/opensearch"
	"github.com/romanchechyotkin/syncgo/pkg/postgresql"

	es "github.com/elastic/go-elasticsearch/v9"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

const cancelTimeoutVarByNastya = 30 * time.Second

func main() {
	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	initCtx, cancel := context.WithTimeout(ctx, cancelTimeoutVarByNastya)
	defer cancel()

	esClient, err := initEsClient(initCtx)
	if err != nil {
		slog.Error("failed init es connection", slog.String("err", err.Error()))
		os.Exit(1)
	}
	_ = esClient

	osClient, err := initOpenSearchClient(initCtx)
	if err != nil {
		slog.Error("failed init os connection", slog.String("err", err.Error()))
		os.Exit(1)
	}
	_ = osClient

	replicationConn, err := initPostgresqlReplicationConn(initCtx)
	if err != nil {
		slog.Error("failed init postgresql connection", slog.String("err", err.Error()))
		os.Exit(1)
	}

	if err = replicationConn.StartReplication(ctx); err != nil {
		slog.Error("failed to start replication", slog.String("err", err.Error()))
		os.Exit(1)
	}

	replicationConn.Close()
}

func initEsClient(ctx context.Context) (*es.Client, error) {
	esClient, err := elasticsearch.New(ctx, elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
		Username:  "admin",
		Password:  "Es123456",
	})
	if err != nil {
		slog.Error("failed to created", slog.String("error", err.Error()))
		return nil, err
	}

	return esClient, nil
}

func initOpenSearchClient(ctx context.Context) (*opensearchapi.Client, error) {
	osClient, err := opensearch.New(ctx, opensearch.Config{
		Addresses: []string{"http://localhost:9300"},
		Username:  "admin",
		Password:  "Op3nS3arch!",
	})
	if err != nil {
		slog.Error("failed to created", slog.String("error", err.Error()))
		return nil, err
	}

	return osClient, nil
}

func initPostgresqlReplicationConn(ctx context.Context) (*replication.LogicalReplicationConn, error) {
	conn, err := postgresql.New(ctx, postgresql.Config{
		User:     "pglogrepl",
		Password: "secret",
		Host:     "localhost",
		Port:     "5432",
		Database: "pglogrepl",
	})
	if err != nil {
		return nil, err
	}

	return replication.New(ctx, conn)
}
