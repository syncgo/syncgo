package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/romanchechyotkin/syncgo/internal/replication"
	"github.com/romanchechyotkin/syncgo/pkg/config"
	"github.com/romanchechyotkin/syncgo/pkg/elasticsearch"
	_ "github.com/romanchechyotkin/syncgo/pkg/logger"
	"github.com/romanchechyotkin/syncgo/pkg/opensearch"
	"github.com/romanchechyotkin/syncgo/pkg/postgresql"
)

const cancelTimeout = 30 * time.Second

func main() {
	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	initCtx, cancel := context.WithTimeout(ctx, cancelTimeout)
	defer cancel()

	configPath := parseConfigFlag()

	cfg, err := config.LoadFromYAML(configPath)
	if err != nil {
		slog.Error("failed to load config", slog.String("err", err.Error()))
		os.Exit(1)
	}

	if cfg.IsElastic {
		esClient, err := initEsClient(initCtx, cfg)
		if err != nil {
			slog.Error("failed init es connection", slog.String("err", err.Error()))
			os.Exit(1)
		}
		_ = esClient
	} else if cfg.IsOpenSearch {
		osClient, err := initOpenSearchClient(initCtx, cfg)
		if err != nil {
			slog.Error("failed init os connection", slog.String("err", err.Error()))
			os.Exit(1)
		}
		_ = osClient
	}

	replicationConn, err := initPostgresqlReplicationConn(initCtx, cfg)
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

func initEsClient(ctx context.Context, cfg *config.Config) (*elasticsearch.Client, error) {
	esClient, err := elasticsearch.New(ctx, elasticsearch.Config{
		Addresses: cfg.Search.Addresses,
		Username:  cfg.Search.Username,
		Password:  cfg.Search.Password,
		Index:     cfg.Search.Index,
	})
	if err != nil {
		slog.Error("failed to create elasticsearch client", slog.String("error", err.Error()))
		return nil, err
	}

	return esClient, nil
}

func initOpenSearchClient(ctx context.Context, cfg *config.Config) (*opensearch.Client, error) {
	osClient, err := opensearch.New(ctx, opensearch.Config{
		Addresses: cfg.Search.Addresses,
		Username:  cfg.Search.Username,
		Password:  cfg.Search.Password,
		Index:     cfg.Search.Index,
	})
	if err != nil {
		slog.Error("failed to create opensearch client", slog.String("error", err.Error()))
		return nil, err
	}

	return osClient, nil
}

func initPostgresqlReplicationConn(ctx context.Context, cfg *config.Config) (*replication.LogicalReplicationConn, error) {
	conn, err := postgresql.New(ctx, postgresql.Config{
		User:     cfg.PostgreSQL.User,
		Password: cfg.PostgreSQL.Password,
		Host:     cfg.PostgreSQL.Host,
		Port:     cfg.PostgreSQL.Port,
		Database: cfg.PostgreSQL.Database,
	})
	if err != nil {
		slog.Error("failed to create postgresql connection", slog.String("error", err.Error()))
		return nil, err
	}

	return replication.New(ctx, conn)
}

func parseConfigFlag() string {
	var configPath string

	flag.StringVar(&configPath, "config", "", "Path to configuration file (YAML)")
	flag.StringVar(&configPath, "cfg", "", "Path to configuration file (YAML) (shorthand for --config)")
	flag.Parse()

	return configPath
}
