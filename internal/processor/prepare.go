package processor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/logger"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/postgresql"
	"github.com/syncgo/syncgo/pkg/search_engine_client"
)

const prepareTimeout = 30 * time.Second

// Prepare creates the PostgreSQL publication and replication slot and the
// search engine index. Every step is idempotent: existing objects are left
// as is, so the command can be run multiple times safely.
func Prepare(parentCtx context.Context, cfg *config.Config, monitoring *metrics.Metrics) error {
	ctx, cancel := context.WithTimeout(parentCtx, prepareTimeout)
	defer cancel()

	if err := preparePublicationAndSlot(ctx, cfg); err != nil {
		return err
	}

	if err := prepareIndex(ctx, cfg, monitoring); err != nil {
		return err
	}

	return nil
}

// preparePublicationAndSlot ensures the configured publication (when a table
// list is provided) and replication slot exist.
func preparePublicationAndSlot(ctx context.Context, cfg *config.Config) error {
	adminConn, err := postgresql.NewStandard(ctx, postgresql.Config{
		User:     cfg.PostgreSQL.User,
		Password: cfg.PostgreSQL.Password,
		Host:     cfg.PostgreSQL.Host,
		Port:     cfg.PostgreSQL.Port,
		Database: cfg.PostgreSQL.Database,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	defer func() {
		if closeErr := adminConn.Close(ctx); closeErr != nil {
			logger.Error("failed to close admin connection", closeErr)
		}
	}()

	if len(cfg.PostgreSQL.Tables) > 0 {
		if err := postgresql.EnsurePublication(ctx, adminConn, cfg.PostgreSQL.PublicationName, cfg.PostgreSQL.Tables); err != nil {
			return fmt.Errorf("failed to ensure publication %q: %w", cfg.PostgreSQL.PublicationName, err)
		}
	} else {
		slog.Info("no tables configured, skipping publication management",
			slog.String("publication", cfg.PostgreSQL.PublicationName),
		)
	}

	if err := postgresql.EnsureReplicationSlot(ctx, adminConn, cfg.PostgreSQL.SlotName); err != nil {
		return fmt.Errorf("failed to ensure replication slot %q: %w", cfg.PostgreSQL.SlotName, err)
	}

	return nil
}

// prepareIndex ensures the search engine index exists.
func prepareIndex(ctx context.Context, cfg *config.Config, monitoring *metrics.Metrics) error {
	client, err := search_engine_client.New(ctx, search_engine_client.Config{
		Name:              cfg.SearchEngine.Name,
		Address:           cfg.SearchEngine.Address,
		Username:          cfg.SearchEngine.Username,
		Password:          cfg.SearchEngine.Password,
		Index:             cfg.SearchEngine.Index,
		ConnectionTimeout: cfg.SearchEngine.ConnectionTimeout,
		GzipCompression:   cfg.SearchEngine.GzipCompression,
		TLS:               searchTLSToClient(cfg.SearchEngine.TLS),
		KeepAlive:         searchKeepAliveToClient(cfg.SearchEngine.KeepAlive),
	}, monitoring)
	if err != nil {
		return fmt.Errorf("failed to create search engine client: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			logger.Error("failed to close search engine client", closeErr)
		}
	}()

	exists, err := client.IndexExists(ctx, cfg.SearchEngine.Index)
	if err != nil {
		return fmt.Errorf("failed to check index %q: %w", cfg.SearchEngine.Index, err)
	}

	if exists {
		slog.Info("index already exists", slog.String("index", cfg.SearchEngine.Index))
		return nil
	}

	if err := client.CreateIndex(ctx, cfg.SearchEngine.Index, nil); err != nil {
		return fmt.Errorf("failed to create index %q: %w", cfg.SearchEngine.Index, err)
	}

	slog.Info("created index", slog.String("index", cfg.SearchEngine.Index))

	return nil
}
