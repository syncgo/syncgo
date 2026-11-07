package processor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/logger"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/postgresql"
	"github.com/syncgo/syncgo/pkg/search_engine_client"
	"github.com/syncgo/syncgo/pkg/state"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

const prepareTimeout = 30 * time.Second

// Prepare creates the PostgreSQL publication and replication slot and the
// search engine index. Every step is idempotent: existing objects are left
// as is, so the command can be run multiple times safely.
//
// When the replication slot is created, the slot exports a snapshot and the
// publication tables are copied into the search engine from that snapshot
// (the database state exactly at the slot's consistent point). The
// consistent point is persisted in the state file, so the run command can
// start streaming from it without losing or duplicating changes.
//
// The state file path is derived from the config path: the state file lives
// next to the config file.
func Prepare(parentCtx context.Context, cfg *config.Config, configPath string, monitoring *metrics.Metrics) error {
	ctx, cancel := context.WithTimeout(parentCtx, prepareTimeout)
	defer cancel()

	statePath := state.PathFromConfig(configPath)

	if err := preparePublicationAndSlot(ctx, cfg, statePath, monitoring); err != nil {
		return err
	}

	if err := prepareIndex(ctx, cfg, monitoring); err != nil {
		return err
	}

	return nil
}

// preparePublicationAndSlot ensures the configured publication (when a table
// list is provided) and replication slot exist. On slot creation it also
// performs the initial data copy from the exported snapshot and persists the
// consistent point in the state file.
func preparePublicationAndSlot(ctx context.Context, cfg *config.Config, statePath string, monitoring *metrics.Metrics) error {
	adminConn, err := postgresql.New(ctx, postgresql.Config{
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
		if err := adminConn.EnsurePublication(ctx, cfg.PostgreSQL.PublicationName, cfg.PostgreSQL.Tables); err != nil {
			return fmt.Errorf("failed to ensure publication %q: %w", cfg.PostgreSQL.PublicationName, err)
		}
	} else {
		slog.Info("no tables configured, skipping publication management",
			slog.String("publication", cfg.PostgreSQL.PublicationName),
		)
	}

	slotExists, err := adminConn.ReplicationSlotExists(ctx, cfg.PostgreSQL.SlotName)
	if err != nil {
		return fmt.Errorf("failed to check replication slot %q: %w", cfg.PostgreSQL.SlotName, err)
	}

	if slotExists {
		slog.Info("replication slot already exists", slog.String("name", cfg.PostgreSQL.SlotName))
		return nil
	}

	if err := createSlotWithInitialCopy(ctx, cfg, adminConn, statePath, monitoring); err != nil {
		return err
	}

	return nil
}

// createSlotWithInitialCopy creates the replication slot on a walsender
// connection with an exported snapshot, copies the publication tables into
// the search engine from that snapshot while the slot connection stays open,
// and persists the slot's consistent point in the state file.
func createSlotWithInitialCopy(ctx context.Context, cfg *config.Config, adminConn *postgresql.Client, statePath string, monitoring *metrics.Metrics) error {
	replicationConn, err := postgresql.New(ctx, postgresql.Config{
		User:        cfg.PostgreSQL.User,
		Password:    cfg.PostgreSQL.Password,
		Host:        cfg.PostgreSQL.Host,
		Port:        cfg.PostgreSQL.Port,
		Database:    cfg.PostgreSQL.Database,
		Replication: true,
	})
	if err != nil {
		return fmt.Errorf("failed to open replication connection: %w", err)
	}
	// The exported snapshot is only valid while this connection is open and
	// executes no other commands, so it must outlive the initial copy.
	defer func() {
		if closeErr := replicationConn.Close(ctx); closeErr != nil {
			logger.Error("failed to close replication connection", closeErr)
		}
	}()

	consistentPoint, snapshotName, err := postgresql.CreateReplicationSlotWithSnapshot(ctx, replicationConn.PgConn(), cfg.PostgreSQL.SlotName)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.DuplicateObject {
			// The slot was created concurrently; nothing to copy — the
			// state file must already exist.
			slog.Info("replication slot already exists", slog.String("name", cfg.PostgreSQL.SlotName))
			return nil
		}
		return fmt.Errorf("failed to create replication slot %q: %w", cfg.PostgreSQL.SlotName, err)
	}

	if len(cfg.PostgreSQL.Tables) > 0 {
		if err := copyInitialData(ctx, cfg, snapshotName, monitoring); err != nil {
			return fmt.Errorf("failed to run initial copy: %w", err)
		}
	} else {
		slog.Info("no tables configured, skipping initial copy")
	}

	st := &state.State{
		SlotName:        cfg.PostgreSQL.SlotName,
		ConsistentPoint: consistentPoint,
	}
	if err := state.Save(statePath, st); err != nil {
		return fmt.Errorf("failed to save state file %q: %w", statePath, err)
	}
	slog.Info("saved consistent point",
		slog.String("stateFile", statePath),
		slog.String("consistentPoint", consistentPoint),
	)

	return nil
}

// prepareIndex ensures the search engine index exists.
func prepareIndex(ctx context.Context, cfg *config.Config, monitoring *metrics.Metrics) error {
	client, err := search_engine_client.New(ctx, searchEngineClientConfig(cfg.SearchEngine), monitoring)
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
