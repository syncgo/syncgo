//go:build e2e

// Package utils holds test helpers shared by the e2e suites, so each doesn't
// have to duplicate the same PostgreSQL and search-engine plumbing.
package utils

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/syncgo/syncgo/internal/processor"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/postgresql"
)

// defaultConfigPath is used by LoadConfig when SYNCGO_E2E_CONFIG is not set.
const defaultConfigPath = "../config.yaml"

// LoadConfig loads the e2e config from SYNCGO_E2E_CONFIG or, when unset,
// from the default pipeline config path.
func LoadConfig(t *testing.T) *config.Config {
	t.Helper()

	path := os.Getenv("SYNCGO_E2E_CONFIG")
	if path == "" {
		path = defaultConfigPath
	}

	return LoadConfigFrom(t, path)
}

// LoadConfigFrom loads the config from the given path, failing the test on
// error. Each suite passes its own config so that its slot/publication/index
// names do not clash with other suites running in the same CI job.
func LoadConfigFrom(t *testing.T, path string) *config.Config {
	t.Helper()

	cfg, err := config.LoadFromYAML(path)
	if err != nil {
		t.Fatalf("e2e; failed to load config %q; error: %v", path, err)
	}

	return cfg
}

// PgConfigFrom extracts the PostgreSQL connection config from the app config.
func PgConfigFrom(cfg *config.Config) postgresql.Config {
	return postgresql.Config{
		User:     cfg.PostgreSQL.User,
		Password: cfg.PostgreSQL.Password,
		Host:     cfg.PostgreSQL.Host,
		Port:     cfg.PostgreSQL.Port,
		Database: cfg.PostgreSQL.Database,
	}
}

// NewClient connects to PostgreSQL and registers the connection cleanup on
// the test.
func NewClient(t *testing.T, ctx context.Context, pgCfg postgresql.Config) *postgresql.Client {
	t.Helper()

	client, err := postgresql.New(ctx, pgCfg)
	if err != nil {
		t.Fatal("e2e; failed to connect to postgres; error: ", err)
	}
	t.Cleanup(func() {
		if err := client.Close(ctx); err != nil {
			t.Log("e2e; failed to close postgres client; error: ", err)
		}
	})

	return client
}

// RunProcessor runs prepare (creates publication, slot and index) and then
// starts the sync process, stopping it on test cleanup. Returns the cancel
// function for the run context. The state file with the consistent point is
// written next to the suite's config file.
func RunProcessor(t *testing.T, ctx context.Context, cfg *config.Config, configPath string) context.CancelFunc {
	t.Helper()

	if err := processor.Prepare(ctx, cfg, configPath, metrics.DefaultMetrics); err != nil {
		t.Fatal("e2e; prepare failed; error: ", err)
	}

	return StartProcessor(t, ctx, cfg, configPath)
}

// StartProcessor starts the sync process (the run command) without running
// prepare first. Returns a stop function that cancels the run context and
// waits for a clean shutdown; it is safe to call multiple times and is also
// registered on the test cleanup.
func StartProcessor(t *testing.T, ctx context.Context, cfg *config.Config, configPath string) context.CancelFunc {
	t.Helper()

	p, err := processor.New(ctx, cfg, configPath, metrics.DefaultMetrics)
	if err != nil {
		t.Fatal("e2e; failed to init processor; error: ", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- p.Run(runCtx)
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)

	return stop
}

// WaitForStreamCaughtUp polls pg_replication_slots until the slot's
// confirmed_flush_lsn reaches the current WAL position, i.e. the consumer
// has confirmed everything written so far.
func WaitForStreamCaughtUp(t *testing.T, ctx context.Context, client *postgresql.Client, slotName string) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := client.Query(ctx,
			`SELECT confirmed_flush_lsn >= pg_current_wal_lsn()
			 FROM pg_replication_slots WHERE slot_name = $1`, slotName)
		if err == nil && len(rows) == 1 && string(rows[0][0]) == "t" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("e2e; sync; replication slot %q did not catch up with WAL within timeout", slotName)
}

// SetupTable creates (and truncates) the table used by a sync test.
// Publication, slot and index are managed by the prepare command.
func SetupTable(t *testing.T, ctx context.Context, client *postgresql.Client, table string) {
	t.Helper()

	MustExec(t, ctx, client,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id int primary key, name text)", postgresql.QuoteQualifiedIdent(table)))
	MustExec(t, ctx, client, fmt.Sprintf("TRUNCATE TABLE %s", postgresql.QuoteQualifiedIdent(table)))
}

func InsertRow(t *testing.T, ctx context.Context, client *postgresql.Client, table string, id int, name string) {
	t.Helper()
	MustExec(t, ctx, client,
		fmt.Sprintf("INSERT INTO %s (id, name) VALUES ($1, $2)", postgresql.QuoteQualifiedIdent(table)),
		fmt.Sprintf("%d", id), name)
}

func UpdateRow(t *testing.T, ctx context.Context, client *postgresql.Client, table string, id int, name string) {
	t.Helper()
	MustExec(t, ctx, client,
		fmt.Sprintf("UPDATE %s SET name = $1 WHERE id = $2", postgresql.QuoteQualifiedIdent(table)),
		name, fmt.Sprintf("%d", id))
}

func DeleteRow(t *testing.T, ctx context.Context, client *postgresql.Client, table string, id int) {
	t.Helper()
	MustExec(t, ctx, client,
		fmt.Sprintf("DELETE FROM %s WHERE id = $1", postgresql.QuoteQualifiedIdent(table)),
		fmt.Sprintf("%d", id))
}

// MustExec executes a statement with optional text parameters, failing the
// test on error. Identifiers must be quoted into the SQL text (see
// postgresql.QuoteQualifiedIdent); values are passed as parameters.
func MustExec(t *testing.T, ctx context.Context, client *postgresql.Client, sql string, args ...string) {
	t.Helper()

	if err := client.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("e2e; failed to execute %q: %v", sql, err)
	}
}
