//go:build e2e

// Package utils holds test helpers shared by the e2e suites in e2e/elastic
// and e2e/opensearch, so each doesn't have to duplicate the same PostgreSQL
// and search-engine plumbing.
package utils

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/syncgo/syncgo/internal/processor"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/postgresql"

	"github.com/jackc/pgx/v5/pgconn"
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

// RunProcessor runs prepare (creates publication, slot and index) and then
// starts the sync process, stopping it on test cleanup. Returns the cancel
// function for the run context.
func RunProcessor(t *testing.T, ctx context.Context, cfg *config.Config) context.CancelFunc {
	t.Helper()

	if err := processor.Prepare(ctx, cfg, metrics.DefaultMetrics); err != nil {
		t.Fatal("e2e; prepare failed; error: ", err)
	}

	p, err := processor.New(ctx, cfg, metrics.DefaultMetrics)
	if err != nil {
		t.Fatal("e2e; failed to init processor; error: ", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- p.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	return cancel
}

// SetupTable creates (and truncates) the table used by a sync test.
// Publication, slot and index are managed by the prepare command.
func SetupTable(t *testing.T, ctx context.Context, pgCfg postgresql.Config, table string) {
	t.Helper()

	conn, err := postgresql.NewStandard(ctx, pgCfg)
	if err != nil {
		t.Fatal("e2e; sync; failed to connect to postgres; error: ", err)
	}
	defer conn.Close(ctx)

	MustExec(t, ctx, conn, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id int primary key, name text)`, table))
	MustExec(t, ctx, conn, fmt.Sprintf(`TRUNCATE TABLE %s`, table))
}

func InsertRow(t *testing.T, ctx context.Context, pgCfg postgresql.Config, table string, id int, name string) {
	t.Helper()
	ExecSQL(t, ctx, pgCfg, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (%d, '%s')`, table, id, name))
}

func UpdateRow(t *testing.T, ctx context.Context, pgCfg postgresql.Config, table string, id int, name string) {
	t.Helper()
	ExecSQL(t, ctx, pgCfg, fmt.Sprintf(`UPDATE %s SET name = '%s' WHERE id = %d`, table, name, id))
}

func DeleteRow(t *testing.T, ctx context.Context, pgCfg postgresql.Config, table string, id int) {
	t.Helper()
	ExecSQL(t, ctx, pgCfg, fmt.Sprintf(`DELETE FROM %s WHERE id = %d`, table, id))
}

func ExecSQL(t *testing.T, ctx context.Context, pgCfg postgresql.Config, sql string) {
	t.Helper()

	conn, err := postgresql.NewStandard(ctx, pgCfg)
	if err != nil {
		t.Fatal("e2e; sync; failed to connect to postgres; error: ", err)
	}
	defer conn.Close(ctx)

	MustExec(t, ctx, conn, sql)
}

func MustExec(t *testing.T, ctx context.Context, conn *pgconn.PgConn, sql string) {
	t.Helper()

	if _, err := conn.Exec(ctx, sql).ReadAll(); err != nil {
		t.Fatalf("e2e; sync; failed to execute %q: %v", sql, err)
	}
}
