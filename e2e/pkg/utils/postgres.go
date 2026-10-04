//go:build e2e

// Package utils holds test helpers shared by the e2e suites in e2e/elastic
// and e2e/opensearch, so each doesn't have to duplicate the same PostgreSQL
// and search-engine plumbing.
package utils

import (
	"context"
	"fmt"
	"testing"

	"github.com/syncgo/syncgo/pkg/postgresql"

	"github.com/jackc/pgx/v5/pgconn"
)

// SetupTable creates (and truncates) the table used by a sync test, and
// creates its publication if it doesn't already exist.
func SetupTable(t *testing.T, ctx context.Context, pgCfg postgresql.Config, table, publication string) {
	t.Helper()

	conn, err := postgresql.NewStandard(ctx, pgCfg)
	if err != nil {
		t.Fatal("e2e; sync; failed to connect to postgres; error: ", err)
	}
	defer conn.Close(ctx)

	MustExec(t, ctx, conn, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id int primary key, name text)`, table))
	MustExec(t, ctx, conn, fmt.Sprintf(`TRUNCATE TABLE %s`, table))

	exists, err := postgresql.PublicationExists(ctx, conn, publication)
	if err != nil {
		t.Fatal("e2e; sync; failed to check publication existence; error: ", err)
	}
	if !exists {
		MustExec(t, ctx, conn, fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`, publication, table))
	}
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
