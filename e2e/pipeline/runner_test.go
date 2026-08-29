//go:build e2e

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const rowCount = 240

// runPipelineTest drives one full cycle of the pipeline under test: it
// provisions an isolated table/publication/slot/index, starts syncgo (as a
// binary or a docker container per mode), writes a large batch of diverse
// data into PostgreSQL, mutates and deletes a subset of it, and verifies the
// search engine ends up with exactly the expected, correct documents.
func runPipelineTest(t *testing.T, mode runMode, eng engineKind, artifact, network string) {
	t.Helper()
	ctx := context.Background()

	suffix := shortID()
	table := "pipeline_" + suffix
	pub := "pub_" + suffix
	slot := "slot_" + suffix
	index := "idx-" + suffix
	containerName := "syncgo-e2e-" + suffix

	pgConn, err := pgx.Connect(ctx, adminDSN(pgHostPort))
	if err != nil {
		t.Fatalf("e2e; pipeline; failed to connect to postgres: %v", err)
	}
	// Registered before cleanupPostgres below, so it runs last (LIFO): a plain
	// `defer` here would close the connection before any t.Cleanup runs, since
	// defers unwind on Goexit/return before the testing framework invokes
	// t.Cleanup callbacks at all.
	t.Cleanup(func() { pgConn.Close(context.Background()) })

	if err := createTable(ctx, pgConn, table); err != nil {
		t.Fatalf("e2e; pipeline; failed to create table %q: %v", table, err)
	}
	if err := createPublication(ctx, pgConn, pub, table); err != nil {
		t.Fatalf("e2e; pipeline; failed to create publication %q: %v", pub, err)
	}
	t.Cleanup(func() { cleanupPostgres(pgConn, table, pub, slot) })

	cfgPath := writeConfig(t, mode, eng, pub, slot, index)

	a := newApp(mode, artifact, network, cfgPath, containerName)
	if err := a.Start(ctx); err != nil {
		t.Fatalf("e2e; pipeline; failed to start syncgo: %v", err)
	}
	t.Cleanup(func() { a.Stop(context.Background()) })
	// Registered after a.Stop's cleanup, so it runs first (LIFO) and can still
	// read logs from the still-running process/container on failure.
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("e2e; pipeline; app logs:\n%s", a.Logs())
		}
	})

	waitForAppLog(t, a, `"logical replication started"`, 30*time.Second)

	rows := generateRows(rowCount)
	if err := insertRows(ctx, pgConn, table, rows); err != nil {
		t.Fatalf("e2e; pipeline; failed to insert rows: %v", err)
	}
	if err := updateSubset(ctx, pgConn, table, rows, 10); err != nil {
		t.Fatalf("e2e; pipeline; failed to update rows: %v", err)
	}
	deleted, err := deleteSubset(ctx, pgConn, table, rows, 15)
	if err != nil {
		t.Fatalf("e2e; pipeline; failed to delete rows: %v", err)
	}

	user, pass := credentials(eng)
	sc := newSearchClient(verifySearchAddr(eng), index, user, pass)
	t.Cleanup(func() { sc.deleteIndex(context.Background()) })

	verifySync(t, sc, rows, deleted)
}
