//go:build e2e

// Package e2e covers the PostgreSQL admin features of pkg/postgresql:
// publication and replication slot management, including idempotency,
// drop (teardown) and identifier quoting.
package e2e

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/syncgo/syncgo/e2e/pkg/utils"
	"github.com/syncgo/syncgo/pkg/postgresql"
)

const pgTestTimeout = 30 * time.Second

func newPgClient(t *testing.T) (*postgresql.Client, *postgresql.Config) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), pgTestTimeout)
	t.Cleanup(cancel)

	cfg := utils.LoadConfigFrom(t, "../config.yaml")
	pgCfg := utils.PgConfigFrom(cfg)

	return utils.NewClient(t, ctx, pgCfg), &pgCfg
}

func TestPublicationExists(t *testing.T) {
	client, _ := newPgClient(t)
	ctx := context.Background()

	const (
		pub      = "syncgo_e2e_exists_pub"
		pubTable = "syncgo_e2e_exists_rows"
	)
	t.Cleanup(func() { _ = client.DropPublication(ctx, pub) })

	// The table referenced by the publication must exist beforehand.
	utils.SetupTable(t, ctx, client, pubTable)
	t.Cleanup(func() {
		_ = client.Exec(ctx, "DROP TABLE IF EXISTS "+postgresql.QuoteQualifiedIdent(pubTable))
	})

	exists, err := client.PublicationExists(ctx, pub)
	if err != nil {
		t.Fatal("e2e; postgres; failed to check publication; error: ", err)
	}
	if exists {
		t.Fatalf("e2e; postgres; publication %q should not exist initially", pub)
	}

	if err := client.EnsurePublication(ctx, pub, []postgresql.PublicationTable{{Table: "public." + pubTable}}); err != nil {
		t.Fatal("e2e; postgres; failed to ensure publication; error: ", err)
	}

	exists, err = client.PublicationExists(ctx, pub)
	if err != nil {
		t.Fatal("e2e; postgres; failed to check publication; error: ", err)
	}
	if !exists {
		t.Fatalf("e2e; postgres; publication %q should exist after EnsurePublication", pub)
	}
}

func TestEnsurePublication_Idempotent(t *testing.T) {
	client, _ := newPgClient(t)
	ctx := context.Background()

	const (
		pub      = "syncgo_e2e_idempotent_pub"
		pubTable = "syncgo_e2e_idempotent_rows"
	)

	t.Cleanup(func() { _ = client.DropPublication(ctx, pub) })

	setupPubTable(t, ctx, client, pubTable)

	if err := client.EnsurePublication(ctx, pub, []postgresql.PublicationTable{{Table: "public." + pubTable}}); err != nil {
		t.Fatal("e2e; postgres; first EnsurePublication failed; error: ", err)
	}
	// Second call on an existing publication must not fail or change it.
	if err := client.EnsurePublication(ctx, pub, []postgresql.PublicationTable{{Table: "public." + pubTable}}); err != nil {
		t.Fatal("e2e; postgres; second EnsurePublication failed; error: ", err)
	}

	tables, err := client.PublicationTables(ctx, pub)
	if err != nil {
		t.Fatal("e2e; postgres; failed to list publication tables; error: ", err)
	}
	if len(tables) != 1 || tables[0] != "public."+pubTable {
		t.Fatalf("e2e; postgres; unexpected publication tables: %v", tables)
	}
}

// setupPubTable creates the table referenced by a publication, dropping it
// on test cleanup.
func setupPubTable(t *testing.T, ctx context.Context, client *postgresql.Client, table string) {
	t.Helper()

	utils.SetupTable(t, ctx, client, table)
	t.Cleanup(func() {
		_ = client.Exec(ctx, "DROP TABLE IF EXISTS "+postgresql.QuoteQualifiedIdent(table))
	})
}

func TestEnsurePublication_SchemaQualifiedAndQuoting(t *testing.T) {
	client, _ := newPgClient(t)
	ctx := context.Background()

	const (
		pub      = "syncgo_e2e_quoting_pub"
		pubTable = "syncgo_e2e_quoting_rows"
	)
	t.Cleanup(func() { _ = client.DropPublication(ctx, pub) })

	setupPubTable(t, ctx, client, pubTable)

	if err := client.EnsurePublication(ctx, pub, []postgresql.PublicationTable{{Table: "public." + pubTable}}); err != nil {
		t.Fatal("e2e; postgres; EnsurePublication failed; error: ", err)
	}

	tables, err := client.PublicationTables(ctx, pub)
	if err != nil {
		t.Fatal("e2e; postgres; failed to list publication tables; error: ", err)
	}
	if !slices.Equal(tables, []string{"public." + pubTable}) {
		t.Fatalf("e2e; postgres; unexpected publication tables: %v", tables)
	}
}

func TestDropPublication(t *testing.T) {
	client, _ := newPgClient(t)
	ctx := context.Background()

	const (
		pub      = "syncgo_e2e_drop_pub"
		pubTable = "syncgo_e2e_drop_rows"
	)

	setupPubTable(t, ctx, client, pubTable)

	if err := client.EnsurePublication(ctx, pub, []postgresql.PublicationTable{{Table: "public." + pubTable}}); err != nil {
		t.Fatal("e2e; postgres; EnsurePublication failed; error: ", err)
	}

	if err := client.DropPublication(ctx, pub); err != nil {
		t.Fatal("e2e; postgres; DropPublication failed; error: ", err)
	}

	exists, err := client.PublicationExists(ctx, pub)
	if err != nil {
		t.Fatal("e2e; postgres; failed to check publication; error: ", err)
	}
	if exists {
		t.Fatalf("e2e; postgres; publication %q should not exist after drop", pub)
	}

	// Dropping a non-existent publication is a no-op, not an error.
	if err := client.DropPublication(ctx, pub); err != nil {
		t.Fatal("e2e; postgres; second DropPublication failed; error: ", err)
	}
}

func TestEnsurePublication_RowFilter(t *testing.T) {
	client, _ := newPgClient(t)
	ctx := context.Background()

	const (
		pub      = "syncgo_e2e_filter_pub"
		pubTable = "syncgo_e2e_filter_rows"
	)
	t.Cleanup(func() { _ = client.DropPublication(ctx, pub) })

	setupPubTable(t, ctx, client, pubTable)

	tables := []postgresql.PublicationTable{
		{Table: "public." + pubTable, Filter: "id > 100"},
	}
	if err := client.EnsurePublication(ctx, pub, tables); err != nil {
		t.Fatal("e2e; postgres; EnsurePublication with row filter failed; error: ", err)
	}

	// The stored row filter must match the requested expression. The
	// filtering itself is performed by PostgreSQL.
	rows, err := client.Query(ctx,
		"SELECT pg_get_expr(prqual, prrelid) FROM pg_publication_rel WHERE prpubid = (SELECT oid FROM pg_publication WHERE pubname = $1)",
		pub)
	if err != nil {
		t.Fatal("e2e; postgres; failed to query publication row filter; error: ", err)
	}
	if len(rows) != 1 || string(rows[0][0]) != "id > 100" {
		t.Fatalf("e2e; postgres; unexpected publication row filter: %v", rows)
	}
}

func TestDropPublication_AllowsRecreate(t *testing.T) {
	client, _ := newPgClient(t)
	ctx := context.Background()

	const (
		pub      = "syncgo_e2e_recreate_pub"
		pubTable = "syncgo_e2e_recreate_rows"
	)
	t.Cleanup(func() { _ = client.DropPublication(ctx, pub) })

	setupPubTable(t, ctx, client, pubTable)

	if err := client.EnsurePublication(ctx, pub, []postgresql.PublicationTable{{Table: "public." + pubTable}}); err != nil {
		t.Fatal("e2e; postgres; first EnsurePublication failed; error: ", err)
	}
	if err := client.DropPublication(ctx, pub); err != nil {
		t.Fatal("e2e; postgres; DropPublication failed; error: ", err)
	}
	// After drop, Ensure must create the publication again.
	if err := client.EnsurePublication(ctx, pub, []postgresql.PublicationTable{{Table: "public." + pubTable}}); err != nil {
		t.Fatal("e2e; postgres; EnsurePublication after drop failed; error: ", err)
	}
}

func TestReplicationSlotExists(t *testing.T) {
	client, _ := newPgClient(t)
	ctx := context.Background()

	const slot = "syncgo_e2e_exists_slot"
	t.Cleanup(func() { _ = client.DropReplicationSlot(ctx, slot) })

	exists, err := client.ReplicationSlotExists(ctx, slot)
	if err != nil {
		t.Fatal("e2e; postgres; failed to check slot; error: ", err)
	}
	if exists {
		t.Fatalf("e2e; postgres; slot %q should not exist initially", slot)
	}

	if err := client.EnsureReplicationSlot(ctx, slot); err != nil {
		t.Fatal("e2e; postgres; failed to ensure slot; error: ", err)
	}

	exists, err = client.ReplicationSlotExists(ctx, slot)
	if err != nil {
		t.Fatal("e2e; postgres; failed to check slot; error: ", err)
	}
	if !exists {
		t.Fatalf("e2e; postgres; slot %q should exist after EnsureReplicationSlot", slot)
	}
}

func TestEnsureReplicationSlot_Idempotent(t *testing.T) {
	client, _ := newPgClient(t)
	ctx := context.Background()

	const slot = "syncgo_e2e_idempotent_slot"
	t.Cleanup(func() { _ = client.DropReplicationSlot(ctx, slot) })

	if err := client.EnsureReplicationSlot(ctx, slot); err != nil {
		t.Fatal("e2e; postgres; first EnsureReplicationSlot failed; error: ", err)
	}
	if err := client.EnsureReplicationSlot(ctx, slot); err != nil {
		t.Fatal("e2e; postgres; second EnsureReplicationSlot failed; error: ", err)
	}
}

func TestDropReplicationSlot(t *testing.T) {
	client, _ := newPgClient(t)
	ctx := context.Background()

	const slot = "syncgo_e2e_drop_slot"

	if err := client.EnsureReplicationSlot(ctx, slot); err != nil {
		t.Fatal("e2e; postgres; EnsureReplicationSlot failed; error: ", err)
	}

	if err := client.DropReplicationSlot(ctx, slot); err != nil {
		t.Fatal("e2e; postgres; DropReplicationSlot failed; error: ", err)
	}

	exists, err := client.ReplicationSlotExists(ctx, slot)
	if err != nil {
		t.Fatal("e2e; postgres; failed to check slot; error: ", err)
	}
	if exists {
		t.Fatalf("e2e; postgres; slot %q should not exist after drop", slot)
	}

	// Dropping a non-existent slot is a no-op, not an error.
	if err := client.DropReplicationSlot(ctx, slot); err != nil {
		t.Fatal("e2e; postgres; second DropReplicationSlot failed; error: ", err)
	}
}

func TestDropReplicationSlot_AllowsRecreate(t *testing.T) {
	client, _ := newPgClient(t)
	ctx := context.Background()

	const slot = "syncgo_e2e_recreate_slot"
	t.Cleanup(func() { _ = client.DropReplicationSlot(ctx, slot) })

	if err := client.EnsureReplicationSlot(ctx, slot); err != nil {
		t.Fatal("e2e; postgres; first EnsureReplicationSlot failed; error: ", err)
	}
	if err := client.DropReplicationSlot(ctx, slot); err != nil {
		t.Fatal("e2e; postgres; DropReplicationSlot failed; error: ", err)
	}
	// After drop, Ensure must create the slot again.
	if err := client.EnsureReplicationSlot(ctx, slot); err != nil {
		t.Fatal("e2e; postgres; EnsureReplicationSlot after drop failed; error: ", err)
	}
}

func TestExec_WithParams(t *testing.T) {
	client, _ := newPgClient(t)
	ctx := context.Background()

	const execTable = "syncgo_e2e_exec_rows"

	utils.SetupTable(t, ctx, client, execTable)
	t.Cleanup(func() {
		_ = client.Exec(ctx, "DROP TABLE IF EXISTS "+postgresql.QuoteQualifiedIdent(execTable))
	})

	// Parameterized statements reuse the cached prepared statement on
	// repeated calls.
	utils.InsertRow(t, ctx, client, execTable, 1, "foo")
	utils.InsertRow(t, ctx, client, execTable, 2, "bar")
	utils.UpdateRow(t, ctx, client, execTable, 1, "foo-updated")
	utils.DeleteRow(t, ctx, client, execTable, 2)

	rows, err := client.Query(ctx, "SELECT id, name FROM "+postgresql.QuoteQualifiedIdent(execTable)+" ORDER BY id")
	if err != nil {
		t.Fatal("e2e; postgres; query failed; error: ", err)
	}
	if len(rows) != 1 {
		t.Fatalf("e2e; postgres; expected 1 row, got %d", len(rows))
	}
	if string(rows[0][0]) != "1" || string(rows[0][1]) != "foo-updated" {
		t.Fatalf("e2e; postgres; unexpected row: %v", rows[0])
	}
}
