//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/syncgo/syncgo/e2e/pkg/utils"
	"github.com/syncgo/syncgo/internal/processor"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/state"

	"github.com/jackc/pglogrepl"
)

const (
	prepareTable   = "sync_prepare_rows"
	prepareIndex   = "sync_prepare_index"
	prepareTimeout = 30 * time.Second
)

// prepareConfig builds a config pointing prepare at the dedicated table,
// slot, publication and index.
func prepareConfig() *config.Config {
	return &config.Config{
		PostgreSQL: config.PostgreSQLConfig{
			User:            "postgres",
			Password:        "postgres",
			Host:            "localhost",
			Port:            "5432",
			Database:        "postgres",
			PublicationName: "sync_prepare_pub",
			SlotName:        "sync_prepare_slot",
			Tables:          []string{"public." + prepareTable},
		},
		SearchEngine: config.SearchEngineConfig{
			Name:    "elasticsearch",
			Address: "http://localhost:9200",
			Index:   prepareIndex,
		},
	}
}

func TestPrepare_CreatesSlotPublicationAndIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), prepareTimeout)
	defer cancel()

	cfg := prepareConfig()
	client := utils.NewClient(t, ctx, utils.PgConfigFrom(cfg))
	statePath := filepath.Join(t.TempDir(), "syncgo_state.json")

	// The table referenced by the publication must exist beforehand.
	utils.SetupTable(t, ctx, client, prepareTable)

	// First run creates the slot, publication and index.
	if err := processor.Prepare(ctx, cfg, statePath, metrics.DefaultMetrics); err != nil {
		t.Fatal("e2e; prepare; first run failed; error: ", err)
	}

	// Second run is idempotent: existing objects are left as is, no error.
	if err := processor.Prepare(ctx, cfg, statePath, metrics.DefaultMetrics); err != nil {
		t.Fatal("e2e; prepare; second run (idempotency) failed; error: ", err)
	}

	slotExists, err := client.ReplicationSlotExists(ctx, cfg.PostgreSQL.SlotName)
	if err != nil {
		t.Fatal("e2e; prepare; failed to check slot; error: ", err)
	}
	if !slotExists {
		t.Fatalf("e2e; prepare; replication slot %q was not created", cfg.PostgreSQL.SlotName)
	}

	pubExists, err := client.PublicationExists(ctx, cfg.PostgreSQL.PublicationName)
	if err != nil {
		t.Fatal("e2e; prepare; failed to check publication; error: ", err)
	}
	if !pubExists {
		t.Fatalf("e2e; prepare; publication %q was not created", cfg.PostgreSQL.PublicationName)
	}

	if ok := indexExists(t, cfg.SearchEngine.Address, prepareIndex); !ok {
		t.Fatalf("e2e; prepare; index %q was not created", prepareIndex)
	}

	// Teardown.
	if err := client.DropPublication(ctx, cfg.PostgreSQL.PublicationName); err != nil {
		t.Fatal("e2e; prepare; failed to drop publication; error: ", err)
	}
	if err := client.DropReplicationSlot(ctx, cfg.PostgreSQL.SlotName); err != nil {
		t.Fatal("e2e; prepare; failed to drop replication slot; error: ", err)
	}
}

// TestPrepare_InitialCopyFromSnapshot checks that prepare copies the data
// present at slot creation into the search engine and persists the
// consistent point in the state file.
func TestPrepare_InitialCopyFromSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), prepareTimeout)
	defer cancel()

	cfg := prepareConfig()
	client := utils.NewClient(t, ctx, utils.PgConfigFrom(cfg))
	statePath := filepath.Join(t.TempDir(), "syncgo_state.json")

	utils.SetupTable(t, ctx, client, prepareTable)
	utils.InsertRow(t, ctx, client, prepareTable, 1, "copy-foo")
	utils.InsertRow(t, ctx, client, prepareTable, 2, "copy-bar")

	if err := processor.Prepare(ctx, cfg, statePath, metrics.DefaultMetrics); err != nil {
		t.Fatal("e2e; prepare; run failed; error: ", err)
	}

	// Rows present at slot creation time must be in the search engine.
	utils.AssertDocument(t, cfg.SearchEngine.Address, prepareIndex, "1", "copy-foo")
	utils.AssertDocument(t, cfg.SearchEngine.Address, prepareIndex, "2", "copy-bar")

	// The state file must contain the slot's consistent point.
	st, err := state.Load(statePath)
	if err != nil {
		t.Fatal("e2e; prepare; failed to load state file; error: ", err)
	}
	if st == nil {
		t.Fatal("e2e; prepare; state file was not written")
	}
	if st.SlotName != cfg.PostgreSQL.SlotName {
		t.Fatalf("e2e; prepare; state slot name %q, want %q", st.SlotName, cfg.PostgreSQL.SlotName)
	}
	if st.ConsistentPoint == "" {
		t.Fatal("e2e; prepare; state file has empty consistent point")
	}
	if _, err := pglogrepl.ParseLSN(st.ConsistentPoint); err != nil {
		t.Fatalf("e2e; prepare; consistent point %q is not a valid LSN; error: %v", st.ConsistentPoint, err)
	}

	// Teardown.
	if err := client.DropPublication(ctx, cfg.PostgreSQL.PublicationName); err != nil {
		t.Fatal("e2e; prepare; failed to drop publication; error: ", err)
	}
	if err := client.DropReplicationSlot(ctx, cfg.PostgreSQL.SlotName); err != nil {
		t.Fatal("e2e; prepare; failed to drop replication slot; error: ", err)
	}
}

func indexExists(t *testing.T, address, index string) bool {
	t.Helper()

	resp, err := utils.DoRequest(t, "HEAD", fmt.Sprintf("%s/%s", address, index))
	if err != nil {
		t.Fatal("e2e; prepare; failed to check index; error: ", err)
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200
}
