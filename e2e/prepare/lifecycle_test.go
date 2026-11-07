//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/syncgo/syncgo/e2e/pkg/utils"
)

const lifecycleTable = "sync_lifecycle_rows"

// TestPrepareRun_SnapshotLifecycle uses the dedicated lifecycle config, so
// its slot/publication/index do not clash with other suites running in the
// same CI job.
//
// It checks that no data is lost or stale across the full syncgo lifecycle,
// event by event:
//
//  1. rows written BEFORE prepare must be in the search engine after the
//     initial copy from the slot's exported snapshot;
//  2. inserts and updates right after the copy must overwrite the snapshot
//     values via the replication stream (fresh wins over stale copy);
//  3. after a clean stop and restart, rows written/updated/deleted while
//     syncgo was down must be applied from the buffered slot — the restarted
//     run resumes from the persisted consistent point, not from "now";
//  4. the restarted process keeps streaming normally.
//
// Both runs share one state file next to the config, so the restart path is
// the same as in production.
func TestPrepareRun_SnapshotLifecycle(t *testing.T) {
	ctx := context.Background()

	cfg := utils.LoadConfigFrom(t, "config_lifecycle.yaml")
	client := utils.NewClient(t, ctx, utils.PgConfigFrom(cfg))
	address := cfg.SearchEngine.Address
	index := cfg.SearchEngine.Index

	utils.SetupTable(t, ctx, client, lifecycleTable)

	// Phase 1: data written before prepare is copied from the snapshot.
	utils.InsertRow(t, ctx, client, lifecycleTable, 1, "before-prepare")
	utils.InsertRow(t, ctx, client, lifecycleTable, 2, "before-prepare")

	stop := utils.RunProcessor(t, ctx, cfg, "config_lifecycle.yaml")

	utils.AssertDocument(t, address, index, "1", "before-prepare")
	utils.AssertDocument(t, address, index, "2", "before-prepare")

	// Phase 2: updates right after the copy must win over the snapshot
	// values — the stream replays them after the copy is done.
	utils.InsertRow(t, ctx, client, lifecycleTable, 3, "after-copy-insert")
	utils.UpdateRow(t, ctx, client, lifecycleTable, 2, "updated-after-copy")

	utils.AssertDocument(t, address, index, "3", "after-copy-insert")
	utils.AssertDocument(t, address, index, "2", "updated-after-copy")

	// Phase 3: full restart. Stop syncgo cleanly — the stop is a barrier:
	// everything written after it is later in the WAL than anything the
	// first run consumed. Replay of not-yet-confirmed events is idempotent
	// (index/delete), so the resumed run applies them safely. Let WAL
	// accumulate with no consumer, then start a fresh run from the same
	// state file.
	stop()

	utils.InsertRow(t, ctx, client, lifecycleTable, 4, "while-syncgo-down")
	utils.UpdateRow(t, ctx, client, lifecycleTable, 1, "updated-while-down")
	utils.DeleteRow(t, ctx, client, lifecycleTable, 3)

	utils.StartProcessor(t, ctx, cfg, "config_lifecycle.yaml")

	// Every change made while syncgo was down must be replayed from the
	// slot: the insert and update land, the delete removes the document.
	utils.AssertDocument(t, address, index, "4", "while-syncgo-down")
	utils.AssertDocument(t, address, index, "1", "updated-while-down")
	utils.WaitForDocumentAbsent(t, address, index, "3", 15*time.Second)

	// Phase 4: the restarted process keeps streaming normally.
	utils.InsertRow(t, ctx, client, lifecycleTable, 5, "after-restart")
	utils.AssertDocument(t, address, index, "5", "after-restart")
}
