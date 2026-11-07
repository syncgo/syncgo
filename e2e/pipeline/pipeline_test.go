//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/syncgo/syncgo/e2e/pkg/utils"
	"github.com/syncgo/syncgo/pkg/config"
)

const (
	rowCount         = 500
	waitForAbsentTTL = 60 * time.Second
	pipelineTable    = "test"
)

func varietyTag(i int) string {
	switch i % 4 {
	case 0:
		return "plain"
	case 1:
		return "with space"
	case 2:
		return "sÿmbøl-💾"
	default:
		return `quote"and\slash`
	}
}

func TestPipeline_InsertSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := utils.LoadConfig(t)
	client := utils.NewClient(t, ctx, utils.PgConfigFrom(cfg))

	utils.SetupTable(t, ctx, client, pipelineTable)

	run := time.Now().UnixNano()
	names := make(map[int]string, rowCount)
	for i := 1; i <= rowCount; i++ {
		name := fmt.Sprintf("row-%d-%d-%s", run, i, varietyTag(i))
		names[i] = name
		utils.InsertRow(t, ctx, client, pipelineTable, i, name)
	}

	for id, name := range names {
		utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, fmt.Sprint(id), name)
	}
}

func TestPipeline_UpdateDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := utils.LoadConfig(t)
	client := utils.NewClient(t, ctx, utils.PgConfigFrom(cfg))

	utils.SetupTable(t, ctx, client, pipelineTable)

	run := time.Now().UnixNano()
	keepName := fmt.Sprintf("keep-%d", run)
	updateName := fmt.Sprintf("update-%d", run)
	deleteName := fmt.Sprintf("delete-%d", run)

	utils.InsertRow(t, ctx, client, pipelineTable, 1, keepName)
	utils.InsertRow(t, ctx, client, pipelineTable, 2, updateName)
	utils.InsertRow(t, ctx, client, pipelineTable, 3, deleteName)

	utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "2", updateName)
	utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "3", deleteName)

	updatedName := fmt.Sprintf("updated-%d", run)
	utils.UpdateRow(t, ctx, client, pipelineTable, 2, updatedName)
	utils.DeleteRow(t, ctx, client, pipelineTable, 3)

	utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "2", updatedName)
	utils.WaitForDocumentAbsent(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "3", waitForAbsentTTL)
}

// TestPipeline_RowFilter syncs a publication created with a row filter: only
// rows satisfying the filter must reach the search engine, PostgreSQL drops
// the rest from the replication stream.
func TestPipeline_RowFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := utils.LoadConfig(t)
	const (
		filterTable = "syncgo_filter"
		filterIndex = "syncgo_filter_index"
	)
	filtered := *cfg
	filtered.PostgreSQL.Tables = []config.PublicationTable{
		{Table: "public." + filterTable, Filter: "name != 'hidden'"},
	}
	// Distinct publication, slot and index so the test does not clash with
	// the shared-configuration tests running in parallel.
	filtered.PostgreSQL.PublicationName = "syncgo_filter_publication"
	filtered.PostgreSQL.SlotName = "syncgo_filter_slot"
	filtered.SearchEngine.Index = filterIndex

	client := utils.NewClient(t, ctx, utils.PgConfigFrom(&filtered))

	utils.SetupTable(t, ctx, client, filterTable)
	t.Cleanup(func() {
		_ = client.DropPublication(ctx, filtered.PostgreSQL.PublicationName)
		_ = client.DropReplicationSlot(ctx, filtered.PostgreSQL.SlotName)
	})

	utils.RunProcessor(t, ctx, &filtered)

	run := time.Now().UnixNano()
	publishedName := fmt.Sprintf("published-%d", run)
	hiddenName := fmt.Sprintf("hidden-%d", run)

	// The hidden row does not satisfy the filter and must never appear in
	// the index; the published row must be synced.
	utils.InsertRow(t, ctx, client, filterTable, 1, hiddenName)
	utils.InsertRow(t, ctx, client, filterTable, 2, publishedName)

	utils.AssertDocument(t, filtered.SearchEngine.Address, filterIndex, "2", publishedName)
	utils.WaitForDocumentAbsent(t, filtered.SearchEngine.Address, filterIndex, "1", waitForAbsentTTL)

	// Updating a published row into the filtered-out state must remove it
	// from the stream (delete is delivered for old row versions).
	utils.UpdateRow(t, ctx, client, filterTable, 2, hiddenName)
	utils.WaitForDocumentAbsent(t, filtered.SearchEngine.Address, filterIndex, "2", waitForAbsentTTL)
}
