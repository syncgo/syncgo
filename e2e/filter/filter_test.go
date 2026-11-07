//go:build e2e

// Package e2e covers the full sync pipeline for a publication with a row
// filter: the processor runs in-process (like the e2e job, not the
// binary/docker jobs) and PostgreSQL must drop filtered-out rows from the
// replication stream before they reach the search engine.
package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/syncgo/syncgo/e2e/pkg/utils"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/postgresql"
)

const (
	filterTable       = "syncgo_filter"
	filterIndex       = "syncgo_filter_index"
	filterPublication = "syncgo_filter_publication"
	filterSlot        = "syncgo_filter_slot"
	waitForAbsentTTL  = 60 * time.Second
)

func filteredConfig(base *config.Config) *config.Config {
	filtered := *base
	filtered.PostgreSQL.Tables = []config.PublicationTable{
		{Table: "public." + filterTable, Filter: "name != 'hidden'"},
	}
	// Distinct publication, slot and index so the test does not clash with
	// the shared-configuration tests running in parallel.
	filtered.PostgreSQL.PublicationName = filterPublication
	filtered.PostgreSQL.SlotName = filterSlot
	filtered.SearchEngine.Index = filterIndex

	return &filtered
}

// TestPipeline_RowFilter syncs a publication created with a row filter: only
// rows satisfying the filter must reach the search engine, PostgreSQL drops
// the rest from the replication stream.
func TestPipeline_RowFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := filteredConfig(utils.LoadConfig(t))
	client := utils.NewClient(t, ctx, utils.PgConfigFrom(cfg))

	utils.SetupTable(t, ctx, client, filterTable)
	// The row filter references column "name", which is not part of the
	// replica identity (the primary key). Publishing UPDATE/DELETE for such
	// a table requires REPLICA IDENTITY FULL, otherwise PostgreSQL rejects
	// the update (SQLSTATE 42P10).
	utils.MustExec(t, ctx, client,
		fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", postgresql.QuoteQualifiedIdent(filterTable)))

	t.Cleanup(func() {
		_ = client.DropPublication(ctx, cfg.PostgreSQL.PublicationName)
		_ = client.DropReplicationSlot(ctx, cfg.PostgreSQL.SlotName)
	})
	// An old index may still contain documents from a failed run, silently
	// breaking this test.
	if resp, err := utils.DoRequest(t, "DELETE", cfg.SearchEngine.Address+"/"+filterIndex); err == nil {
		_ = resp.Body.Close()
	}

	utils.RunProcessor(t, ctx, cfg)

	run := time.Now().UnixNano()
	publishedName := fmt.Sprintf("published-%d", run)
	// The filter is name != 'hidden', so the hidden row must use the exact
	// value excluded by the filter.
	hiddenName := "hidden"

	// The hidden row does not satisfy the filter and must never appear in
	// the index; the published row must be synced.
	utils.InsertRow(t, ctx, client, filterTable, 1, hiddenName)
	utils.InsertRow(t, ctx, client, filterTable, 2, publishedName)

	utils.AssertDocument(t, cfg.SearchEngine.Address, filterIndex, "2", publishedName)
	utils.WaitForDocumentAbsent(t, cfg.SearchEngine.Address, filterIndex, "1", waitForAbsentTTL)

	// Updating a published row into the filtered-out state must remove it
	// from the stream (delete is delivered for old row versions).
	utils.UpdateRow(t, ctx, client, filterTable, 2, hiddenName)
	utils.WaitForDocumentAbsent(t, cfg.SearchEngine.Address, filterIndex, "2", waitForAbsentTTL)
}
