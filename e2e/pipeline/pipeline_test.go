//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/syncgo/syncgo/e2e/pkg/utils"
)

const (
	rowCount             = 500
	waitForAbsentTTL     = 60 * time.Second
	pipelineTable        = "test"
	filterPipelineTable  = "syncgo_filter_pipeline"
	filterPipelineIDBase = 1_000_000
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

// TestPipeline_RowFilterSync exercises the filtered table of the shared
// publication: the binary (or docker container) under test created the
// publication with a row filter on public.syncgo_filter_pipeline (see
// e2e/config.yaml), so PostgreSQL must drop filtered-out rows from the
// replication stream before they reach the search engine. Only inserts and
// assertions happen here — no processor is started by the test itself.
func TestPipeline_RowFilterSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := utils.LoadConfig(t)
	client := utils.NewClient(t, ctx, utils.PgConfigFrom(cfg))

	// Separate table (declared in e2e/init.sql, filtered in the shared
	// publication) writing into the shared index. Row ids are shifted past
	// the ids used by the plain test table to keep the documents distinct.
	utils.SetupTable(t, ctx, client, filterPipelineTable)

	run := time.Now().UnixNano()
	publishedName := fmt.Sprintf("published-%d", run)

	utils.InsertRow(t, ctx, client, filterPipelineTable, filterPipelineIDBase+1, "hidden")
	utils.InsertRow(t, ctx, client, filterPipelineTable, filterPipelineIDBase+2, publishedName)

	utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, fmt.Sprint(filterPipelineIDBase+2), publishedName)
	// The hidden row is excluded by the publication row filter and must
	// never reach the index.
	utils.WaitForDocumentAbsent(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, fmt.Sprint(filterPipelineIDBase+1), waitForAbsentTTL)
}
