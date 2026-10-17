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
	pgCfg := utils.PgConfigFrom(cfg)

	utils.SetupTable(t, ctx, pgCfg, pipelineTable)

	run := time.Now().UnixNano()
	names := make(map[int]string, rowCount)
	for i := 1; i <= rowCount; i++ {
		name := fmt.Sprintf("row-%d-%d-%s", run, i, varietyTag(i))
		names[i] = name
		utils.InsertRow(t, ctx, pgCfg, pipelineTable, i, name)
	}

	for id, name := range names {
		utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, fmt.Sprint(id), name)
	}
}

func TestPipeline_UpdateDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := utils.LoadConfig(t)
	pgCfg := utils.PgConfigFrom(cfg)

	utils.SetupTable(t, ctx, pgCfg, pipelineTable)

	run := time.Now().UnixNano()
	keepName := fmt.Sprintf("keep-%d", run)
	updateName := fmt.Sprintf("update-%d", run)
	deleteName := fmt.Sprintf("delete-%d", run)

	utils.InsertRow(t, ctx, pgCfg, pipelineTable, 1, keepName)
	utils.InsertRow(t, ctx, pgCfg, pipelineTable, 2, updateName)
	utils.InsertRow(t, ctx, pgCfg, pipelineTable, 3, deleteName)

	utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "2", updateName)
	utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "3", deleteName)

	updatedName := fmt.Sprintf("updated-%d", run)
	utils.UpdateRow(t, ctx, pgCfg, pipelineTable, 2, updatedName)
	utils.DeleteRow(t, ctx, pgCfg, pipelineTable, 3)

	utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "2", updatedName)
	utils.WaitForDocumentAbsent(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "3", waitForAbsentTTL)
}
