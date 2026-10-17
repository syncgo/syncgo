//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/syncgo/syncgo/e2e/pkg/utils"
)

const syncElasticTable = "sync_elastic_rows"

func TestElasticsearchSync_ReplicatesRowsFromPostgres(t *testing.T) {
	ctx := context.Background()

	cfg := utils.LoadConfigFrom(t, "config_sync_elasticsearch.yaml")
	pgCfg := utils.PgConfigFrom(cfg)

	utils.SetupTable(t, ctx, pgCfg, syncElasticTable)

	utils.RunProcessor(t, ctx, cfg)

	utils.InsertRow(t, ctx, pgCfg, syncElasticTable, 1, "foo")
	utils.InsertRow(t, ctx, pgCfg, syncElasticTable, 2, "bar")
	utils.InsertRow(t, ctx, pgCfg, syncElasticTable, 3, "baz")

	utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "1", "foo")
	utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "2", "bar")
	utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "3", "baz")

	utils.UpdateRow(t, ctx, pgCfg, syncElasticTable, 1, "foo-updated")
	utils.AssertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "1", "foo-updated")

	utils.DeleteRow(t, ctx, pgCfg, syncElasticTable, 2)
	utils.WaitForDocumentAbsent(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "2", 15*time.Second)
}
