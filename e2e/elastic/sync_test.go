//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/syncgo/syncgo/e2e/pkg/utils"
	"github.com/syncgo/syncgo/internal/processor"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/postgresql"

	"github.com/prometheus/client_golang/prometheus"
)

const syncElasticTable = "sync_elastic_rows"

func TestElasticsearchSync_ReplicatesRowsFromPostgres(t *testing.T) {
	ctx := context.Background()

	cfg, err := config.LoadFromYAML("config_sync_elasticsearch.yaml")
	if err != nil {
		t.Fatal("e2e; sync; elasticsearch; failed to load config; error: ", err)
	}

	pgCfg := postgresql.Config{
		User:     cfg.PostgreSQL.User,
		Password: cfg.PostgreSQL.Password,
		Host:     cfg.PostgreSQL.Host,
		Port:     cfg.PostgreSQL.Port,
		Database: cfg.PostgreSQL.Database,
	}

	utils.SetupTable(t, ctx, pgCfg, syncElasticTable, cfg.PostgreSQL.PublicationName)

	p, err := processor.New(ctx, cfg, metrics.New(prometheus.NewRegistry()))
	if err != nil {
		t.Fatal("e2e; sync; elasticsearch; failed to init processor; error: ", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- p.Run(runCtx)
	}()
	defer func() {
		runCancel()
		<-done
	}()

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
