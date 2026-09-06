//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/syncgo/syncgo/internal/processor"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/postgresql"

	"github.com/jackc/pgx/v5/pgconn"
)

const syncOpensearchTable = "sync_opensearch_rows"

func TestOpensearchSync_ReplicatesRowsFromPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := config.LoadFromYAML("config_sync_opensearch.yaml")
	if err != nil {
		t.Fatal("e2e; sync; opensearch; failed to load config; error: ", err)
	}

	pgCfg := postgresql.Config{
		User:     cfg.PostgreSQL.User,
		Password: cfg.PostgreSQL.Password,
		Host:     cfg.PostgreSQL.Host,
		Port:     cfg.PostgreSQL.Port,
		Database: cfg.PostgreSQL.Database,
	}

	setupTable(t, ctx, pgCfg, syncOpensearchTable, cfg.PostgreSQL.PublicationName)

	p, err := processor.New(ctx, cfg, metrics.New())
	if err != nil {
		t.Fatal("e2e; sync; opensearch; failed to init processor; error: ", err)
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

	insertRow(t, ctx, pgCfg, syncOpensearchTable, 1, "foo")
	insertRow(t, ctx, pgCfg, syncOpensearchTable, 2, "bar")
	insertRow(t, ctx, pgCfg, syncOpensearchTable, 3, "baz")

	assertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "1", "foo")
	assertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "2", "bar")
	assertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "3", "baz")

	updateRow(t, ctx, pgCfg, syncOpensearchTable, 1, "foo-updated")
	assertDocument(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "1", "foo-updated")

	deleteRow(t, ctx, pgCfg, syncOpensearchTable, 2)
	waitForDocumentAbsent(t, cfg.SearchEngine.Address, cfg.SearchEngine.Index, "2", 15*time.Second)
}

func setupTable(t *testing.T, ctx context.Context, pgCfg postgresql.Config, table, publication string) {
	t.Helper()

	conn, err := postgresql.NewStandard(ctx, pgCfg)
	if err != nil {
		t.Fatal("e2e; sync; failed to connect to postgres; error: ", err)
	}
	defer conn.Close(ctx)

	mustExec(t, ctx, conn, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id int primary key, name text)`, table))
	mustExec(t, ctx, conn, fmt.Sprintf(`TRUNCATE TABLE %s`, table))

	exists, err := postgresql.PublicationExists(ctx, conn, publication)
	if err != nil {
		t.Fatal("e2e; sync; failed to check publication existence; error: ", err)
	}
	if !exists {
		mustExec(t, ctx, conn, fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`, publication, table))
	}
}

func insertRow(t *testing.T, ctx context.Context, pgCfg postgresql.Config, table string, id int, name string) {
	t.Helper()
	execSQL(t, ctx, pgCfg, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (%d, '%s')`, table, id, name))
}

func updateRow(t *testing.T, ctx context.Context, pgCfg postgresql.Config, table string, id int, name string) {
	t.Helper()
	execSQL(t, ctx, pgCfg, fmt.Sprintf(`UPDATE %s SET name = '%s' WHERE id = %d`, table, name, id))
}

func deleteRow(t *testing.T, ctx context.Context, pgCfg postgresql.Config, table string, id int) {
	t.Helper()
	execSQL(t, ctx, pgCfg, fmt.Sprintf(`DELETE FROM %s WHERE id = %d`, table, id))
}

func execSQL(t *testing.T, ctx context.Context, pgCfg postgresql.Config, sql string) {
	t.Helper()

	conn, err := postgresql.NewStandard(ctx, pgCfg)
	if err != nil {
		t.Fatal("e2e; sync; failed to connect to postgres; error: ", err)
	}
	defer conn.Close(ctx)

	mustExec(t, ctx, conn, sql)
}

func mustExec(t *testing.T, ctx context.Context, conn *pgconn.PgConn, sql string) {
	t.Helper()

	if _, err := conn.Exec(ctx, sql).ReadAll(); err != nil {
		t.Fatalf("e2e; sync; failed to execute %q: %v", sql, err)
	}
}

func assertDocument(t *testing.T, address, index, id, expectedName string) {
	t.Helper()

	timeout := 15 * time.Second
	deadline := time.Now().Add(timeout)

	var lastName string
	for time.Now().Before(deadline) {
		if source, ok := fetchDocument(t, address, index, id); ok {
			lastName, _ = source["name"].(string)
			if lastName == expectedName {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("e2e; sync; document %s has name %q, want %q after %s", id, lastName, expectedName, timeout)
}

func waitForDocumentAbsent(t *testing.T, address, index, id string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := fetchDocument(t, address, index, id); !ok {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("e2e; sync; document %s was still present in %s/%s after %s", id, address, index, timeout)
}

func fetchDocument(t *testing.T, address, index, id string) (map[string]any, bool) {
	t.Helper()

	url := fmt.Sprintf("%s/%s/_doc/%s", address, index, id)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal("e2e; sync; failed to fetch document; error: ", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, false
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("e2e; sync; unexpected status %d fetching document %s", resp.StatusCode, id)
	}

	var body struct {
		Source map[string]any `json:"_source"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal("e2e; sync; failed to decode document response; error: ", err)
	}

	return body.Source, true
}
