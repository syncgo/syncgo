//go:build e2e_pipeline

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/syncgo/syncgo/pkg/config"
)

const (
	rowCount     = 500
	syncTimeout  = 60 * time.Second
	pollInterval = 500 * time.Millisecond
)

const defaultConfigPath = "../config.yaml"

func loadConfig(t *testing.T) *config.Config {
	t.Helper()

	path := os.Getenv("SYNCGO_E2E_CONFIG")
	if path == "" {
		path = defaultConfigPath
	}

	cfg, err := config.LoadFromYAML(path)
	require.NoErrorf(t, err, "load e2e config %q", path)
	return cfg

}

func pgDSN(cfg *config.Config) string {
	pg := cfg.PostgreSQL
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pg.User, pg.Password, pg.Host, pg.Port, pg.Database)
}

func connectPG(t *testing.T, ctx context.Context, cfg *config.Config) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, pgDSN(cfg))
	require.NoError(t, err, "connect to postgres")
	return conn
}

func execPG(t *testing.T, ctx context.Context, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	_, err := conn.Exec(ctx, sql, args...)
	require.NoErrorf(t, err, "exec: %s", sql)
}

func resetPostgres(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	execPG(t, ctx, conn, `DELETE FROM test`)
}

func insertRows(t *testing.T, ctx context.Context, conn *pgx.Conn, ids []int32, names []string) {
	t.Helper()
	execPG(t, ctx, conn,
		`INSERT INTO test (id, name) SELECT * FROM unnest($1::int[], $2::text[])`,
		ids, names)
}

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

type searchTarget struct {
	base  string
	index string
	user  string
	pass  string
}

type mgetResponse struct {
	Docs []struct {
		ID     string         `json:"_id"`
		Found  bool           `json:"found"`
		Source map[string]any `json:"_source"`
	} `json:"docs"`
}

type foundDoc struct {
	Found  bool
	Source map[string]any
}

func newSearchTarget(cfg *config.Config) searchTarget {
	return searchTarget{
		base:  strings.TrimRight(cfg.SearchEngine.Address, "/"),
		index: cfg.SearchEngine.Index,
		user:  cfg.SearchEngine.Username,
		pass:  cfg.SearchEngine.Password,
	}
}

func (s searchTarget) do(req *http.Request) (*http.Response, error) {
	if s.user != "" {
		req.SetBasicAuth(s.user, s.pass)
	}
	return http.DefaultClient.Do(req)
}

func (s searchTarget) deleteIndex(t *testing.T) {
	t.Helper()

	req, err := http.NewRequest(http.MethodDelete, s.base+"/"+s.index, nil)
	require.NoError(t, err)
	resp, err := s.do(req)
	require.NoError(t, err, "delete index")
	defer resp.Body.Close()
	require.Truef(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound,
		"unexpected status deleting index: %d", resp.StatusCode)
}

// fetches many docs by id
func (s searchTarget) mget(ctx context.Context, ids []string) (map[string]foundDoc, error) {
	body, _ := json.Marshal(map[string][]string{"ids": ids})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.base+"/"+s.index+"/_mget", bytes.NewReader(body))

	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		return map[string]foundDoc{}, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("_mget status %d: %s", resp.StatusCode, raw)
	}

	var parsed mgetResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}

	out := make(map[string]foundDoc, len(parsed.Docs))
	for _, d := range parsed.Docs {
		out[d.ID] = foundDoc{Found: d.Found, Source: d.Source}
	}
	return out, nil
}

// it polls until every id in expected is present with the expected name. reporting match count.
func (s searchTarget) waitForDocs(t *testing.T, ctx context.Context, expected map[string]string) {
	t.Helper()
	ids := make([]string, 0, len(expected))
	for id := range expected {
		ids = append(ids, id)
	}
	deadline := time.Now().Add(syncTimeout)
	var matched int

	for time.Now().Before(deadline) {
		docs, err := s.mget(ctx, ids)
		require.NoError(t, err)

		matched = 0
		for id, wantName := range expected {
			if doc, ok := docs[id]; ok && doc.Found && doc.Source["name"] == wantName {
				matched += 1
			}
		}
		if matched == len(expected) {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out after %s: %d/%d docs synced correctly", syncTimeout, matched, len(expected))
}

func (s searchTarget) waitForAbsent(t *testing.T, ctx context.Context, id string) {
	t.Helper()
	deadline := time.Now().Add(syncTimeout)
	for time.Now().Before(deadline) {
		docs, err := s.mget(ctx, []string{id})
		require.NoError(t, err)
		if doc, ok := docs[id]; !ok || !doc.Found {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out after %s: doc %q still present (delete not synced)", syncTimeout, id)
}

func TestPipeline_InsertSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := loadConfig(t)
	conn := connectPG(t, ctx, cfg)
	defer conn.Close(ctx)
	search := newSearchTarget(cfg)

	resetPostgres(t, ctx, conn)
	search.deleteIndex(t)

	run := time.Now().UnixNano()

	expected := make(map[string]string, rowCount)
	ids := make([]int32, rowCount)
	names := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		id := int32(i + 1)
		name := fmt.Sprintf("row-%d-%d-%s", run, id, varietyTag(i))
		ids[i] = id
		names[i] = name
		expected[fmt.Sprint(id)] = name
	}
	insertRows(t, ctx, conn, ids, names)
	search.waitForDocs(t, ctx, expected)
}

func TestPipeline_UpdateDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := loadConfig(t)
	conn := connectPG(t, ctx, cfg)
	defer conn.Close(ctx)
	search := newSearchTarget(cfg)

	resetPostgres(t, ctx, conn)
	search.deleteIndex(t)

	run := time.Now().UnixNano()
	insertRows(t, ctx, conn, []int32{1, 2, 3}, []string{
		fmt.Sprintf("keep-%d", run),
		fmt.Sprintf("update-%d", run),
		fmt.Sprintf("delete-%d", run),
	})

	// wait for initial state to sync
	search.waitForDocs(t, ctx, map[string]string{
		"2": fmt.Sprintf("update-%d", run),
		"3": fmt.Sprintf("delete-%d", run),
	})

	updatedName := fmt.Sprintf("updated-%d", run)
	execPG(t, ctx, conn, `UPDATE test SET name = $1 WHERE id=2`, updatedName)
	execPG(t, ctx, conn, `DELETE FROM test WHERE id = 3`)

	search.waitForDocs(t, ctx, map[string]string{"2": updatedName})
	search.waitForAbsent(t, ctx, "3")
}
