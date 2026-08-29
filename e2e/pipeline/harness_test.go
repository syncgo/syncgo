//go:build e2e

// Package pipeline exercises the full syncgo pipeline end to end: a real
// PostgreSQL instance with logical replication enabled, a real Elasticsearch
// or OpenSearch instance, and syncgo itself running either as a locally
// built binary or as its Docker image. Data is written to PostgreSQL and
// verified to arrive, correctly, in the search engine.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/syncgo/syncgo/pkg/config"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

type runMode int

const (
	modeBinary runMode = iota
	modeDocker
)

type engineKind int

const (
	engineElasticsearch engineKind = iota
	engineOpensearch
)

func (e engineKind) name() string {
	if e == engineOpensearch {
		return "opensearch"
	}
	return "elasticsearch"
}

func credentials(eng engineKind) (user, pass string) {
	if eng == engineOpensearch {
		return "admin", "Op3nS3arch!"
	}
	return "admin", "Es123456"
}

// endpoints returns the postgres host/port and search engine address as seen
// from the given run mode: binary mode runs on the host and reaches services
// through their published ports, docker mode runs on the compose network and
// reaches services by their service name and internal port.
func endpoints(mode runMode, eng engineKind) (pgHost, pgPort, searchAddr string) {
	if mode == modeDocker {
		pgHost, pgPort = "postgres", "5432"
		if eng == engineOpensearch {
			return pgHost, pgPort, "http://opensearch:9200"
		}
		return pgHost, pgPort, "http://elasticsearch:9200"
	}

	pgHost, pgPort = "localhost", pgHostPort
	return pgHost, pgPort, verifySearchAddr(eng)
}

// verifySearchAddr is the address the *test process* uses to read back
// documents. Unlike the app-under-test (which, in docker mode, reaches the
// search engine over the compose network by service name), this test binary
// always runs on the host, so it always goes through the published port.
func verifySearchAddr(eng engineKind) string {
	if eng == engineOpensearch {
		return "http://localhost:9300"
	}
	return "http://localhost:9200"
}

// ---------------------------------------------------------------------------
// Shared docker-compose infrastructure (postgres + elasticsearch + opensearch)
// ---------------------------------------------------------------------------

var (
	infraOnce   sync.Once
	infraErr    error
	pgHostPort  string
	networkName string
)

func ensureInfra(t *testing.T) {
	t.Helper()

	infraOnce.Do(func() {
		pgHostPort = resolveFreePort("5432")
		if infraErr = startCompose(pgHostPort); infraErr != nil {
			return
		}
		networkName, infraErr = detectNetwork()
	})

	if infraErr != nil {
		t.Fatalf("e2e; pipeline; failed to prepare docker compose infra: %v", infraErr)
	}
}

// resolveFreePort avoids colliding with an unrelated service already bound to
// the preferred port on this host. Best effort: a TOCTOU race against another
// process grabbing the port is possible but acceptable for test tooling.
func resolveFreePort(preferred string) string {
	if ln, err := net.Listen("tcp", "127.0.0.1:"+preferred); err == nil {
		_ = ln.Close()
		return preferred
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return preferred
	}
	defer ln.Close()

	_, port, _ := net.SplitHostPort(ln.Addr().String())
	return port
}

func startCompose(pgPort string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}

	composeFile := filepath.Join(root, "e2e", "docker-compose.yml")
	cmd := exec.Command("docker", "compose", "-f", composeFile, "up", "-d", "--wait")
	cmd.Env = append(os.Environ(), "SYNCGO_E2E_PG_PORT="+pgPort)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose up failed: %w\n%s", err, out)
	}
	return nil
}

// detectNetwork asks docker directly which network the postgres container
// ended up on, rather than assuming a compose network naming convention.
func detectNetwork() (string, error) {
	out, err := exec.Command("docker", "inspect", "-f",
		`{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}`, "e2e-postgres-1").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to detect docker network: %w: %s", err, out)
	}

	name := strings.TrimSpace(string(out))
	if name == "" {
		return "", fmt.Errorf("could not determine docker network for e2e-postgres-1")
	}
	return name, nil
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above %s", dir)
		}
		dir = parent
	}
}

// ---------------------------------------------------------------------------
// Building the artifact under test: the syncgo binary, or its Docker image.
// ---------------------------------------------------------------------------

var (
	buildBinaryOnce sync.Once
	binPath         string
	binBuildErr     error
	binTempDir      string
)

func ensureBinary(t *testing.T) string {
	t.Helper()

	buildBinaryOnce.Do(func() {
		root, err := repoRoot()
		if err != nil {
			binBuildErr = err
			return
		}

		dir, err := os.MkdirTemp("", "syncgo-e2e-bin-")
		if err != nil {
			binBuildErr = err
			return
		}
		binTempDir = dir

		out := filepath.Join(dir, "syncgo")
		build := func(env []string) ([]byte, error) {
			cmd := exec.Command("go", "build", "-o", out, "./cmd/syncgo")
			cmd.Dir = root
			if env != nil {
				cmd.Env = env
			}
			return cmd.CombinedOutput()
		}

		firstOut, err := build(nil)
		if err != nil {
			// The module may require an older Go toolchain than the one
			// installed locally (e.g. a grpc/x/net combination that only
			// builds under Go <1.27); retry pinned to the version go.mod
			// declares before giving up.
			fallbackOut, fallbackErr := build(append(os.Environ(), "GOTOOLCHAIN=go1.26.6"))
			if fallbackErr != nil {
				binBuildErr = fmt.Errorf("go build failed: %w\ndefault toolchain output:\n%s\nfallback toolchain output:\n%s", err, firstOut, fallbackOut)
				return
			}
		}

		binPath = out
	})

	if binBuildErr != nil {
		t.Fatalf("e2e; pipeline; failed to build syncgo binary: %v", binBuildErr)
	}
	return binPath
}

var (
	buildImageOnce sync.Once
	imageTag       string
	imageBuildErr  error
)

const dockerImageTag = "syncgo-e2e-pipeline:test"

func ensureImage(t *testing.T) string {
	t.Helper()

	buildImageOnce.Do(func() {
		root, err := repoRoot()
		if err != nil {
			imageBuildErr = err
			return
		}

		cmd := exec.Command("docker", "build", "-t", dockerImageTag, "-f", filepath.Join(root, "Dockerfile"), root)
		if out, err := cmd.CombinedOutput(); err != nil {
			imageBuildErr = fmt.Errorf("docker build failed: %w\n%s", err, out)
			return
		}

		imageTag = dockerImageTag
	})

	if imageBuildErr != nil {
		t.Fatalf("e2e; pipeline; failed to build docker image: %v", imageBuildErr)
	}
	return imageTag
}

func TestMain(m *testing.M) {
	code := m.Run()
	if binTempDir != "" {
		_ = os.RemoveAll(binTempDir)
	}
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// Running the artifact under test as a syncgo process.
// ---------------------------------------------------------------------------

type app interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
	Logs() string
}

type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type binaryApp struct {
	binPath string
	cfgPath string

	cmd *exec.Cmd
	buf *syncBuffer
}

func (a *binaryApp) Start(ctx context.Context) error {
	a.cmd = exec.CommandContext(ctx, a.binPath, "--config", a.cfgPath)
	a.buf = &syncBuffer{}
	a.cmd.Stdout = a.buf
	a.cmd.Stderr = a.buf
	a.cmd.Cancel = nil // rely on our own SIGTERM handling in Stop, not ctx cancellation killing the process

	return a.cmd.Start()
}

func (a *binaryApp) Stop(_ context.Context) {
	if a.cmd == nil || a.cmd.Process == nil {
		return
	}

	_ = a.cmd.Process.Signal(syscall.SIGTERM)

	done := make(chan error, 1)
	go func() { done <- a.cmd.Wait() }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = a.cmd.Process.Kill()
		<-done
	}
}

func (a *binaryApp) Logs() string {
	if a.buf == nil {
		return ""
	}
	return a.buf.String()
}

type dockerApp struct {
	image       string
	network     string
	name        string
	cfgHostPath string
}

func (a *dockerApp) Start(ctx context.Context) error {
	_ = exec.Command("docker", "rm", "-f", a.name).Run()

	args := []string{
		"run", "-d",
		"--name", a.name,
		"--network", a.network,
		"-v", a.cfgHostPath + ":/etc/syncgo/config.yaml:ro",
		a.image,
		"--config", "/etc/syncgo/config.yaml",
	}

	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker run failed: %w: %s", err, out)
	}
	return nil
}

func (a *dockerApp) Stop(_ context.Context) {
	_ = exec.Command("docker", "stop", "-t", "10", a.name).Run()
	_ = exec.Command("docker", "rm", "-f", a.name).Run()
}

func (a *dockerApp) Logs() string {
	out, _ := exec.Command("docker", "logs", a.name).CombinedOutput()
	return string(out)
}

func newApp(mode runMode, artifact, network, cfgPath, name string) app {
	if mode == modeDocker {
		return &dockerApp{image: artifact, network: network, name: name, cfgHostPath: cfgPath}
	}
	return &binaryApp{binPath: artifact, cfgPath: cfgPath}
}

func waitForAppLog(t *testing.T, a app, substr string, timeout time.Duration) {
	t.Helper()

	ok := waitFor(timeout, 200*time.Millisecond, func() (bool, error) {
		return strings.Contains(a.Logs(), substr), nil
	})
	if !ok {
		t.Fatalf("e2e; pipeline; app did not log %q within %s; logs:\n%s", substr, timeout, a.Logs())
	}
}

// ---------------------------------------------------------------------------
// Config generation
// ---------------------------------------------------------------------------

func writeConfig(t *testing.T, mode runMode, eng engineKind, pub, slot, index string) string {
	t.Helper()

	pgHost, pgPort, searchAddr := endpoints(mode, eng)
	user, pass := credentials(eng)

	cfg := config.Config{
		PostgreSQL: config.PostgreSQLConfig{
			User:            "postgres",
			Password:        "postgres",
			Host:            pgHost,
			Port:            pgPort,
			Database:        "postgres",
			SlotName:        slot,
			PublicationName: pub,
		},
		SearchEngine: config.SearchEngineConfig{
			Name:     eng.name(),
			Address:  searchAddr,
			Username: user,
			Password: pass,
			Index:    index,
		},
		Batcher: config.BatcherConfig{
			Size:          40,
			FlushInterval: 100 * time.Millisecond,
		},
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("e2e; pipeline; failed to marshal config: %v", err)
	}

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("e2e; pipeline; failed to write config: %v", err)
	}
	return path
}

// ---------------------------------------------------------------------------
// PostgreSQL helpers
// ---------------------------------------------------------------------------

func adminDSN(port string) string {
	return fmt.Sprintf("postgres://postgres:postgres@localhost:%s/postgres", port)
}

func createTable(ctx context.Context, conn *pgx.Conn, table string) error {
	ident := pgx.Identifier{table}.Sanitize()

	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		id INT PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT NOT NULL,
		age INT NOT NULL,
		active BOOLEAN NOT NULL,
		score DOUBLE PRECISION NOT NULL,
		meta JSONB,
		bio TEXT,
		created_at TIMESTAMPTZ NOT NULL
	)`, ident))
	return err
}

func createPublication(ctx context.Context, conn *pgx.Conn, pub, table string) error {
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		pgx.Identifier{pub}.Sanitize(), pgx.Identifier{table}.Sanitize()))
	return err
}

func cleanupPostgres(conn *pgx.Conn, table, pub, slot string) {
	ctx := context.Background()

	_, _ = conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{pub}.Sanitize()))
	_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pgx.Identifier{table}.Sanitize()))
	_, _ = conn.Exec(ctx,
		`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active`,
		slot)
}

func insertRows(ctx context.Context, conn *pgx.Conn, table string, rows []testRow) error {
	const chunkSize = 40

	for start := 0; start < len(rows); start += chunkSize {
		end := min(start+chunkSize, len(rows))
		if err := insertChunk(ctx, conn, table, rows[start:end]); err != nil {
			return fmt.Errorf("insert chunk [%d:%d]: %w", start, end, err)
		}
	}
	return nil
}

func insertChunk(ctx context.Context, conn *pgx.Conn, table string, rows []testRow) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "INSERT INTO %s (id, name, email, age, active, score, meta, bio, created_at) VALUES ",
		pgx.Identifier{table}.Sanitize())

	args := make([]any, 0, len(rows)*9)
	for i, r := range rows {
		if i > 0 {
			sb.WriteString(",")
		}
		base := i * 9
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9)

		metaJSON, err := json.Marshal(r.Meta)
		if err != nil {
			return err
		}
		args = append(args, r.ID, r.Name, r.Email, r.Age, r.Active, r.Score, string(metaJSON), r.Bio, r.CreatedAt)
	}

	_, err := conn.Exec(ctx, sb.String(), args...)
	return err
}

// updateSubset updates every step-th row and mutates rows in place so the
// caller's slice reflects the new expected state.
func updateSubset(ctx context.Context, conn *pgx.Conn, table string, rows []testRow, step int) error {
	ident := pgx.Identifier{table}.Sanitize()

	for i := step - 1; i < len(rows); i += step {
		r := rows[i]
		newName := r.Name + "-updated"
		newScore := r.Score + 100

		_, err := conn.Exec(ctx, fmt.Sprintf("UPDATE %s SET name=$1, score=$2 WHERE id=$3", ident), newName, newScore, r.ID)
		if err != nil {
			return err
		}

		r.Name = newName
		r.Score = newScore
		rows[i] = r
	}
	return nil
}

func deleteSubset(ctx context.Context, conn *pgx.Conn, table string, rows []testRow, step int) (map[int]bool, error) {
	ident := pgx.Identifier{table}.Sanitize()
	deleted := make(map[int]bool)

	for i := step - 1; i < len(rows); i += step {
		id := rows[i].ID
		if _, err := conn.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id=$1", ident), id); err != nil {
			return nil, err
		}
		deleted[id] = true
	}
	return deleted, nil
}

// ---------------------------------------------------------------------------
// Test data generation
// ---------------------------------------------------------------------------

type testRow struct {
	ID        int
	Name      string
	Email     string
	Age       int
	Active    bool
	Score     float64
	Meta      map[string]any
	Bio       *string
	CreatedAt time.Time
}

var namePool = []string{
	"Ada", "Grace", "Alan", "Linus", "Barbara", "Donald",
	"Margaret", "Dennis", "Radia", "Edsger", "Katherine", "Guido",
}

// generateRows builds n rows spanning a variety of column types and shapes:
// strings, ints, bools, floats, nested JSON (objects/arrays/booleans/numbers),
// NULLs, and timestamps, so the sync pipeline is exercised against realistic
// data diversity rather than a single trivial shape.
func generateRows(n int) []testRow {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	rows := make([]testRow, 0, n)
	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("%s-%d", namePool[i%len(namePool)], i)

		var bio *string
		if i%4 != 0 {
			b := fmt.Sprintf("Bio for %s, employee #%d", name, i)
			bio = &b
		}

		rows = append(rows, testRow{
			ID:     i,
			Name:   name,
			Email:  fmt.Sprintf("user%d@example.test", i),
			Age:    18 + (i*7)%63,
			Active: i%2 == 0,
			Score:  float64((i*337)%9973) / 100,
			Meta: map[string]any{
				"rank":     i % 10,
				"verified": i%3 == 0,
				"tags":     []any{"seed", fmt.Sprintf("group-%d", i%5)},
			},
			Bio:       bio,
			CreatedAt: base.Add(time.Duration(i) * time.Hour),
		})
	}
	return rows
}

func shortID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")[:12]
}

// ---------------------------------------------------------------------------
// Search engine HTTP helpers (count/get/refresh) — the production client only
// exposes existence checks, but these tests need to read document contents
// back to verify the synced data really matches what was written.
// ---------------------------------------------------------------------------

type searchClient struct {
	httpClient  *http.Client
	base, index string
	user, pass  string
}

func newSearchClient(addr, index, user, pass string) *searchClient {
	return &searchClient{
		httpClient: &http.Client{Timeout: 10 * time.Second},
		base:       addr,
		index:      index,
		user:       user,
		pass:       pass,
	}
}

func (s *searchClient) do(ctx context.Context, method, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, s.base+path, nil)
	if err != nil {
		return nil, err
	}
	if s.user != "" {
		req.SetBasicAuth(s.user, s.pass)
	}
	return s.httpClient.Do(req)
}

func (s *searchClient) refresh(ctx context.Context) error {
	resp, err := s.do(ctx, http.MethodPost, "/"+s.index+"/_refresh")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("refresh failed: status %d", resp.StatusCode)
	}
	return nil
}

func (s *searchClient) count(ctx context.Context) (int, error) {
	resp, err := s.do(ctx, http.MethodGet, "/"+s.index+"/_count")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}

	var body struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("count failed: status %d", resp.StatusCode)
	}
	return body.Count, nil
}

func (s *searchClient) getSource(ctx context.Context, id string) (map[string]any, bool, error) {
	resp, err := s.do(ctx, http.MethodGet, "/"+s.index+"/_doc/"+id)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, false, fmt.Errorf("get doc failed: status %d body %s", resp.StatusCode, b)
	}

	var body struct {
		Source map[string]any `json:"_source"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, false, err
	}
	return body.Source, true, nil
}

func (s *searchClient) deleteIndex(ctx context.Context) {
	resp, err := s.do(ctx, http.MethodDelete, "/"+s.index)
	if err == nil {
		_ = resp.Body.Close()
	}
}

func waitFor(timeout, interval time.Duration, cond func() (bool, error)) bool {
	deadline := time.Now().Add(timeout)
	for {
		if ok, err := cond(); err == nil && ok {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}
