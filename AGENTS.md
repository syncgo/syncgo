# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

SyncGo is a Go implementation of [PGSync](https://pgsync.com/): it consumes PostgreSQL logical replication (WAL via `pgoutput`) and writes changes to Elasticsearch or OpenSearch in batches. Distributed as a binary (`cmd/syncgo`), configured via YAML.

## Commands

```bash
make build            # build binary to ./bin (go build -o bin ./...)
make test             # go test ./... -v -short
make lint             # golangci-lint run ./...
make generate         # install tools (go install tool) + go generate ./... (mocks)
make demo             # build + make prepare (e2e docker env) + run with e2e/config.yaml
```

- Run a single test: `go test ./internal/batcher/ -run TestName -v`
- Unit tests use `-short`; CI also runs them with `-race` and coverage. Set `LOG_LEVEL=error` to quiet logs.
- E2E tests (build tag `e2e`, in `./e2e/...`) require the docker environment: `make prepare` (starts Postgres/Elasticsearch/OpenSearch via `e2e/docker-compose.yml`, configures `wal_level=logical`, creates the publication), then `go test -tags e2e -timeout 5m ./e2e/...`. `make generate_data` seeds changes into the e2e Postgres.
- Mocks are generated with mockgen (`go.uber.org/mock`) via `//go:generate` directives in `internal/batcher/batcher.go` and `pkg/search_engine_client/client.go`; `make generate` must not produce a git diff (CI enforces this).
- Import ordering is enforced by `goimports` with `-local github.com/syncgo/syncgo` (syncgo imports form a separate group after third-party ones); CI fails on unformatted code.

## Syncgo binary commands

The compiled binary (`cmd/syncgo`) has three subcommands plus a config flag:

```bash
syncgo prepare --config config.yaml   # idempotently create publication, replication slot and search index
syncgo run --config config.yaml       # run the sync process (expects prepare to have been done)
syncgo version                        # print the version (set via ldflags at build time)
syncgo -h                             # usage
```

- `-config` has the `-cfg` shorthand; when omitted the config defaults to `/etc/syncgo/config.yaml`.
- `version` prints the value of the unexported `pkg/cli.version` variable (see `pkg/cli/cli.go`); builds must set it via `-X github.com/syncgo/syncgo/pkg/cli.version=<version>` (this is what `.goreleaser.yaml` and the `Dockerfile` do — `main.version` does not exist).
- `run` handles `SIGINT`/`SIGTERM` and flushes the current batch on graceful shutdown.
- `prepare` and `run` exit non-zero with a log message on failure (config load, processor init, run error).

## Architecture

Startup flow: `cmd/syncgo/main.go` → `pkg/cli.Main` (subcommand dispatch) → `config.LoadFromYAML` → `processor.New`/`processor.Prepare` (wires everything) → `processor.Run`. The processor starts a metrics HTTP server (only if `metrics.port > 0`) and keeps a graceful shutdown path (`SIGTERM`/`SIGINT` via `signal.NotifyContext`).

Data pipeline (in order):

1. **`internal/replication`** — `LogicalReplicationConn` wraps `pgconn.PgConn` + `pglogrepl`. On creation it validates/creates the replication slot through a separate standard connection (`pkg/postgresql`), starts replication with the `pgoutput` plugin, and reads the WAL in a goroutine. It decodes begin/insert/update/delete/commit/relation messages and feeds rows into the batcher, tracking per-transaction row counts with `txnTracker` (`txn_batcher.go`) so only *committed* rows are committed to the batcher and streamed-transaction aborts roll them back. Periodic standby status updates (LSN acks) run on `pkg/async_task`.
2. **`internal/batcher`** — thread-safe buffer with commit/rollback semantics (`lastCommitted` index): the replication layer adds rows, then commits N on transaction commit or rolls back N on abort. Flush is triggered by buffer size and/or `flush_interval` (disabled when 0); flushing sends to a `BulkSender`.
3. **`internal/bulk_transformer`** — converts row data into Elasticsearch/OpenSearch bulk action payloads (index/create/update/delete per operation type).
4. **`pkg/search_engine_client`** — a single `Client` for both engines, selected by `search_engine.name`. Bulk goes over HTTP (`_bulk`, NDJSON, optional gzip, Basic auth/API key/TLS via `pkg/http_client`, `pkg/gzip`, `pkg/tls`) or, for OpenSearch only, over gRPC (`search_engine.grpc` config, `DocumentService`). Startup pings `GET /_cluster/health` and fails fast if unreachable.

Packages are intentionally split `internal/` (app logic) vs `pkg/` (reusable infra: config, postgresql client, http client, tls, gzip, logger, metrics, async_task).

Configuration lives in `pkg/config/config.go` — the YAML has four sections: `postgresql`, `search_engine`, `batcher`, `metrics`. Batch/timeout defaults are defined there.

Metrics: `pkg/metrics` (`SearchMetrics`) is injected into the search client and reported via Prometheus; a `Monitoring` interface is mocked for tests (`pkg/search_engine_client/mocks`).
