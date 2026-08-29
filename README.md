# SyncGo

**SyncGo** is a Golang implementation of [PGSync](https://pgsync.com/) for continuously synchronizing changes from PostgreSQL to Elasticsearch or OpenSearch.

SyncGo uses PostgreSQL logical replication to consume database changes and writes them to the configured search engine in batches.

# Requirements

For production, SyncGo is distributed and executed as a **binary**.

Running SyncGo from its Docker image is covered by the automated end-to-end pipeline
tests (see [End-to-end tests](#end-to-end-tests)), but it has not been hardened for
production and is not currently considered a supported production deployment method.

# Installation

## Build the binary

Clone the repository:

```bash
git clone https://github.com/syncgo/syncgo.git
cd syncgo
```

Build SyncGo:

```bash
make build
```

The resulting binary is placed in:

```text
bin/syncgo
```

The `build` target runs `go build -o bin ./...`.

You can also build it directly:

```bash
go build -o bin/syncgo ./cmd/syncgo
```

## Production installation

A typical production installation can look like:

```text
/etc/syncgo/
└── config.yaml

/usr/local/bin/
└── syncgo
```

Start SyncGo with:

```bash
/usr/local/bin/syncgo --config /etc/syncgo/config.yaml
```

There is also a short `--cfg` alias:

```bash
/usr/local/bin/syncgo --cfg /etc/syncgo/config.yaml
```

If no configuration path is specified, SyncGo looks for:

```text
/etc/syncgo/config.yaml
```

---

# Configuration

SyncGo uses a YAML configuration file.

A minimal configuration looks like:

```yaml
postgresql:
  user: postgres
  password: postgres
  host: localhost
  port: 5432
  database: postgres
  slot_name: syncgo_slot
  publication_name: syncgo_publication

search_engine:
  name: elasticsearch
  address: http://localhost:9200
  username: elastic
  password: password
  index: my_index

batcher:
  size: 100
  flush_interval: 50ms
```

The complete configuration is divided into four sections:

* `postgresql`
* `search_engine`
* `batcher`
* `metrics`

The configuration structure is defined in `pkg/config/config.go`.

---

# PostgreSQL configuration

```yaml
postgresql:
  user: postgres
  password: postgres
  host: localhost
  port: 5432
  database: postgres
  slot_name: syncgo_slot
  publication_name: syncgo_publication
  id_column: id
```

| Parameter          | Required | Description                                                              |
| ------------------ | -------: | ------------------------------------------------------------------------ |
| `user`             |      Yes | PostgreSQL username used by SyncGo                                       |
| `password`         |      Yes | PostgreSQL password                                                      |
| `host`             |      Yes | PostgreSQL hostname or IP address                                        |
| `port`             |      Yes | PostgreSQL port                                                          |
| `database`         |      Yes | Database containing the publication                                      |
| `slot_name`        |      Yes | Logical replication slot used by SyncGo                                  |
| `publication_name` |      Yes | PostgreSQL publication consumed by SyncGo                                |
| `id_column`        |       No | Primary identifier column used when processing changes; defaults to `id` |

`slot_name` and `publication_name` are mandatory.

If `id_column` is not specified, SyncGo uses:

```text
id
```

## Logical replication

PostgreSQL must be configured for logical replication.

At minimum, the PostgreSQL instance needs appropriate values for:

```text
wal_level = logical
max_wal_senders > 0
max_replication_slots > 0
```

The E2E environment demonstrates this setup by changing `wal_level`, `max_wal_senders`, `max_replication_slots`, and `logical_decoding_work_mem`.

Create a publication containing the tables SyncGo should consume:

```sql
CREATE PUBLICATION syncgo_publication
FOR TABLE public.users, public.orders;
```

Alternatively, for all tables:

```sql
CREATE PUBLICATION syncgo_publication
FOR ALL TABLES;
```

The configured publication determines which PostgreSQL changes are delivered to SyncGo.

---

# Search engine configuration

SyncGo supports:

```yaml
search_engine:
  name: elasticsearch
```

or:

```yaml
search_engine:
  name: opensearch
```

The configuration validator accepts only these two values.

## Basic configuration

```yaml
search_engine:
  name: elasticsearch
  address: http://localhost:9200
  username: elastic
  password: password
  index: users
```

| Parameter            | Required | Description                              |
| -------------------- | -------: | ---------------------------------------- |
| `name`               |      Yes | `elasticsearch` or `opensearch`          |
| `address`            |      Yes | HTTP address of the search cluster       |
| `username`           |       No | Username for Basic Authentication        |
| `password`           |       No | Password for Basic Authentication        |
| `api_key`            |       No | API key authentication                   |
| `cloud_id`           |       No | Elasticsearch Cloud ID                   |
| `index`              |       No | Target index                             |
| `connection_timeout` |       No | HTTP connection/request timeout          |
| `gzip_compression`   |       No | Compression level for bulk requests      |
| `tls`                |       No | TLS configuration                        |
| `keep_alive`         |       No | HTTP connection keep-alive configuration |
| `grpc`               |       No | OpenSearch gRPC configuration            |

SyncGo checks the search cluster during startup using:

```http
GET /_cluster/health
```

Therefore, SyncGo will fail to start if the configured search engine cannot be reached or returns an unsuccessful status.

---

# Authentication

## Basic authentication

```yaml
search_engine:
  name: elasticsearch
  address: https://elasticsearch.example.com:9200
  username: elastic
  password: secret
  index: users
```

SyncGo creates a standard HTTP Basic Authentication header from `username` and `password`.

## API key

API key authentication can be configured with:

```yaml
search_engine:
  name: elasticsearch
  address: https://elasticsearch.example.com:9200
  api_key: <api-key>
  index: users
```

When `api_key` is configured, it takes precedence over username/password authentication.

---

# TLS

TLS can be configured with:

```yaml
search_engine:
  name: elasticsearch
  address: https://elasticsearch.example.com:9200
  tls:
    ca_cert: /etc/syncgo/certs/ca.crt
    insecure_skip_verify: false
```

| Parameter              | Description                                                 |
| ---------------------- | ----------------------------------------------------------- |
| `ca_cert`              | Path to the CA certificate used to verify the search engine |
| `insecure_skip_verify` | Disables TLS certificate verification when set to `true`    |

For production, avoid:

```yaml
insecure_skip_verify: true
```

unless there is a specific operational reason to do so.

---

# Connection timeout

```yaml
search_engine:
  connection_timeout: 30s
```

This controls the timeout used by the search-engine HTTP client.

If it is not configured, the default connection timeout is **30 seconds**.

Examples:

```yaml
connection_timeout: 10s
```

```yaml
connection_timeout: 1m
```

---

# Gzip compression

Bulk requests can use gzip compression:

```yaml
search_engine:
  gzip_compression: 5
```

Compression can reduce network traffic when sending large bulk payloads.

The exact accepted compression-level values depend on SyncGo's gzip configuration implementation.

For a simple initial deployment, this option can be omitted.

---

# HTTP keep-alive

SyncGo supports persistent HTTP connections:

```yaml
search_engine:
  keep_alive:
    max_conn_duration: 5m
    max_idle_conn_duration: 1m
```

| Parameter                | Description                                   |
| ------------------------ | --------------------------------------------- |
| `max_conn_duration`      | Maximum lifetime of an HTTP connection        |
| `max_idle_conn_duration` | Maximum time an idle connection is kept alive |

This can be useful for high-throughput production deployments where SyncGo continuously sends bulk requests.

---

# OpenSearch gRPC

OpenSearch can optionally use gRPC for bulk operations instead of the HTTP `_bulk` API.

Example:

```yaml
search_engine:
  name: opensearch
  address: http://localhost:9200
  index: users

  grpc:
    host: localhost
    port: 9300
```

The `grpc` section is valid only when:

```yaml
name: opensearch
```

SyncGo validates this during startup.

When valid gRPC configuration is present, the search client creates an OpenSearch gRPC connection and uses the OpenSearch `DocumentService` for bulk operations.

Example:

```yaml
search_engine:
  name: opensearch
  address: http://localhost:9200
  index: users

  grpc:
    host: opensearch.example.com
    port: 9300
```

If `grpc` is not configured, OpenSearch uses the HTTP bulk API.

---

# Batcher configuration

The batcher controls how PostgreSQL changes are accumulated before being sent to Elasticsearch/OpenSearch.

```yaml
batcher:
  size: 100
  flush_interval: 50ms
```

| Parameter        | Description                                                       |
| ---------------- | ----------------------------------------------------------------- |
| `size`           | Maximum number of buffered changes before a size-triggered flush  |
| `flush_interval` | Maximum idle period before buffered committed changes are flushed |

The default values are:

```text
size: 100
flush_interval: 50ms
```

These defaults are defined in the configuration package.

## Batch size

Example:

```yaml
batcher:
  size: 1000
```

A larger batch can improve throughput by reducing the number of requests sent to the search engine.

However, larger batches also increase:

* memory usage
* individual request size
* request latency
* impact of a failed bulk request

Start with a moderate value such as:

```yaml
size: 100
```

and tune it according to production workload.

## Flush interval

Example:

```yaml
batcher:
  flush_interval: 100ms
```

This prevents low-volume workloads from waiting indefinitely for the batch to become full.

The timer-based flush can be disabled by setting:

```yaml
flush_interval: 0
```

However, for normal production workloads it is recommended to keep the interval enabled.

The batcher flushes committed records and retains records that have not yet been successfully committed.

---

# Prometheus metrics

SyncGo can expose Prometheus metrics through an HTTP endpoint.

Configuration:

```yaml
metrics:
  port: 9090
```

The metrics endpoint is:

```text
http://localhost:9090/metrics
```

For example:

```bash
curl http://localhost:9090/metrics
```

If:

```yaml
metrics:
  port: 0
```

or the `metrics` section is omitted, the metrics HTTP server is disabled.

The application starts the metrics server only when the configured port is greater than zero.

A typical production configuration is:

```yaml
metrics:
  port: 9090
```

Then Prometheus can scrape:

```yaml
scrape_configs:
  - job_name: syncgo
    static_configs:
      - targets:
          - syncgo-host:9090
```

---

# Complete configuration example

```yaml
postgresql:
  user: postgres
  password: postgres
  host: postgres.example.com
  port: 5432
  database: application
  slot_name: syncgo_slot
  publication_name: syncgo_publication
  id_column: id

search_engine:
  name: opensearch
  address: https://opensearch.example.com:9200
  username: admin
  password: secret
  index: application

  connection_timeout: 30s

  tls:
    ca_cert: /etc/syncgo/certs/ca.crt
    insecure_skip_verify: false

  keep_alive:
    max_conn_duration: 5m
    max_idle_conn_duration: 1m

batcher:
  size: 500
  flush_interval: 100ms

metrics:
  port: 9090
```

---

# Running SyncGo

## Run from source

```bash
go run ./cmd/syncgo --config ./config.yaml
```

## Run the compiled binary

```bash
./bin/syncgo --config ./config.yaml
```

## Production example

```bash
/usr/local/bin/syncgo --config /etc/syncgo/config.yaml
```

SyncGo handles `SIGINT` and `SIGTERM` and performs a graceful shutdown. During shutdown, the current batch is flushed and the PostgreSQL replication connection is closed.

---

# Demo

The repository contains a complete local E2E environment based on Docker Compose.

The demo starts:

* PostgreSQL
* Elasticsearch
* OpenSearch

The current E2E Compose configuration exposes PostgreSQL on `5432`, Elasticsearch on `9200`, and OpenSearch HTTP on `9300`.

## 1. Start the demo

From the repository root:

```bash
make demo
```

The `demo` target:

1. builds SyncGo;
2. prepares the E2E environment;
3. starts SyncGo using `./e2e/config.yaml`.

The relevant Makefile target is effectively:

```make
demo: build prepare
	./bin/syncgo --config ./e2e/config.yaml
```

## 2. Generate PostgreSQL changes

Keep SyncGo running and open another terminal:

```bash
make generate_data
```

The demo inserts several rows, updates two rows, and deletes one row from the PostgreSQL `test` table.

## 3. Check Elasticsearch

The demo configuration uses:

```yaml
search_engine:
  name: elasticsearch
  address: http://localhost:9200
  index: test_index
```

You can query the resulting documents with:

```bash
curl -X GET "http://localhost:9200/test_index/_search?pretty"
```

This is also the example used by the repository's existing README.

---

# Demo configuration

The current E2E configuration is:

```yaml
postgresql:
  user: postgres
  password: postgres
  host: localhost
  port: 5432
  database: postgres
  slot_name: test_slot
  publication_name: test_publication

search_engine:
  name: elasticsearch
  address: http://localhost:9200
  username: admin
  password: Es123456
  index: test_index

batcher:
  size: 100
  flush_interval: 50ms
```

The E2E environment automatically configures PostgreSQL for logical replication, creates the test table, and creates the `test_publication` publication.

## Demo

```bash
make demo
```

Builds SyncGo, prepares the Docker-based E2E environment, and starts SyncGo using the E2E configuration.

## Generate demo data

```bash
make generate_data
```

Inserts/updates/deletes records in the demo PostgreSQL instance to demonstrate CDC synchronization.

---

# End-to-end tests

In addition to the demo above, the repository has two automated end-to-end test suites
under `e2e/`, both built with the `e2e` build tag:

* `e2e/elastic` and `e2e/opensearch` — exercise the search engine clients directly
  (bulk indexing, batching, index/document existence) against real Elasticsearch and
  OpenSearch instances. They expect the E2E Docker Compose stack to already be running
  (`make prepare`).

* `e2e/pipeline` — runs the full CDC pipeline: a real PostgreSQL instance with logical
  replication, and SyncGo itself running either as a **locally built binary**
  (`go build`) or as its **Docker image** (`docker build` + `docker run`, on the same
  Compose network, configured from a generated file under `e2e/`). Each test provisions
  its own isolated table/publication/slot/index, writes a large batch of rows spanning a
  variety of column types (strings, numbers, booleans, nested JSON, NULLs, timestamps),
  updates and deletes a subset, and verifies — by reading documents back from
  Elasticsearch/OpenSearch — that every surviving row synced correctly and every deleted
  row is gone. This suite manages its own Docker Compose lifecycle (including working
  around a locally occupied PostgreSQL port) and does not require `make prepare` first.

Run them with:

```bash
make test-e2e-client    # e2e/elastic + e2e/opensearch
make test-e2e-pipeline  # e2e/pipeline (binary and Docker image)
make test-e2e           # both
```

---

# PostgreSQL permissions

The PostgreSQL user used by SyncGo must be able to connect to the configured database and use logical replication.

Depending on your PostgreSQL setup, this may require replication privileges, for example:

```sql
ALTER ROLE syncgo WITH REPLICATION;
```

The exact privileges should be adapted to your PostgreSQL security model.

SyncGo also needs to be able to access the publication and replication slot.

SyncGo checks whether the configured replication slot exists and creates it when necessary.

# License

SyncGo is licensed under the MIT License.
