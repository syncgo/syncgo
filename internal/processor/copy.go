package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/logger"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/postgresql"
	"github.com/syncgo/syncgo/pkg/search_engine_client"
)

// copyBatchSize is the number of rows read from PostgreSQL and sent to the
// search engine in one batch during the initial copy.
const copyBatchSize = 1000

// copyInitialData reads all publication tables inside a REPEATABLE READ
// transaction pinned to the slot's exported snapshot — i.e. the database
// state exactly at the slot's consistent point — and indexes the rows into
// the search engine. After this copy the replication stream starting at the
// consistent point continues without gaps or missed changes.
//
// It must be called while the walsender connection that exported the
// snapshot is still open.
func copyInitialData(ctx context.Context, cfg *config.Config, snapshotName string, monitoring *metrics.Metrics) error {
	client, err := search_engine_client.New(ctx, searchEngineClientConfig(cfg.SearchEngine), monitoring)
	if err != nil {
		return fmt.Errorf("failed to create search engine client for initial copy: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			logger.Error("failed to close search engine client after initial copy", closeErr)
		}
	}()

	pgConn, err := postgresql.New(ctx, postgresql.Config{
		User:     cfg.PostgreSQL.User,
		Password: cfg.PostgreSQL.Password,
		Host:     cfg.PostgreSQL.Host,
		Port:     cfg.PostgreSQL.Port,
		Database: cfg.PostgreSQL.Database,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to postgres for initial copy: %w", err)
	}
	defer func() {
		if closeErr := pgConn.Close(ctx); closeErr != nil {
			logger.Error("failed to close initial copy connection", closeErr)
		}
	}()

	// Pin the transaction to the exported snapshot before any query: the
	// whole transaction then sees the database state at the moment the
	// replication slot was created. The statements are sent as separate
	// Exec calls: Exec uses prepared statements and PostgreSQL forbids
	// multiple commands in a single prepared statement. SET TRANSACTION
	// SNAPSHOT does not accept parameters, so the name is quoted into the
	// statement text.
	if err := pgConn.Exec(ctx, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	if err := pgConn.Exec(ctx,
		fmt.Sprintf("SET TRANSACTION SNAPSHOT %s", quoteLiteral(snapshotName)),
	); err != nil {
		return fmt.Errorf("failed to import snapshot %q: %w", snapshotName, err)
	}

	total := 0
	for _, table := range cfg.PostgreSQL.Tables {
		n, err := copyTable(ctx, pgConn, client, cfg.PostgreSQL.IDColumn, table)
		if err != nil {
			return fmt.Errorf("failed to copy table %q: %w", table, err)
		}
		total += n
		slog.Info("copied table",
			slog.String("table", table),
			slog.Int("rows", n),
		)
	}

	slog.Info("initial copy finished", slog.Int("rows", total))

	return nil
}

// copyTable streams one table in batches of copyBatchSize rows and indexes
// each batch into the search engine. Returns the number of indexed rows.
func copyTable(ctx context.Context, pgConn *postgresql.Client, client *search_engine_client.Client, idColumn string, table string) (int, error) {
	names, err := tableColumnNames(ctx, pgConn, table)
	if err != nil {
		return 0, err
	}

	// Use a cursor-less streaming query: SELECT * with the column order
	// taken from information_schema.
	rows, err := pgConn.Query(ctx,
		fmt.Sprintf("SELECT * FROM %s", postgresql.QuoteQualifiedIdent(table)))
	if err != nil {
		return 0, err
	}

	total := 0
	batch := make(bulk_transformer.DataPayload, 0, copyBatchSize)

	for _, row := range rows {
		values := make(map[string]any, len(row))
		for i, raw := range row {
			if i >= len(names) {
				break
			}
			if raw == nil {
				continue
			}
			var v any
			if err := json.Unmarshal(raw, &v); err != nil {
				// Not JSON: keep the raw text (numbers, strings, bools).
				values[names[i]] = string(raw)
				continue
			}
			values[names[i]] = v
		}

		id, ok := documentIDValue(idColumn, values)
		if !ok {
			return total, fmt.Errorf("row without %q column in table %q", idColumn, table)
		}

		body, err := json.Marshal(values)
		if err != nil {
			return total, fmt.Errorf("failed to marshal row to JSON: %w", err)
		}

		batch = append(batch, bulk_transformer.Data{
			ID:     id,
			Action: bulk_transformer.Index,
			Body:   body,
		})

		if len(batch) >= copyBatchSize {
			if err := client.Bulk(ctx, batch); err != nil {
				return total, fmt.Errorf("failed to index batch of %d rows: %w", len(batch), err)
			}
			total += len(batch)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := client.Bulk(ctx, batch); err != nil {
			return total, fmt.Errorf("failed to index final batch of %d rows: %w", len(batch), err)
		}
		total += len(batch)
	}

	return total, nil
}

// tableColumnNames returns the column names of the given table in order.
func tableColumnNames(ctx context.Context, pgConn *postgresql.Client, table string) ([]string, error) {
	rows, err := pgConn.Query(ctx,
		"SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
		schemaOf(table), bareTableName(table))
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, string(row[0]))
	}

	if len(names) == 0 {
		return nil, fmt.Errorf("table %q not found in information_schema", table)
	}

	return names, nil
}

// schemaOf extracts the schema part of a possibly schema-qualified table
// name, defaulting to "public".
func schemaOf(table string) string {
	for i := len(table) - 1; i >= 0; i-- {
		if table[i] == '.' {
			return table[:i]
		}
	}

	return "public"
}

// bareTableName strips the schema qualifier from a table name.
func bareTableName(table string) string {
	for i := len(table) - 1; i >= 0; i-- {
		if table[i] == '.' {
			return table[i+1:]
		}
	}

	return table
}

// documentIDValue extracts the document id from the row values.
func documentIDValue(idColumn string, values map[string]any) (string, bool) {
	val, ok := values[idColumn]
	if !ok || val == nil {
		return "", false
	}

	return fmt.Sprint(val), true
}

// quoteLiteral quotes a string literal (e.g. a snapshot name) for
// interpolation into a SQL statement. Embedded single quotes are doubled per
// the SQL standard.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
