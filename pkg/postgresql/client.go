package postgresql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/syncgo/syncgo/pkg/logger"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// replicationPlugin is the logical decoding output plugin used by syncgo.
const replicationPlugin = "pgoutput"

// stmtPrefix prefixes the prepared statement names derived from SQL text.
const stmtPrefix = "syncgo_"

// Config holds the PostgreSQL connection configuration.
type Config struct {
	User     string
	Password string
	Host     string
	Port     string
	Database string
	// Replication enables the replication=database connection parameter,
	// turning the connection into a walsender connection for logical
	// replication. Regular (admin/query) connections leave it off.
	Replication bool
}

// Client wraps a PostgreSQL connection with helpers for the replication
// admin objects (publications, logical replication slots) and for running
// ad-hoc statements, e.g. from tests.
type Client struct {
	conn *pgconn.PgConn
}

// New opens a PostgreSQL connection described by cfg.
func New(ctx context.Context, cfg Config) (*Client, error) {
	conn, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

// connect opens a raw PostgreSQL connection described by cfg.
func connect(ctx context.Context, cfg Config) (*pgconn.PgConn, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	if cfg.Replication {
		url += "?replication=database"
	}

	pgCfg, err := pgconn.ParseConfig(url)
	if err != nil {
		logger.Error("failed to parse postgres config", err)
		return nil, err
	}

	conn, err := pgconn.ConnectConfig(ctx, pgCfg)
	if err != nil {
		logger.Error("failed to connect to postgres", err)
		return nil, err
	}

	return conn, nil
}

// Close closes the underlying connection.
func (c *Client) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}

// PgConn exposes the underlying low-level connection, e.g. for pglogrepl.
func (c *Client) PgConn() *pgconn.PgConn {
	return c.conn
}

// PublicationExists reports whether the publication exists.
func (c *Client) PublicationExists(ctx context.Context, pubName string) (bool, error) {
	rows, err := c.queryPrepared(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
		[]string{pubName},
	)
	if err != nil {
		return false, fmt.Errorf("failed to check publication %q: %w", pubName, err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		return false, fmt.Errorf("publication exists query returned %d rows of %d columns", len(rows), len(rows[0]))
	}

	return string(rows[0][0]) == "t", nil
}

// EnsurePublication creates the publication for the given tables if it does
// not exist yet. It is idempotent: an existing publication is left as is.
func (c *Client) EnsurePublication(ctx context.Context, pubName string, tables []string) error {
	exists, err := c.PublicationExists(ctx, pubName)
	if err != nil {
		return err
	}
	if exists {
		slog.Info("publication already exists", slog.String("name", pubName))
		return nil
	}

	// CREATE PUBLICATION takes identifiers, not values, so the statement
	// cannot be parameterized; identifiers are quoted manually.
	sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		QuoteIdent(pubName), strings.Join(quoteIdents(tables), ", "))

	if _, err := c.conn.Exec(ctx, sql).ReadAll(); err != nil {
		logger.Error("failed to create publication", err)
		return fmt.Errorf("failed to create publication %q: %w", pubName, err)
	}

	slog.Info("created publication",
		slog.String("name", pubName),
		slog.String("tables", strings.Join(tables, ", ")),
	)

	return nil
}

// PublicationTables returns the schema-qualified names of the tables
// currently in the publication.
func (c *Client) PublicationTables(ctx context.Context, pubName string) ([]string, error) {
	rows, err := c.queryPrepared(ctx,
		"SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1",
		[]string{pubName},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables of publication %q: %w", pubName, err)
	}

	tables := make([]string, 0, len(rows))
	for _, row := range rows {
		tables = append(tables, string(row[0]))
	}

	return tables, nil
}

// DropPublication drops the publication if it exists.
//
// The publication name is an identifier, not a value, so it cannot be a
// prepared statement parameter; it is quoted into the SQL text instead.
func (c *Client) DropPublication(ctx context.Context, pubName string) error {
	sql := "DROP PUBLICATION IF EXISTS " + QuoteIdent(pubName)

	if _, err := c.conn.Exec(ctx, sql).ReadAll(); err != nil {
		return fmt.Errorf("failed to drop publication %q: %w", pubName, err)
	}

	slog.Info("dropped publication", slog.String("name", pubName))

	return nil
}

// ReplicationSlotExists reports whether the logical replication slot exists.
func (c *Client) ReplicationSlotExists(ctx context.Context, slotName string) (bool, error) {
	rows, err := c.queryPrepared(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		[]string{slotName},
	)
	if err != nil {
		return false, fmt.Errorf("failed to check replication slot %q: %w", slotName, err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		return false, fmt.Errorf("replication slot exists query returned %d rows of %d columns", len(rows), len(rows[0]))
	}

	return string(rows[0][0]) == "t", nil
}

// EnsureReplicationSlot creates the logical replication slot if it does not
// exist yet. It is idempotent: an existing slot is left as is.
//
// The slot is created with the pg_create_logical_replication_slot SQL
// function instead of the CREATE_REPLICATION_SLOT replication command,
// because the admin connection is a regular (non-walsender) connection.
// The slot name and plugin are passed as prepared statement parameters.
func (c *Client) EnsureReplicationSlot(ctx context.Context, slotName string) error {
	exists, err := c.ReplicationSlotExists(ctx, slotName)
	if err != nil {
		return err
	}
	if exists {
		slog.Info("replication slot already exists", slog.String("name", slotName))
		return nil
	}

	if _, err := c.queryPrepared(ctx,
		"SELECT pg_create_logical_replication_slot($1, $2)",
		[]string{slotName, replicationPlugin},
	); err != nil {
		logger.Error("failed to create replication slot", err)
		return fmt.Errorf("failed to create replication slot %q: %w", slotName, err)
	}

	slog.Info("created replication slot", slog.String("name", slotName))

	return nil
}

// CreateReplicationSlotWithSnapshot creates a logical replication slot on a
// walsender connection with the CREATE_REPLICATION_SLOT replication command
// and SNAPSHOT 'export', so the initial data copy can read the database
// state exactly at the slot creation point.
//
// It must be called on a replication-mode connection (Config.Replication
// is true); the connection must stay open while the exported snapshot is
// being consumed by another session, and must run no other commands.
//
// Returns the slot's consistent point (the LSN to start streaming from) and
// the exported snapshot name.
func CreateReplicationSlotWithSnapshot(ctx context.Context, conn *pgconn.PgConn, slotName string) (consistentPoint string, snapshotName string, err error) {
	result, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, replicationPlugin, pglogrepl.CreateReplicationSlotOptions{
		SnapshotAction: "EXPORT_SNAPSHOT",
		Mode:           pglogrepl.LogicalReplication,
	})
	if err != nil {
		return "", "", fmt.Errorf("failed to create replication slot %q with snapshot: %w", slotName, err)
	}

	slog.Info("created replication slot with exported snapshot",
		slog.String("name", slotName),
		slog.String("consistentPoint", result.ConsistentPoint),
		slog.String("snapshotName", result.SnapshotName),
	)

	return result.ConsistentPoint, result.SnapshotName, nil
}

// DropReplicationSlot drops the logical replication slot if it exists.
func (c *Client) DropReplicationSlot(ctx context.Context, slotName string) error {
	if _, err := c.queryPrepared(ctx,
		"SELECT pg_drop_replication_slot($1) WHERE EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		[]string{slotName},
	); err != nil {
		return fmt.Errorf("failed to drop replication slot %q: %w", slotName, err)
	}

	slog.Info("dropped replication slot", slog.String("name", slotName))

	return nil
}

// Exec runs a statement with optional text parameters. The statement is
// cached on the connection, so repeated calls (e.g. in test loops) reuse it.
// Identifiers cannot be passed as parameters; quote them into the SQL text
// instead (see QuoteIdent).
func (c *Client) Exec(ctx context.Context, sql string, args ...string) error {
	_, err := c.queryPrepared(ctx, sql, args)

	return err
}

// Query runs a statement with optional text parameters and returns all
// result rows. The statement is cached on the connection like in Exec.
// Identifiers cannot be passed as parameters; quote them into the SQL text
// instead (see QuoteIdent).
func (c *Client) Query(ctx context.Context, sql string, args ...string) ([][][]byte, error) {
	return c.queryPrepared(ctx, sql, args)
}

// QuoteIdent quotes a single SQL identifier (schema, table, publication or
// slot name) for interpolation into a SQL statement. Embedded double quotes
// are doubled per the SQL standard.
func QuoteIdent(name string) string {
	return quoteIdent(name)
}

// QuoteQualifiedIdent quotes a possibly schema-qualified name (e.g.
// "public.users"), quoting each dot-separated part individually. A name
// without a dot is treated as a bare identifier.
func QuoteQualifiedIdent(name string) string {
	return quoteQualifiedIdent(name)
}

// quoteIdent quotes a single SQL identifier (schema, table, publication or
// slot name). Embedded double quotes are doubled per the SQL standard.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteQualifiedIdent quotes a possibly schema-qualified name (e.g.
// "public.users"), quoting each dot-separated part individually. A name
// without a dot is treated as a bare identifier.
func quoteQualifiedIdent(name string) string {
	parts := strings.Split(name, ".")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = quoteIdent(part)
	}

	return strings.Join(quoted, ".")
}

func quoteIdents(names []string) []string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quoteQualifiedIdent(name)
	}

	return quoted
}

// queryPrepared prepares the statement derived from the SQL text (re-using
// an existing statement with the same hash name on this connection) and
// executes it with the given text parameters, returning all result rows.
func (c *Client) queryPrepared(ctx context.Context, sql string, args []string) ([][][]byte, error) {
	name := stmtPrefix + stmtName(sql)

	_, err := c.conn.Prepare(ctx, name, sql, nil)
	if err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != pgerrcode.DuplicatePreparedStatement {
			return nil, err
		}
	}

	paramValues := make([][]byte, len(args))
	for i, arg := range args {
		paramValues[i] = []byte(arg)
	}

	result := c.conn.ExecPrepared(ctx, name, paramValues, nil, nil).Read()
	if result.Err != nil {
		return nil, result.Err
	}

	return result.Rows, nil
}

// stmtName derives a unique prepared statement name from the SQL text. The
// same SQL always maps to the same name, so repeated calls reuse the cached
// server-side statement while different statements never collide.
func stmtName(sql string) string {
	sum := sha256.Sum256([]byte(sql))

	return hex.EncodeToString(sum[:8])
}
