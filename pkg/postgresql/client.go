package postgresql

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/syncgo/syncgo/pkg/logger"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// replicationPlugin is the logical decoding output plugin used by syncgo.
const replicationPlugin = "pgoutput"

type Config struct {
	User        string
	Password    string
	Host        string
	Port        string
	Database    string
	Replication bool
}

type Client struct {
	conn *pgconn.PgConn
}

func New(ctx context.Context, cfg Config) (*Client, error) {
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
		logger.Error("pool constructor failed", err)
		return nil, err
	}

	return &Client{conn: conn}, nil
}

func (cl *Client) Close(ctx context.Context) error {
	return cl.conn.Close(ctx)
}

func (cl *Client) ReplicationSlotExists(ctx context.Context, slotName string) (bool, error) {
	return cl.queryExists(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		slotName,
	)
}

func (cl *Client) PublicationExists(ctx context.Context, pubName string) (bool, error) {
	return cl.queryExists(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
		pubName,
	)
}

// EnsurePublication creates the publication for the given tables if it does
// not exist yet. It is idempotent: an existing publication is left as is.
func (cl *Client) EnsurePublication(ctx context.Context, pubName string, tables []string) error {
	exists, err := cl.PublicationExists(ctx, pubName)
	if err != nil {
		return fmt.Errorf("failed to check publication %q: %w", pubName, err)
	}
	if exists {
		slog.Info("publication already exists", slog.String("name", pubName))
		return nil
	}

	sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		cl.quoteIdent(pubName), strings.Join(cl.quoteIdents(tables), ", "))

	if _, err := cl.conn.Exec(ctx, sql).ReadAll(); err != nil {
		logger.Error("failed to create publication", err)
		return err
	}

	slog.Info("created publication",
		slog.String("name", pubName),
		slog.String("tables", strings.Join(tables, ", ")),
	)

	return nil
}

// EnsureReplicationSlot creates the logical replication slot if it does not
// exist yet. It is idempotent: an existing slot is left as is.
//
// The slot is created with the pg_create_logical_replication_slot SQL
// function instead of the CREATE_REPLICATION_SLOT replication command,
// because the admin connection is a regular (non-walsender) connection.
// The slot name and plugin are passed as query parameters.
func (cl *Client) EnsureReplicationSlot(ctx context.Context, slotName string) error {
	exists, err := cl.ReplicationSlotExists(ctx, slotName)
	if err != nil {
		return fmt.Errorf("failed to check replication slot %q: %w", slotName, err)
	}
	if exists {
		slog.Info("replication slot already exists", slog.String("name", slotName))
		return nil
	}

	const sql = "SELECT pg_create_logical_replication_slot($1, $2)"
	args := [][]byte{[]byte(slotName), []byte(replicationPlugin)}

	if result := cl.conn.ExecParams(ctx, sql, args, []uint32{pgtype.TextOID, pgtype.TextOID}, nil, nil); !result.NextRow() {
		_, err := result.Close()
		logger.Error("failed to create replication slot", err)
		return fmt.Errorf("failed to create replication slot %q: %w", slotName, err)
	}

	slog.Info("created replication slot", slog.String("name", slotName))

	return nil
}

// quoteIdent quotes a single SQL identifier (schema, table, publication or
// slot name). Embedded double quotes are doubled per the SQL standard.
func (cl *Client) quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteQualifiedIdent quotes a possibly schema-qualified name (e.g.
// "public.users"), quoting each dot-separated part individually. A name
// without a dot is treated as a bare identifier.
func (cl *Client) quoteQualifiedIdent(name string) string {
	parts := strings.Split(name, ".")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = cl.quoteIdent(part)
	}

	return strings.Join(quoted, ".")
}

func (cl *Client) quoteIdents(names []string) []string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = cl.quoteQualifiedIdent(name)
	}

	return quoted
}

func (cl *Client) queryExists(ctx context.Context, sql string, arg string) (bool, error) {
	result := cl.conn.ExecParams(ctx, sql, [][]byte{[]byte(arg)}, []uint32{pgtype.TextOID}, nil, nil)
	if !result.NextRow() {
		_, err := result.Close()
		return false, fmt.Errorf("exists query returned no rows: %w", err)
	}

	values := result.Values()
	if len(values) != 1 {
		_, err := result.Close()
		return false, fmt.Errorf("expected one column in exists query result: %w", err)
	}

	exists := string(values[0]) == "t"
	_, err := result.Close()
	return exists, err
}
