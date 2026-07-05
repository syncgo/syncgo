package postgresql

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func NewStandard(ctx context.Context, cfg Config) (*pgconn.PgConn, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	pgCfg, err := pgconn.ParseConfig(url)
	if err != nil {
		slog.Error("failed to parse postgres config", slog.String("error", err.Error()))
		return nil, err
	}

	conn, err := pgconn.ConnectConfig(ctx, pgCfg)
	if err != nil {
		slog.Error("failed to connect to postgres", slog.String("error", err.Error()))
		return nil, err
	}

	return conn, nil
}

func ReplicationSlotExists(ctx context.Context, conn *pgconn.PgConn, slotName string) (bool, error) {
	return queryExists(ctx, conn,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		slotName,
	)
}

func PublicationExists(ctx context.Context, conn *pgconn.PgConn, pubName string) (bool, error) {
	return queryExists(ctx, conn,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
		pubName,
	)
}

func queryExists(ctx context.Context, conn *pgconn.PgConn, sql string, arg string) (bool, error) {
	result := conn.ExecParams(ctx, sql, [][]byte{[]byte(arg)}, []uint32{pgtype.TextOID}, nil, nil)
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
