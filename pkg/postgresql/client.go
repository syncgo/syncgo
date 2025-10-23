package postgresql

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PgxPool interface {
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	Ping(ctx context.Context) error
	Close()
}

type Config struct {
	User     string
	Password string
	Host     string
	Port     string
	Database string
}

type Postgres struct {
	Builder squirrel.StatementBuilderType
	Pool    PgxPool
}

func New(cfg Config) (*Postgres, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	slog.Debug("connection url", slog.String("url", url))

	pgCfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		slog.Error("failed to parse postgres config", slog.String("error", err.Error()))

		return nil, err
	}

	conn, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		slog.Error("pool constructor failed", slog.String("error", err.Error()))

		return nil, err
	}

	return &Postgres{
		Pool:    conn,
		Builder: squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar),
	}, conn.Ping(ctx)
}

func (p *Postgres) Close() {
	slog.Debug("close postgres connection")
	if p.Pool != nil {
		p.Pool.Close()
	}
}
