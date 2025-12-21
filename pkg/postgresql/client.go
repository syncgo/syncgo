package postgresql

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
)

type Config struct {
	User     string
	Password string
	Host     string
	Port     string
	Database string
}

func New(ctx context.Context, cfg Config) (*pgconn.PgConn, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?replication=database",
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
		slog.Error("pool constructor failed", slog.String("error", err.Error()))

		return nil, err
	}

	return conn, nil
}
