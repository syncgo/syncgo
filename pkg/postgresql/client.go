package postgresql

import (
	"context"
	"fmt"

	"github.com/syncgo/syncgo/pkg/logger"

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
		logger.Error("failed to parse postgres config", err)
		return nil, err
	}

	conn, err := pgconn.ConnectConfig(ctx, pgCfg)
	if err != nil {
		logger.Error("pool constructor failed", err)
		return nil, err
	}

	return conn, nil
}
