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
	// Replication enables the replication=database connection parameter,
	// turning the connection into a walsender connection for logical
	// replication. Regular (admin/query) connections leave it off.
	Replication bool
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
