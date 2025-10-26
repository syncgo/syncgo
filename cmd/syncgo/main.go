package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/romanchechyotkin/syncgo/pkg/logger"
	"github.com/romanchechyotkin/syncgo/pkg/postgresql"
)

func main() {
	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	replicationConn, err := initPostgresqlReplicationConn(ctx)
	if err != nil {
		slog.Error("failed init postgresql connection", slog.String("err", err.Error()))
		os.Exit(1)
	}

	if err = replicationConn.StartReplication(ctx); err != nil {
		slog.Error("failed to start replication", slog.String("err", err.Error()))
		os.Exit(1)
	}

	replicationConn.Close()
}

func initPostgresqlReplicationConn(ctx context.Context) (*postgresql.LogicalReplicationConn, error) {
	postgresqlCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	return postgresql.New(postgresqlCtx, postgresql.Config{
		User:     "postgres",
		Password: "postgres",
		Host:     "localhost",
		Port:     "5432",
		Database: "postgres",
	})
}
