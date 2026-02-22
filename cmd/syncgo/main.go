package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/romanchechyotkin/syncgo/internal/replication"
	"github.com/romanchechyotkin/syncgo/pkg/config"
	"github.com/romanchechyotkin/syncgo/pkg/elasticsearch"
	"github.com/romanchechyotkin/syncgo/pkg/http_client"
	_ "github.com/romanchechyotkin/syncgo/pkg/logger"
	"github.com/romanchechyotkin/syncgo/pkg/metrics"
	"github.com/romanchechyotkin/syncgo/pkg/opensearch"
	"github.com/romanchechyotkin/syncgo/pkg/postgresql"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const cancelTimeout = 30 * time.Second

func main() {
	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	initCtx, cancel := context.WithTimeout(ctx, cancelTimeout)
	defer cancel()

	configPath := parseConfigFlag()

	cfg, err := config.LoadFromYAML(configPath)
	if err != nil {
		slog.Error("failed to load config", slog.String("err", err.Error()))
		os.Exit(1)
	}

	monitoring := metrics.New()

	if cfg.Elasticsearch != nil {
		esClient, err := initEsClient(initCtx, cfg.Elasticsearch, monitoring)
		if err != nil {
			slog.Error("failed init elasticsearch connection", slog.String("err", err.Error()))
			os.Exit(1)
		}
		_ = esClient
	}
	if cfg.OpenSearch != nil {
		osClient, err := initOpenSearchClient(initCtx, cfg.OpenSearch, monitoring)
		if err != nil {
			slog.Error("failed init opensearch connection", slog.String("err", err.Error()))
			os.Exit(1)
		}
		_ = osClient
	}

	replicationConn, err := initPostgresqlReplicationConn(initCtx, cfg)
	if err != nil {
		slog.Error("failed init postgresql connection", slog.String("err", err.Error()))
		os.Exit(1)
	}

	startMetricsServer(ctx, cfg)

	if err = replicationConn.StartReplication(ctx); err != nil {
		slog.Error("failed to start replication", slog.String("err", err.Error()))
		os.Exit(1)
	}

	if err := replicationConn.Close(); err != nil {
		slog.Error("failed to close connection", slog.String("err", err.Error()))
		os.Exit(1)
	}

	slog.Info("syncgo stopped successfully")
}

func initEsClient(ctx context.Context, cfg *config.SearchConfig, monitoring *metrics.SearchMetrics) (*elasticsearch.Client, error) {
	esClient, err := elasticsearch.New(ctx, elasticsearch.Config{
		Addresses:         cfg.Addresses,
		Username:          cfg.Username,
		Password:          cfg.Password,
		Index:             cfg.Index,
		ConnectionTimeout: cfg.ConnectionTimeout,
		GzipCompression:   cfg.GzipCompression,
		TLS:               searchTLSToClient(cfg.TLS),
		KeepAlive:         searchKeepAliveToClient(cfg.KeepAlive),
	}, monitoring)
	if err != nil {
		slog.Error("failed to create elasticsearch client", slog.String("error", err.Error()))
		return nil, err
	}

	return esClient, nil
}

func initOpenSearchClient(ctx context.Context, cfg *config.OpenSearchConfig, monitoring *metrics.SearchMetrics) (*opensearch.Client, error) {
	osCfg := opensearch.Config{
		Addresses:         cfg.Addresses,
		Username:          cfg.Username,
		Password:          cfg.Password,
		Index:             cfg.Index,
		ConnectionTimeout: cfg.ConnectionTimeout,
		GzipCompression:   cfg.GzipCompression,
		TLS:               searchTLSToClient(cfg.TLS),
		KeepAlive:         searchKeepAliveToClient(cfg.KeepAlive),
	}
	if cfg.UseGRPC() {
		osCfg.GRPC = &opensearch.GRPCConfig{
			Host: cfg.GRPC.Host,
			Port: cfg.GRPC.Port,
		}
	}
	osClient, err := opensearch.New(ctx, osCfg, monitoring)
	if err != nil {
		slog.Error("failed to create opensearch client", slog.String("error", err.Error()))
		return nil, err
	}
	return osClient, nil
}

func initPostgresqlReplicationConn(ctx context.Context, cfg *config.Config) (*replication.LogicalReplicationConn, error) {
	conn, err := postgresql.New(ctx, postgresql.Config{
		User:     cfg.PostgreSQL.User,
		Password: cfg.PostgreSQL.Password,
		Host:     cfg.PostgreSQL.Host,
		Port:     cfg.PostgreSQL.Port,
		Database: cfg.PostgreSQL.Database,
	})
	if err != nil {
		slog.Error("failed to create postgresql connection", slog.String("error", err.Error()))
		return nil, err
	}

	return replication.New(ctx, conn)
}

func searchTLSToClient(t *config.SearchTLSConfig) *http_client.ClientTLSConfig {
	if t == nil {
		return nil
	}
	return &http_client.ClientTLSConfig{
		CACert:             t.CACert,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

func searchKeepAliveToClient(k *config.SearchKeepAliveConfig) *http_client.ClientKeepAliveConfig {
	if k == nil {
		return nil
	}
	return &http_client.ClientKeepAliveConfig{
		MaxConnDuration:     k.MaxConnDuration,
		MaxIdleConnDuration: k.MaxIdleConnDuration,
	}
}

func startMetricsServer(ctx context.Context, cfg *config.Config) {
	if cfg.Metrics.Port <= 0 {
		return
	}
	addr := net.JoinHostPort("0.0.0.0", strconv.Itoa(cfg.Metrics.Port))
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed", slog.String("err", err.Error()))
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("metrics server shutdown failed", slog.String("err", err.Error()))
		}
	}()
	slog.Info("metrics server listening", slog.String("addr", addr))
}

func parseConfigFlag() string {
	var configPath string

	flag.StringVar(&configPath, "config", "", "Path to configuration file (YAML)")
	flag.StringVar(&configPath, "cfg", "", "Path to configuration file (YAML) (shorthand for --config)")
	flag.Parse()

	return configPath
}
