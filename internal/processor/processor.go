package processor

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/syncgo/syncgo/internal/batcher"
	"github.com/syncgo/syncgo/internal/replication"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/http_client"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/postgresql"
	"github.com/syncgo/syncgo/pkg/search_engine_client"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	initTimeout            = 30 * time.Second
	metricsShutdownTimeout = 5 * time.Second
)

type Processor struct {
	client      *search_engine_client.Client
	batcher     *batcher.Batcher
	replication *replication.LogicalReplicationConn
	metricsSrv  *http.Server

	shutdownOnce sync.Once
	shutdownErr  error
}

func New(parentCtx context.Context, cfg *config.Config, monitoring *metrics.Metrics) (*Processor, error) {
	initCtx, cancel := context.WithTimeout(parentCtx, initTimeout)
	defer cancel()

	client, err := initClient(initCtx, cfg.SearchEngine, monitoring)
	if err != nil {
		return nil, err
	}

	b := initBatcher(parentCtx, cfg.Batcher, client, monitoring)

	replicationConn, err := initReplication(initCtx, cfg, b, monitoring)
	if err != nil {
		return nil, err
	}

	metricsSrv := startMetricsServer(cfg)

	return &Processor{
		client:      client,
		batcher:     b,
		replication: replicationConn,
		metricsSrv:  metricsSrv,
	}, nil
}

func (p *Processor) Run(ctx context.Context) error {
	if err := p.replication.StartReplication(ctx); err != nil {
		return err
	}

	<-ctx.Done()
	slog.Info("shutdown signal received")

	return p.Shutdown()
}

func (p *Processor) Shutdown() error {
	p.shutdownOnce.Do(func() {
		p.shutdownErr = p.shutdown()
	})
	return p.shutdownErr
}

func (p *Processor) shutdown() error {
	var errs []error

	if p.batcher != nil {
		p.batcher.Flush()
	}

	if p.replication != nil {
		if err := p.replication.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if p.client != nil {
		if err := p.client.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if p.metricsSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
		defer cancel()
		if err := p.metricsSrv.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func initBatcher(ctx context.Context, cfg config.BatcherConfig, sender batcher.BulkSender, monitoring *metrics.Metrics) *batcher.Batcher {
	b := batcher.NewBatcher(ctx, cfg.Size, cfg.FlushInterval, sender, monitoring)

	slog.Info("batcher initialized",
		slog.Int("size", cfg.Size),
		slog.Duration("flush_interval", cfg.FlushInterval),
	)

	return b
}

func initClient(ctx context.Context, cfg config.SearchEngineConfig, monitoring *metrics.Metrics) (*search_engine_client.Client, error) {
	client, err := search_engine_client.New(ctx, search_engine_client.Config{
		Name:              cfg.Name,
		Address:           cfg.Address,
		Username:          cfg.Username,
		Password:          cfg.Password,
		Index:             cfg.Index,
		ConnectionTimeout: cfg.ConnectionTimeout,
		GzipCompression:   cfg.GzipCompression,
		TLS:               searchTLSToClient(cfg.TLS),
		KeepAlive:         searchKeepAliveToClient(cfg.KeepAlive),
	}, monitoring)
	if err != nil {
		slog.Error("failed to create search engine client", slog.String("error", err.Error()))
		return nil, err
	}

	return client, nil
}

func initReplication(ctx context.Context, cfg *config.Config, b *batcher.Batcher, monitoring *metrics.Metrics) (*replication.LogicalReplicationConn, error) {
	pgCfg := postgresql.Config{
		User:     cfg.PostgreSQL.User,
		Password: cfg.PostgreSQL.Password,
		Host:     cfg.PostgreSQL.Host,
		Port:     cfg.PostgreSQL.Port,
		Database: cfg.PostgreSQL.Database,
	}

	conn, err := postgresql.New(ctx, pgCfg)
	if err != nil {
		slog.Error("failed to create postgresql connection", slog.String("error", err.Error()))
		return nil, err
	}

	return replication.New(ctx, replication.LogicalRepicationConfig{
		PublicationName: cfg.PostgreSQL.PublicationName,
		SlotName:        cfg.PostgreSQL.SlotName,
		IDColumn:        cfg.PostgreSQL.IDColumn,
		DB:              pgCfg,
	}, conn, b, monitoring)
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

func startMetricsServer(cfg *config.Config) *http.Server {
	if cfg.Metrics.Port <= 0 {
		return nil
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

	slog.Info("metrics server listening", slog.String("addr", addr))

	return srv
}
