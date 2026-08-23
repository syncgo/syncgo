package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/syncgo/syncgo/internal/processor"
	"github.com/syncgo/syncgo/pkg/config"
	_ "github.com/syncgo/syncgo/pkg/logger"
	"github.com/syncgo/syncgo/pkg/metrics"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	cfg, err := config.LoadFromYAML(parseConfigFlag())
	if err != nil {
		slog.Error("failed to load config", slog.String("err", err.Error()))
		os.Exit(1)
	}

	monitoring := metrics.New()

	p, err := processor.New(ctx, cfg, monitoring)
	if err != nil {
		slog.Error("failed to initialize processor", slog.String("err", err.Error()))
		os.Exit(1)
	}

	if err := p.Run(ctx); err != nil {
		slog.Error("processor run failed", slog.String("err", err.Error()))
		os.Exit(1)
	}

	slog.Info("syncgo stopped successfully")
}

func parseConfigFlag() string {
	var configPath string

	flag.StringVar(&configPath, "config", "", "Path to configuration file (YAML)")
	flag.StringVar(&configPath, "cfg", "", "Path to configuration file (YAML) (shorthand for --config)")
	flag.Parse()

	return configPath
}
