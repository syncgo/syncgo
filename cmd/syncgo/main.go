package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/syncgo/syncgo/internal/processor"
	"github.com/syncgo/syncgo/pkg/config"
	_ "github.com/syncgo/syncgo/pkg/logger"
	"github.com/syncgo/syncgo/pkg/metrics"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "version":
		err = runVersion()
	case "prepare":
		err = runPrepare(os.Args[2:])
	case "run":
		err = run(os.Args[2:])
	case "-h", "--help", "help":
		printUsage()
		return
	default:
		slog.Error("unknown command", slog.String("cmd", os.Args[1]))
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		slog.Error("command failed", slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func runVersion() error {
	fmt.Printf("syncgo %s\n", version)
	return nil
}

func runPrepare(args []string) error {
	fmt.Printf("preparing slots: %s", args)
	return nil
}

func run(args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	cfgPath, err := parseConfigFlag(args)
	if err != nil {
		return err
	}

	cfg, err := config.LoadFromYAML(cfgPath)
	if err != nil {
		slog.Error("failed to load config", slog.String("err", err.Error()))
		os.Exit(1)
	}

	monitoring := metrics.New(nil)

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
	return nil
}

func parseConfigFlag(args []string) (string, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)

	var configPath string

	fs.StringVar(&configPath, "config", "", "Path to configuration file (YAML)")
	fs.StringVar(&configPath, "cfg", "", "Path to configuration file (YAML) (shorthand for --config)")

	if err := fs.Parse(args); err != nil {
		return "", err
	}
	return configPath, nil
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `syncgo is binary with few commands
	Usage:
	  syncgo version			prints version
	  syncgo prepare [flags]	run prepare commands for postgres (creates publication, slot, etc)
	  syncgo run [flags]		run sync process

	Use "syncgo <command> -h" for flags on specific command.`+"\n\n")
}
