package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/syncgo/syncgo/internal/processor"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/logger"
	"github.com/syncgo/syncgo/pkg/metrics"
)

// version is set at build time via -ldflags "-X github.com/syncgo/syncgo/pkg/cli.version=..."
var version = "dev"

const (
	usage = `SyncGo - sync PostgreSQL changes to Elasticsearch or OpenSearch.

Usage:
  syncgo <command> [flags]

Commands:
  prepare    Create publication, replication slot and index (idempotent)
  run        Run the sync process
  version    Print the SyncGo version

Flags:
  -config, -cfg   Path to configuration file (YAML)

Run 'syncgo <command> -h' for command-specific flags.`
)

// Main is the entry point of the syncgo binary. It parses the subcommand,
// loads the configuration and dispatches to the matching command handler.
func Main() {
	if len(os.Args) < 2 {
		fmt.Println(usage)
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	switch command {
	case "prepare":
		runPrepare(args)
	case "run":
		runRun(args)
	case "version":
		printVersion()
	case "-h", "--help", "help":
		fmt.Println(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", command)
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(1)
	}
}

func printVersion() {
	fmt.Println(version)
}

// parseConfigFlag parses the -config/-cfg flag and returns the config path.
func parseConfigFlag(args []string) string {
	fs := newFlagSet()
	cfgPath := fs.String("config", "", "Path to configuration file (YAML)")
	short := fs.String("cfg", "", "Path to configuration file (YAML) (shorthand for --config)")

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if *short != "" {
		return *short
	}

	return *cfgPath
}

func loadConfig(args []string) *config.Config {
	cfg, err := config.LoadFromYAML(parseConfigFlag(args))
	if err != nil {
		logger.Error("failed to load config", err)
		os.Exit(1)
	}

	return cfg
}

// runPrepare creates the publication, replication slot and search index.
// Every step is idempotent: existing objects are left as is.
func runPrepare(args []string) {
	ctx := context.Background()
	cfg := loadConfig(args)

	if err := processor.Prepare(ctx, cfg, metrics.DefaultMetrics); err != nil {
		logger.Error("prepare failed", err)
		os.Exit(1)
	}

	fmt.Println("prepare finished successfully")
}

// runRun starts the sync process. Preparation logic (publication/slot/index
// creation) is expected to be done via the prepare command.
func runRun(args []string) {
	ctx, cancel := contextWithSignals()
	defer cancel()

	cfg := loadConfig(args)

	p, err := processor.New(ctx, cfg, metrics.DefaultMetrics)
	if err != nil {
		logger.Error("failed to initialize processor", err)
		os.Exit(1)
	}

	if err := p.Run(ctx); err != nil {
		logger.Error("processor run failed", err)
		os.Exit(1)
	}

	fmt.Println("syncgo stopped successfully")
}
