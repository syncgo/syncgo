package cli

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
)

func newFlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("syncgo", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	return fs
}

func contextWithSignals() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}
