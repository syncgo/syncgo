package logger

import (
	"log/slog"
	"os"
	"strings"
)

func init() {
	newLogger()
}

const defaultLevel = slog.LevelDebug

func newLogger() {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: configLevel(),
	})

	slog.SetDefault(slog.New(handler))
}

func configLevel() slog.Level {
	var logLevel slog.Level

	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = defaultLevel
	}

	return logLevel
}
