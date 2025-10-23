package elasticsearch

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

type Config struct {
	Addresses []string
	Username  string
	Password  string
	APIKey    string
	CloudID   string
}

func New(cfg Config) (*elasticsearch.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	esCfg := elasticsearch.Config{
		Addresses: cfg.Addresses,
		Username:  cfg.Username,
		Password:  cfg.Password,
		APIKey:    cfg.APIKey,
		CloudID:   cfg.CloudID,
	}

	slog.Debug("elasticsearch config",
		slog.String("addresses", strings.Join(cfg.Addresses, ",")),
		slog.String("username", cfg.Username),
		slog.Bool("has_password", cfg.Password != ""),
		slog.Bool("has_api_key", cfg.APIKey != ""),
		slog.Bool("has_cloud_id", cfg.CloudID != ""),
	)

	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		slog.Error("failed to create elasticsearch client", slog.String("error", err.Error()))
		return nil, err
	}

	// Test the connection
	res, err := client.Ping(client.Ping.WithContext(ctx))
	if err != nil {
		slog.Error("failed to ping elasticsearch", slog.String("error", err.Error()))
		return nil, err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			slog.Error("failed to close elasticsearch response body", slog.String("error", err.Error()))
		}
	}()

	if res.IsError() {
		err := fmt.Errorf("elasticsearch ping failed with status: %s", res.Status())
		slog.Error("elasticsearch ping failed", slog.String("error", err.Error()))
		return nil, err
	}

	slog.Debug("elasticsearch connection established successfully")

	return client, nil
}
