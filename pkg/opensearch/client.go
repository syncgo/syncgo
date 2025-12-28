package opensearch

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	opensearch "github.com/opensearch-project/opensearch-go/v4"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type Config struct {
	Addresses []string
	Username  string
	Password  string
	APIKey    string
	CloudID   string
}

func New(ctx context.Context, cfg Config) (*opensearchapi.Client, error) {
	slog.Debug("elasticsearch/opensearch config",
		slog.String("addresses", strings.Join(cfg.Addresses, ",")),
		slog.String("username", cfg.Username),
		slog.Bool("has_password", cfg.Password != ""),
		slog.Bool("has_api_key", cfg.APIKey != ""),
		slog.Bool("has_cloud_id", cfg.CloudID != ""),
	)

	opensearchCfg := opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: cfg.Addresses,
			Username:  cfg.Username,
			Password:  cfg.Password,
		},
	}
	client, err := opensearchapi.NewClient(opensearchCfg)
	if err != nil {
		slog.Error("failed to create opensearch client", slog.String("error", err.Error()))
		return nil, err
	}

	// Test the connection
	res, err := client.Ping(ctx, &opensearchapi.PingReq{})
	if err != nil {
		slog.Error("failed to ping opensearch", slog.String("error", err.Error()))
		return nil, err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			slog.Error("failed to close opensearch response body", slog.String("error", err.Error()))
		}
	}()

	if res.IsError() {
		err := fmt.Errorf("opensearch ping failed with status: %s", res.Status())
		slog.Error("opensearch ping failed", slog.String("error", err.Error()))
		return nil, err
	}

	slog.Debug("opensearch connection established successfully")

	return client, nil
}
