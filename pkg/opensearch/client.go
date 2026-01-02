package opensearch

import (
	"context"
	"fmt"
	"log/slog"

	opensearch "github.com/opensearch-project/opensearch-go/v4"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type Client struct {
	client *opensearchapi.Client
	index  string
}

type Config struct {
	Addresses []string
	Username  string
	Password  string
	APIKey    string
	CloudID   string
	Index     string
}

func New(ctx context.Context, cfg Config) (*Client, error) {
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

	return &Client{
		client: client,
		index:  cfg.Index,
	}, nil
}

func (c *Client) Bulk(ctx context.Context, data []byte) error {
	return nil
}
