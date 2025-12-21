package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/elastic/go-elasticsearch/v9"
	opensearch "github.com/opensearch-project/opensearch-go/v4"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type ServiceType byte

const (
	OpenSearch = iota
	ElasticSearch
)

type Config struct {
	Addresses []string
	Username  string
	Password  string
	APIKey    string
	CloudID   string

	ServiceType ServiceType
}

type Client interface {
	// Bulk(ctx context.Context, req opensearchapi.BulkReq) (opensearchapi.BulkReq, error)
}

func New(ctx context.Context, cfg Config) (Client, error) {
	slog.Debug("elasticsearch/opensearch config",
		slog.String("addresses", strings.Join(cfg.Addresses, ",")),
		slog.String("username", cfg.Username),
		slog.Bool("has_password", cfg.Password != ""),
		slog.Bool("has_api_key", cfg.APIKey != ""),
		slog.Bool("has_cloud_id", cfg.CloudID != ""),
	)

	switch cfg.ServiceType {
	case OpenSearch:
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

		return Client(client), nil

	case ElasticSearch:
		esCfg := elasticsearch.Config{
			Addresses: cfg.Addresses,
			Username:  cfg.Username,
			Password:  cfg.Password,
			APIKey:    cfg.APIKey,
			CloudID:   cfg.CloudID,
		}

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

		return Client(client), nil
	default:
		return nil, errors.New("no such service type")
	}
}
