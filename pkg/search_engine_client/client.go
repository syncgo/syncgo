package search_engine_client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/romanchechyotkin/syncgo/internal/bulk_transformer"
	"github.com/romanchechyotkin/syncgo/pkg/gzip"
	"github.com/romanchechyotkin/syncgo/pkg/http_client"
	"github.com/syncgo/opensearchpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultConnTimeout = 30 * time.Second
	pingTimeoutLimit   = 10 * time.Second
	contentTypeNDJSON  = "application/x-ndjson"
)

// GRPCConfig holds gRPC connection settings for OpenSearch (optional).
// When set, Bulk() uses the gRPC DocumentService instead of HTTP.
type GRPCConfig struct {
	Host string
	Port int
}

type Config struct {
	Name              string
	Addresses         []string
	Username          string
	Password          string
	APIKey            string
	CloudID           string
	Index             string
	ConnectionTimeout time.Duration
	GzipCompression   gzip.GzipCompressionLevel
	TLS               *http_client.ClientTLSConfig
	KeepAlive         *http_client.ClientKeepAliveConfig
	// GRPC is optional. When set, the client uses gRPC for bulk operations instead of HTTP.
	GRPC *GRPCConfig
}

//go:generate go tool mockgen -source=client.go -destination=./mocks/monitoring_mock.go -package=mocks
type Monitoring interface {
	IncSearchRequests(backend, status string)
	AddSearchErrors(backend string, errorsCount float64)
}

type Client struct {
	name    string
	baseURL string

	pingClient *http_client.Client
	httpClient *http_client.Client

	docClient opensearchpb.DocumentServiceClient
	grpcConn  *grpc.ClientConn

	index   string
	timeout time.Duration

	monitoring Monitoring
}

func New(ctx context.Context, cfg Config, monitoring Monitoring) (*Client, error) {
	if len(cfg.Addresses) == 0 {
		return nil, fmt.Errorf("at least one address must be provided")
	}

	// Use the first address as the base URL
	baseURL := strings.TrimSuffix(cfg.Addresses[0], "/")

	// Build auth header
	var authHeader string
	if cfg.APIKey != "" {
		authHeader = "ApiKey " + cfg.APIKey
	} else if cfg.Username != "" && cfg.Password != "" {
		auth := base64.StdEncoding.EncodeToString([]byte(cfg.Username + ":" + cfg.Password))
		authHeader = "Basic " + auth
	}

	// Build TLS config
	var tlsConfig *http_client.ClientTLSConfig
	if cfg.TLS != nil {
		tlsConfig = &http_client.ClientTLSConfig{
			CACert:             cfg.TLS.CACert,
			InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
		}
	}

	timeout := cfg.ConnectionTimeout
	if timeout == 0 {
		timeout = defaultConnTimeout
	}

	pingClient, err := http_client.NewClient(&http_client.ClientConfig{
		Endpoint:             baseURL,
		ConnectionTimeout:    timeout,
		AuthHeader:           authHeader,
		GzipCompressionLevel: cfg.GzipCompression,
		TLS:                  tlsConfig,
		KeepAlive:            cfg.KeepAlive,
	})
	if err != nil {
		return nil, fmt.Errorf("can't create http client: %w", err)
	}

	pingTimeout := pingTimeoutLimit
	if timeout < pingTimeout {
		pingTimeout = timeout
	}

	statusCode, err := pingClient.DoTimeout("", http.MethodGet, "", nil, pingTimeout, nil)
	if err != nil {
		return nil, fmt.Errorf("%s ping failed: %w", cfg.Name, err)
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("%s ping fail-ededd with status: %d", cfg.Name, statusCode)
	}

	slog.Debug(fmt.Sprintf("%s connection established successfully", cfg.Name))

	client := &Client{name: cfg.Name, baseURL: baseURL, pingClient: pingClient, index: cfg.Index, timeout: timeout, monitoring: monitoring}

	if cfg.GRPC != nil && cfg.GRPC.Host != "" && cfg.GRPC.Port > 0 {
		addr := net.JoinHostPort(cfg.GRPC.Host, strconv.Itoa(cfg.GRPC.Port))
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("can't connect to %s gRPC at %s: %w", cfg.Name, addr, err)
		}
		client.grpcConn = conn
		client.docClient = opensearchpb.NewDocumentServiceClient(conn)
		slog.Debug(fmt.Sprintf("%s gRPC client connected", cfg.Name), slog.String("addr", addr))
		return client, nil
	}

	bulkEndpoint := baseURL + "/_bulk"
	if cfg.Index != "" {
		bulkEndpoint = baseURL + "/" + cfg.Index + "/_bulk"
	}
	bulkClient, err := http_client.NewClient(&http_client.ClientConfig{
		Endpoint:             bulkEndpoint,
		ConnectionTimeout:    timeout,
		AuthHeader:           authHeader,
		GzipCompressionLevel: cfg.GzipCompression,
		TLS:                  tlsConfig,
		KeepAlive:            cfg.KeepAlive,
	})
	if err != nil {
		return nil, fmt.Errorf("can't create bulk http client: %w", err)
	}
	client.httpClient = bulkClient
	return client, nil
}

// Close closes the gRPC connection if the client was created with gRPC. No-op for HTTP-only client.
func (c *Client) Close() error {
	if c.grpcConn != nil {
		return c.grpcConn.Close()
	}
	return nil
}

// Bulk executes a bulk operation using either gRPC (if configured) or HTTP.
// It accepts a bulk_transformer.DataPayload and returns an error only.
func (c *Client) Bulk(ctx context.Context, data bulk_transformer.DataPayload) error {
	if c.docClient != nil {
		return c.bulkGRPC(ctx, data)
	}
	return c.bulkHTTP(ctx, data)
}

// bulkGRPC sends bulk data via the OpenSearch gRPC DocumentService.
func (c *Client) bulkGRPC(ctx context.Context, data bulk_transformer.DataPayload) error {
	req := data.ToBulkRequest(c.index)
	resp, err := c.docClient.Bulk(ctx, req)
	if err != nil {
		c.monitoring.IncSearchRequests(c.name, "fail")
		return fmt.Errorf("opensearch gRPC bulk: %w", err)
	}

	indexingErrors := reportGRPCBulkErrors(resp)
	if indexingErrors > 0 {
		c.monitoring.AddSearchErrors(c.name, float64(indexingErrors))
		c.monitoring.IncSearchRequests(c.name, "fail")
		return fmt.Errorf("opensearch gRPC bulk: %d item(s) failed to index", indexingErrors)
	}

	c.monitoring.IncSearchRequests(c.name, "success")
	return nil
}

// bulkHTTP sends bulk data via the HTTP _bulk API.
func (c *Client) bulkHTTP(ctx context.Context, data bulk_transformer.DataPayload) error {
	timeout := timeoutFromContext(ctx, c.timeout)
	var indexingErrors int
	body := data.Bytes()
	statusCode, err := c.httpClient.DoTimeout(
		"",
		http.MethodPost,
		contentTypeNDJSON,
		body,
		timeout,
		func(body []byte) error {
			indexingErrors = c.reportOSErrors(body)
			return nil
		})
	if err != nil {
		c.monitoring.IncSearchRequests(c.name, "fail")
		if statusCode >= http.StatusBadRequest {
			return fmt.Errorf("%s bulk request failed with status %d: %w", c.name, statusCode, err)
		}
		return fmt.Errorf("can't send bulk request: %w", err)
	}

	if indexingErrors > 0 {
		c.monitoring.AddSearchErrors(c.name, float64(indexingErrors))
		c.monitoring.IncSearchRequests(c.name, "fail")
		return fmt.Errorf("%s bulk: %d item(s) failed to index", c.name, indexingErrors)
	}
	c.monitoring.IncSearchRequests(c.name, "success")
	return nil
}

func timeoutFromContext(ctx context.Context, defaultTimeout time.Duration) time.Duration {
	if ctx == nil {
		return defaultTimeout
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return defaultTimeout
	}
	if remaining := time.Until(deadline); remaining > 0 {
		return remaining
	}
	return defaultTimeout
}

// reportOSErrors parses the OpenSearch bulk response and logs any indexing errors.
// It returns the number of indexing errors found.
// reportGRPCBulkErrors logs errors from gRPC BulkResponse and returns the count of failed items.
func reportGRPCBulkErrors(resp *opensearchpb.BulkResponse) int {
	if resp == nil || !resp.Errors {
		return 0
	}
	count := 0
	for _, item := range resp.Items {
		if item == nil {
			continue
		}
		var ri *opensearchpb.ResponseItem
		switch v := item.Item.(type) {
		case *opensearchpb.Item_Index:
			ri = v.Index
		case *opensearchpb.Item_Create:
			ri = v.Create
		case *opensearchpb.Item_Update:
			ri = v.Update
		case *opensearchpb.Item_Delete:
			ri = v.Delete
		}
		if ri == nil {
			continue
		}
		if ri.Error != nil || ri.Status >= int32(http.StatusBadRequest) {
			count++
			if ri.Error != nil {
				reason := ""
				if ri.Error.Reason != nil {
					reason = *ri.Error.Reason
				}
				slog.Error("opensearch gRPC indexing error",
					slog.String("type", ri.Error.Type),
					slog.String("reason", reason),
					slog.Int("status", int(ri.Status)),
				)
			} else {
				slog.Error("opensearch gRPC bulk item error",
					slog.Int("status", int(ri.Status)),
					slog.String("index", ri.XIndex),
				)
			}
		}
	}
	if count > 0 {
		slog.Error("some events from gRPC bulk aren't written, check previous logs",
			slog.Int("error_count", count),
		)
	}
	return count
}

func (c *Client) reportOSErrors(data []byte) int {
	var response struct {
		Errors bool                         `json:"errors"`
		Items  []map[string]json.RawMessage `json:"items"`
	}

	if err := json.Unmarshal(data, &response); err != nil {
		return 0
	}

	if !response.Errors {
		return 0
	}

	if len(response.Items) == 0 {
		slog.Error(fmt.Sprintf("unknown %s error, 'items' field in the response is empty", c.name),
			slog.String("response", string(data)),
		)
		return 0
	}

	indexingErrors := 0
	for _, item := range response.Items {
		// Each item can have "index", "create", "update", or "delete" key
		var actionNode json.RawMessage
		var actionType string
		for key, value := range item {
			actionType = key
			actionNode = value
			break
		}

		if actionNode == nil {
			slog.Error(fmt.Sprintf("unknown %s response, action field in the response is empty", c.name),
				slog.String("response", string(data)),
			)
			continue
		}

		var actionResult struct {
			Status int                    `json:"status"`
			Error  map[string]interface{} `json:"error,omitempty"`
		}

		if err := json.Unmarshal(actionNode, &actionResult); err != nil {
			slog.Error("failed to parse action result",
				slog.String("error", err.Error()),
				slog.String("response", string(actionNode)),
			)
			continue
		}

		if actionResult.Error != nil {
			indexingErrors++
			errorJSON, _ := json.Marshal(actionResult.Error)
			slog.Error(fmt.Sprintf("%s indexing error", c.name),
				slog.String("action", actionType),
				slog.String("error", string(errorJSON)),
			)
			continue
		}

		if actionResult.Status >= http.StatusBadRequest {
			slog.Error(fmt.Sprintf("unknown %s error", c.name),
				slog.String("action", actionType),
				slog.Int("status", actionResult.Status),
				slog.String("response", string(actionNode)),
			)
		}
	}

	if indexingErrors != 0 {
		slog.Error("some events from batch aren't written, check previous logs for more information",
			slog.Int("error_count", indexingErrors),
		)
	}

	return indexingErrors
}

// IndexExists checks whether the given index exists.
// Uses HEAD /{index} — identical API for both Elasticsearch and OpenSearch.
// Returns (true, nil) if the index exists, (false, nil) if it does not, or (false, error) on failure.
func (c *Client) IndexExists(ctx context.Context, index string) (bool, error) {
	url := c.baseURL + "/" + index
	timeout := timeoutFromContext(ctx, c.timeout)

	statusCode, err := c.pingClient.DoTimeout(url, http.MethodHead, "", nil, timeout, nil)
	if err != nil {
		return false, fmt.Errorf("%s index exists check failed: %w", c.name, err)
	}

	switch statusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("%s index exists check returned unexpected status: %d", c.name, statusCode)
	}
}

// CreateIndex creates the given index with optional settings and mappings.
// Uses PUT /{index} — identical API for both Elasticsearch and OpenSearch.
// body may be nil to create an index with default settings.
func (c *Client) CreateIndex(ctx context.Context, index string, body []byte) error {
	url := c.baseURL + "/" + index
	timeout := timeoutFromContext(ctx, c.timeout)

	contentType := ""
	if len(body) > 0 {
		contentType = "application/json"
	}

	statusCode, err := c.pingClient.DoTimeout(url, http.MethodPut, contentType, body, timeout, nil)
	if err != nil {
		return fmt.Errorf("%s create index failed: %w", c.name, err)
	}

	if statusCode < http.StatusOK || statusCode > http.StatusAccepted {
		return fmt.Errorf("%s create index failed with status: %d", c.name, statusCode)
	}

	return nil
}

func (c *Client) GrpcIsConnected() bool {
	return c.grpcConn != nil
}
