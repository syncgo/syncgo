package elasticsearch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/romanchechyotkin/syncgo/pkg/gzip"
	"github.com/romanchechyotkin/syncgo/pkg/http_client"
)

type Config struct {
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
}

type Client struct {
	baseURL    string
	index      string
	authHeader string
	timeout    time.Duration
	tlsConfig  *http_client.ClientTLSConfig
	keepAlive  *http_client.ClientKeepAliveConfig
	gzipLevel  gzip.GzipCompressionLevel
}

func New(ctx context.Context, cfg Config) (*Client, error) {
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

	// Set default timeout if not provided
	timeout := cfg.ConnectionTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	// Create http client for ping
	httpClient, err := http_client.NewClient(&http_client.ClientConfig{
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

	// Ping to verify connection
	pingTimeout := 10 * time.Second
	if timeout < pingTimeout {
		pingTimeout = timeout
	}

	statusCode, err := httpClient.DoTimeout(
		"GET",
		"",
		nil,
		pingTimeout,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch ping failed: %w", err)
	}

	if statusCode < http.StatusOK || statusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("elasticsearch ping failed with status: %d", statusCode)
	}

	slog.Debug("elasticsearch connection established successfully")

	return &Client{
		baseURL:    baseURL,
		index:      cfg.Index,
		authHeader: authHeader,
		timeout:    timeout,
		tlsConfig:  tlsConfig,
		keepAlive:  cfg.KeepAlive,
		gzipLevel:  cfg.GzipCompression,
	}, nil
}

func (c *Client) Bulk(ctx context.Context, data []byte) error {
	// Build bulk endpoint
	endpoint := c.baseURL + "/_bulk"
	if c.index != "" {
		endpoint = c.baseURL + "/" + c.index + "/_bulk"
	}

	// Create http client for bulk request
	httpClient, err := http_client.NewClient(&http_client.ClientConfig{
		Endpoint:             endpoint,
		ConnectionTimeout:    c.timeout,
		AuthHeader:           c.authHeader,
		GzipCompressionLevel: c.gzipLevel,
		TLS:                  c.tlsConfig,
		KeepAlive:            c.keepAlive,
	})
	if err != nil {
		return fmt.Errorf("can't create bulk http client: %w", err)
	}

	// Determine timeout from context
	timeout := c.timeout
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			timeout = time.Until(deadline)
			if timeout <= 0 {
				timeout = c.timeout
			}
		}
	}

	// Send bulk request
	statusCode, err := httpClient.DoTimeout(
		"POST",
		"application/x-ndjson",
		data,
		timeout,
		func(responseBody []byte) error {
			return c.reportESErrors(responseBody)
		},
	)
	if err != nil {
		// Check if it's a status code error
		if statusCode >= http.StatusBadRequest {
			return fmt.Errorf("elasticsearch bulk request failed with status %d: %w", statusCode, err)
		}
		return fmt.Errorf("can't send bulk request: %w", err)
	}

	return nil
}

// reportESErrors parses the Elasticsearch bulk response and logs any indexing errors.
// Example of an ElasticSearch response that returned an indexing error:
//
//	{
//	 "took": 5,
//	 "errors": true,
//	 "items": [
//	   {
//	     "index": {
//	       "_index": "logs",
//	       "_type": "_doc",
//	       "_id": "x8YzWowBaqwP8avfpXh8",
//	       "status": 400,
//	       "error": {
//	         "type": "mapper_parsing_exception",
//	         "reason": "failed to parse field [hello] of type [text] in document with id 'x8YzWowBaqwP8avfpXh8'. Preview of field's value: '{test=test}'",
//	         "caused_by": {
//	           "type": "illegal_state_exception",
//	           "reason": "Can't get text on a START_OBJECT at 1:11"
//	         }
//	       }
//	     }
//	   },
//	   {
//	     "index": {
//	       "_index": "logs",
//	       "_type": "_doc",
//	       "_id": "yMYzWowBaqwP8avfpXh8",
//	       "_version": 1,
//	       "result": "created",
//	       "_shards": {
//	         "total": 2,
//	         "successful": 1,
//	         "failed": 0
//	       },
//	       "_seq_no": 4,
//	       "_primary_term": 1,
//	       "status": 201
//	     }
//	   }
//	 ]
//	}
func (c *Client) reportESErrors(data []byte) error {
	var response struct {
		Errors bool                         `json:"errors"`
		Items  []map[string]json.RawMessage `json:"items"`
	}

	if err := json.Unmarshal(data, &response); err != nil {
		return fmt.Errorf("can't decode response: %w", err)
	}

	if !response.Errors {
		return nil
	}

	if len(response.Items) == 0 {
		slog.Error("unknown elasticsearch error, 'items' field in the response is empty",
			slog.String("response", string(data)),
		)
		return nil
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
			slog.Error("unknown elasticsearch response, action field in the response is empty",
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
			slog.Error("elasticsearch indexing error",
				slog.String("action", actionType),
				slog.String("error", string(errorJSON)),
			)
			continue
		}

		if actionResult.Status >= http.StatusBadRequest {
			slog.Error("unknown elasticsearch error",
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

	return nil
}
