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
	"github.com/romanchechyotkin/syncgo/pkg/metrics"
)

const (
	defaultConnTimeout = 30 * time.Second
	pingTimeoutLimit   = 10 * time.Second
	contentTypeNDJSON  = "application/x-ndjson"
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
	pingClient *http_client.Client
	bulkClient *http_client.Client
	timeout    time.Duration
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

	statusCode, err := pingClient.DoTimeout(http.MethodGet, "", nil, pingTimeout, nil)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch ping failed: %w", err)
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("elasticsearch ping failed with status: %d", statusCode)
	}

	slog.Debug("elasticsearch connection established successfully")

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

	return &Client{
		pingClient: pingClient,
		bulkClient: bulkClient,
		timeout:    timeout,
	}, nil
}

const backendName = "elasticsearch"

func (c *Client) Bulk(ctx context.Context, data []byte) error {
	timeout := timeoutFromContext(ctx, c.timeout)
	var indexingErrors int
	statusCode, err := c.bulkClient.DoTimeout(
		http.MethodPost,
		contentTypeNDJSON,
		data,
		timeout,
		func(body []byte) error {
			indexingErrors = c.reportESErrors(body)
			return nil
		},
	)
	status := "success"
	if err != nil || indexingErrors > 0 {
		status = "fail"
	}
	metrics.IncSearchRequests(backendName, status)
	if indexingErrors > 0 {
		metrics.AddSearchErrors(backendName, float64(indexingErrors))
	}
	if err != nil {
		if statusCode >= http.StatusBadRequest {
			return fmt.Errorf("elasticsearch bulk request failed with status %d: %w", statusCode, err)
		}
		return fmt.Errorf("can't send bulk request: %w", err)
	}
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
// reportESErrors parses the bulk response, logs indexing errors, and returns their count.
func (c *Client) reportESErrors(data []byte) int {
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
		slog.Error("unknown elasticsearch error, 'items' field in the response is empty",
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

	return indexingErrors
}
