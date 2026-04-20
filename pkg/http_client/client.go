package http_client

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/romanchechyotkin/syncgo/pkg/gzip"
	"github.com/romanchechyotkin/syncgo/pkg/tls"

	"github.com/valyala/fasthttp"
)

const gzipContentEncoding = "gzip"

type ClientTLSConfig struct {
	CACert             string
	InsecureSkipVerify bool
}

type ClientKeepAliveConfig struct {
	MaxConnDuration     time.Duration
	MaxIdleConnDuration time.Duration
}

type ClientConfig struct {
	Host                 string
	ConnectionTimeout    time.Duration
	AuthHeader           string
	CustomHeaders        map[string]string
	GzipCompressionLevel gzip.GzipCompressionLevel
	TLS                  *ClientTLSConfig
	KeepAlive            *ClientKeepAliveConfig
}

type Client struct {
	client               *fasthttp.Client
	host                 []byte
	authHeader           string
	customHeaders        map[string]string
	gzipCompressionLevel int
}

func NewClient(cfg ClientConfig) (*Client, error) {
	client := &fasthttp.Client{
		ReadTimeout:  cfg.ConnectionTimeout,
		WriteTimeout: cfg.ConnectionTimeout,
	}

	if cfg.KeepAlive != nil {
		client.MaxConnDuration = cfg.KeepAlive.MaxConnDuration
		client.MaxIdleConnDuration = cfg.KeepAlive.MaxIdleConnDuration
	}

	if cfg.TLS != nil {
		b := tls.NewConfigBuilder()
		if cfg.TLS.CACert != "" {
			err := b.AppendCARoot(cfg.TLS.CACert)
			if err != nil {
				return nil, fmt.Errorf("can't append CA root: %w", err)
			}
		}
		b.SetSkipVerify(cfg.TLS.InsecureSkipVerify)

		client.TLSConfig = b.Build()
	}

	return &Client{
		client:               client,
		host:                 []byte(cfg.Host),
		authHeader:           cfg.AuthHeader,
		customHeaders:        cfg.CustomHeaders,
		gzipCompressionLevel: gzip.ParseGzipCompressionLevel(cfg.GzipCompressionLevel),
	}, nil
}

// DoTimeout sends a request using the client's auth and TLS settings.
// If endpoint is empty, the request is sent to the client's configured host.
// If endpoint is an absolute URL (starts with http:// or https://), it is used as-is.
// Otherwise endpoint is treated as a path (and optional query) relative to the configured host.
// The caller is responsible for inspecting the returned status code.
func (c *Client) DoTimeout(
	endpoint, method, contentType string,
	body []byte,
	timeout time.Duration,
	processResponse func([]byte) error,
) (int, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	reqURI, err := c.buildURI(endpoint)
	if err != nil {
		return 0, err
	}

	c.prepareRequest(req, reqURI, method, contentType, body)

	if err := c.client.DoTimeout(req, resp, timeout); err != nil {
		return 0, fmt.Errorf("can't send request to %s: %w", reqURI.String(), err)
	}

	statusCode := resp.Header.StatusCode()

	if processResponse != nil {
		if err := processResponse(resp.Body()); err != nil {
			return statusCode, err
		}
	}

	return statusCode, nil
}

func (c *Client) buildURI(endpoint string) (*fasthttp.URI, error) {
	uri := &fasthttp.URI{}

	trimmed := strings.TrimSpace(endpoint)
	if trimmed == "" {
		if err := uri.Parse(nil, c.host); err != nil {
			return nil, fmt.Errorf("can't parse host %s: %w", string(c.host), err)
		}
		return uri, nil
	}

	// Absolute URL overrides configured host.
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		if err := uri.Parse(nil, []byte(trimmed)); err != nil {
			return nil, fmt.Errorf("can't parse URL %s: %w", trimmed, err)
		}
		return uri, nil
	}

	// Relative path/query appended to configured host (which may include scheme and base path).
	if err := uri.Parse(nil, c.host); err != nil {
		return nil, fmt.Errorf("can't parse host %s: %w", string(c.host), err)
	}

	rel := trimmed
	// Treat endpoint as relative to base path even if it starts with '/'.
	rel = strings.TrimPrefix(rel, "/")

	endpointPath := rel
	query := ""
	if idx := strings.IndexByte(rel, '?'); idx >= 0 {
		endpointPath = rel[:idx]
		if idx+1 < len(rel) {
			query = rel[idx+1:]
		}
	}

	basePath := string(uri.Path())
	joinedPath := path.Join(basePath, endpointPath)
	if joinedPath == "" {
		joinedPath = "/"
	}
	uri.SetPath(joinedPath)

	if query != "" {
		uri.SetQueryString(query)
	} else {
		uri.SetQueryString("")
	}

	return uri, nil
}

func (c *Client) prepareRequest(req *fasthttp.Request, uri *fasthttp.URI, method, contentType string, body []byte) {
	req.SetURI(uri)
	req.Header.SetMethod(method)

	if contentType != "" {
		req.Header.SetContentType(contentType)
	}
	if c.authHeader != "" {
		req.Header.Set(fasthttp.HeaderAuthorization, c.authHeader)
	}

	for header, value := range c.customHeaders {
		req.Header.Set(header, value)
	}

	if c.gzipCompressionLevel != -1 {
		if _, err := fasthttp.WriteGzipLevel(req.BodyWriter(), body, c.gzipCompressionLevel); err != nil {
			req.SetBodyRaw(body)
		} else {
			req.Header.SetContentEncoding(gzipContentEncoding)
		}
	} else {
		req.SetBodyRaw(body)
	}
}
