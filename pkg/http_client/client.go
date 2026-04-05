package http_client

import (
	"fmt"
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
	Endpoint             string
	ConnectionTimeout    time.Duration
	AuthHeader           string
	CustomHeaders        map[string]string
	GzipCompressionLevel gzip.GzipCompressionLevel
	TLS                  *ClientTLSConfig
	KeepAlive            *ClientKeepAliveConfig
}

type Client struct {
	client               *fasthttp.Client
	endpoint             *fasthttp.URI
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

	uri := &fasthttp.URI{}
	if err := uri.Parse(nil, []byte(cfg.Endpoint)); err != nil {
		return nil, fmt.Errorf("can't parse endpoint %s: %w", cfg.Endpoint, err)
	}

	return &Client{
		client:               client,
		endpoint:             uri,
		authHeader:           cfg.AuthHeader,
		customHeaders:        cfg.CustomHeaders,
		gzipCompressionLevel: gzip.ParseGzipCompressionLevel(cfg.GzipCompressionLevel),
	}, nil
}

// DoTimeout sends a request using the client's auth and TLS settings.
// If rawURL is empty, the request is sent to the client's configured endpoint.
// The caller is responsible for inspecting the returned status code.
func (c *Client) DoTimeout(
	rawURL, method, contentType string,
	body []byte,
	timeout time.Duration,
	processResponse func([]byte) error,
) (int, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	endpoint := c.endpoint
	if rawURL != "" {
		endpoint = &fasthttp.URI{}
		if err := endpoint.Parse(nil, []byte(rawURL)); err != nil {
			return 0, fmt.Errorf("can't parse URL %s: %w", rawURL, err)
		}
	}

	c.prepareRequest(req, endpoint, method, contentType, body)

	if err := c.client.DoTimeout(req, resp, timeout); err != nil {
		return 0, fmt.Errorf("can't send request to %s: %w", endpoint.String(), err)
	}

	statusCode := resp.Header.StatusCode()

	if processResponse != nil {
		if err := processResponse(resp.Body()); err != nil {
			return statusCode, err
		}
	}

	return statusCode, nil
}

func (c *Client) prepareRequest(req *fasthttp.Request, endpoint *fasthttp.URI, method, contentType string, body []byte) {
	req.SetURI(endpoint)
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
