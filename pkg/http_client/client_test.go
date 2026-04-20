package http_client

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestBuildURI(t *testing.T) {
	cases := []struct {
		name     string
		host     string
		endpoint string
		wantURI  string
	}{
		{
			name:     "empty endpoint uses host",
			host:     "https://example.com",
			endpoint: "",
			wantURI:  "https://example.com/",
		},
		{
			name:     "absolute endpoint overrides host",
			host:     "https://example.com",
			endpoint: "http://endpoint:3",
			wantURI:  "http://endpoint:3/",
		},
		{
			name:     "relative endpoint with leading slash",
			host:     "https://example.com",
			endpoint: "/v1/items",
			wantURI:  "https://example.com/v1/items",
		},
		{
			name:     "relative endpoint without leading slash",
			host:     "https://example.com",
			endpoint: "v1/items",
			wantURI:  "https://example.com/v1/items",
		},
		{
			name:     "query string preserved",
			host:     "https://example.com/base",
			endpoint: "/search?q=foo&limit=10",
			wantURI:  "https://example.com/base/search?q=foo&limit=10",
		},
		{
			name:     "host with trailing slash",
			host:     "https://example.com/",
			endpoint: "/v1/items",
			wantURI:  "https://example.com/v1/items",
		},
		{
			name:     "endpoint with spaces trimmed",
			host:     "https://example.com",
			endpoint: "   /v1/items   ",
			wantURI:  "https://example.com/v1/items",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := &Client{host: []byte(tt.host)}
			uri, err := c.buildURI(tt.endpoint)
			require.NoError(t, err)
			require.Equal(t, tt.wantURI, uri.String())
		})
	}
}

func TestPrepareRequest(t *testing.T) {
	type inputData struct {
		host                 []byte
		endpoint             string
		method               string
		contentType          string
		body                 string
		authHeader           string
		customHeaders        map[string]string
		gzipCompressionLevel int
	}

	type wantData struct {
		method          []byte
		contentType     []byte
		contentEncoding []byte
		body            []byte
		auth            []byte
	}

	cases := []struct {
		name string
		in   inputData
		want wantData
	}{
		{
			name: "simple",
			in: inputData{
				host:                 []byte("https://example.com"),
				endpoint:             "/1",
				method:               fasthttp.MethodPost,
				contentType:          "application/json",
				body:                 "test simple",
				gzipCompressionLevel: -1,
			},
			want: wantData{
				method:      []byte(fasthttp.MethodPost),
				contentType: []byte("application/json"),
				body:        []byte("test simple"),
			},
		},
		{
			name: "auth header",
			in: inputData{
				host:                 []byte("https://example.com"),
				endpoint:             "/1",
				method:               fasthttp.MethodPost,
				contentType:          "application/json",
				body:                 "test auth",
				authHeader:           "Auth Header",
				gzipCompressionLevel: -1,
			},
			want: wantData{
				method:      []byte(fasthttp.MethodPost),
				contentType: []byte("application/json"),
				body:        []byte("test auth"),
				auth:        []byte("Auth Header"),
			},
		},
		{
			name: "custom headers",
			in: inputData{
				host:        []byte("https://example.com"),
				endpoint:    "/1",
				method:      fasthttp.MethodPost,
				contentType: "application/json",
				body:        "test auth",
				authHeader:  "Auth Header",
				customHeaders: map[string]string{
					"header": "value",
				},
				gzipCompressionLevel: -1,
			},
			want: wantData{
				method:      []byte(fasthttp.MethodPost),
				contentType: []byte("application/json"),
				body:        []byte("test auth"),
				auth:        []byte("Auth Header"),
			},
		},
		{
			name: "gzip",
			in: inputData{
				host:                 []byte("http://endpoint:4"),
				endpoint:             "",
				method:               fasthttp.MethodPost,
				contentType:          "application/json",
				body:                 "test gzip",
				gzipCompressionLevel: 1,
			},
			want: wantData{
				method:          []byte(fasthttp.MethodPost),
				contentType:     []byte("application/json"),
				contentEncoding: []byte(gzipContentEncoding),
				body:            []byte("test gzip"),
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := &Client{
				host:                 tt.in.host,
				authHeader:           tt.in.authHeader,
				gzipCompressionLevel: tt.in.gzipCompressionLevel,
				customHeaders:        tt.in.customHeaders,
			}

			req := fasthttp.AcquireRequest()
			defer fasthttp.ReleaseRequest(req)

			uri, err := c.buildURI(tt.in.endpoint)
			require.NoError(t, err)

			c.prepareRequest(req, uri, tt.in.method, tt.in.contentType, []byte(tt.in.body))

			require.Equal(t, tt.want.method, req.Header.Method(), "wrong method")
			require.Equal(t, tt.want.contentType, req.Header.ContentType(), "wrong content type")
			require.Equal(t, tt.want.contentEncoding, req.Header.ContentEncoding(), "wrong content encoding")
			require.Equal(t, tt.want.auth, req.Header.Peek(fasthttp.HeaderAuthorization), "wrong auth")

			for header, value := range c.customHeaders {
				require.Equal(t, []byte(value), req.Header.Peek(header), "wrong custom header")
			}

			var body []byte
			if tt.in.gzipCompressionLevel != -1 {
				body, _ = req.BodyUncompressed()
			} else {
				body = req.Body()
			}
			require.Equal(t, tt.want.body, body, "wrong body")
		})
	}
}
