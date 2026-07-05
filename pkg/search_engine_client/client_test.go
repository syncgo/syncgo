package search_engine_client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/romanchechyotkin/syncgo/internal/bulk_transformer"
	"github.com/romanchechyotkin/syncgo/pkg/http_client"
	"github.com/romanchechyotkin/syncgo/pkg/search_engine_client/mocks"
	"github.com/syncgo/opensearchpb"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeDocClient is a test double for opensearchpb.DocumentServiceClient.
type fakeDocClient struct {
	resp *opensearchpb.BulkResponse
	err  error
}

func (f *fakeDocClient) Bulk(_ context.Context, _ *opensearchpb.BulkRequest, _ ...grpc.CallOption) (*opensearchpb.BulkResponse, error) {
	return f.resp, f.err
}

func mustHTTPClient(t *testing.T, addr string) *http_client.Client {
	t.Helper()
	c, err := http_client.NewClient(http_client.ClientConfig{
		Host:              addr,
		ConnectionTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("http_client.NewClient(%s): %v", addr, err)
	}
	return c
}

// --- timeoutFromContext ---

func TestTimeoutFromContext(t *testing.T) {
	def := 5 * time.Second

	if got := timeoutFromContext(nil, def); got != def { //nolint:staticcheck
		t.Errorf("nil ctx: got %v, want %v", got, def)
	}

	if got := timeoutFromContext(context.Background(), def); got != def {
		t.Errorf("no deadline: got %v, want %v", got, def)
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	if got := timeoutFromContext(ctx, def); got <= 0 || got > 2*time.Second {
		t.Errorf("future deadline: got %v", got)
	}

	ctx2, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	if got := timeoutFromContext(ctx2, def); got != def {
		t.Errorf("expired deadline: got %v, want %v", got, def)
	}
}

// --- reportGRPCBulkErrors ---

func TestReportGRPCBulkErrors(t *testing.T) {
	if n := reportGRPCBulkErrors(nil); n != 0 {
		t.Errorf("nil resp: got %d", n)
	}
	if n := reportGRPCBulkErrors(&opensearchpb.BulkResponse{Errors: false}); n != 0 {
		t.Errorf("errors=false: got %d", n)
	}
	if n := reportGRPCBulkErrors(&opensearchpb.BulkResponse{Errors: true}); n != 0 {
		t.Errorf("nil items: got %d", n)
	}
	if n := reportGRPCBulkErrors(&opensearchpb.BulkResponse{
		Errors: true, Items: []*opensearchpb.Item{nil},
	}); n != 0 {
		t.Errorf("nil item in slice: got %d", n)
	}
	// item with no inner variant (zero Item)
	if n := reportGRPCBulkErrors(&opensearchpb.BulkResponse{
		Errors: true, Items: []*opensearchpb.Item{{}},
	}); n != 0 {
		t.Errorf("zero inner: got %d", n)
	}

	reason := "field type mismatch"

	// Item_Index with error + reason
	if n := reportGRPCBulkErrors(&opensearchpb.BulkResponse{
		Errors: true,
		Items: []*opensearchpb.Item{{Item: &opensearchpb.Item_Index{
			Index: &opensearchpb.ResponseItem{Error: &opensearchpb.ErrorCause{Type: "mapper_error", Reason: &reason}},
		}}},
	}); n != 1 {
		t.Errorf("index error+reason: got %d", n)
	}
	// Item_Index with error, no reason
	if n := reportGRPCBulkErrors(&opensearchpb.BulkResponse{
		Errors: true,
		Items: []*opensearchpb.Item{{Item: &opensearchpb.Item_Index{
			Index: &opensearchpb.ResponseItem{Error: &opensearchpb.ErrorCause{Type: "mapper_error"}},
		}}},
	}); n != 1 {
		t.Errorf("index error no reason: got %d", n)
	}
	// Item_Create with error
	if n := reportGRPCBulkErrors(&opensearchpb.BulkResponse{
		Errors: true,
		Items: []*opensearchpb.Item{{Item: &opensearchpb.Item_Create{
			Create: &opensearchpb.ResponseItem{Error: &opensearchpb.ErrorCause{Type: "create_error"}},
		}}},
	}); n != 1 {
		t.Errorf("create error: got %d", n)
	}
	// Item_Update with error
	if n := reportGRPCBulkErrors(&opensearchpb.BulkResponse{
		Errors: true,
		Items: []*opensearchpb.Item{{Item: &opensearchpb.Item_Update{
			Update: &opensearchpb.ResponseItem{Error: &opensearchpb.ErrorCause{Type: "update_error"}},
		}}},
	}); n != 1 {
		t.Errorf("update error: got %d", n)
	}
	// Item_Delete with error
	if n := reportGRPCBulkErrors(&opensearchpb.BulkResponse{
		Errors: true,
		Items: []*opensearchpb.Item{{Item: &opensearchpb.Item_Delete{
			Delete: &opensearchpb.ResponseItem{Error: &opensearchpb.ErrorCause{Type: "delete_error"}},
		}}},
	}); n != 1 {
		t.Errorf("delete error: got %d", n)
	}
	// status >= 400, no error field
	if n := reportGRPCBulkErrors(&opensearchpb.BulkResponse{
		Errors: true,
		Items: []*opensearchpb.Item{{Item: &opensearchpb.Item_Index{
			Index: &opensearchpb.ResponseItem{Status: http.StatusNotFound, XIndex: "my-index"},
		}}},
	}); n != 1 {
		t.Errorf("bad status no error: got %d", n)
	}
	// status < 400, no error — success item, not counted
	if n := reportGRPCBulkErrors(&opensearchpb.BulkResponse{
		Errors: true,
		Items: []*opensearchpb.Item{{Item: &opensearchpb.Item_Index{
			Index: &opensearchpb.ResponseItem{Status: http.StatusOK},
		}}},
	}); n != 0 {
		t.Errorf("ok status: got %d", n)
	}
}

// --- reportOSErrors ---

func TestReportOSErrors(t *testing.T) {
	c := &Client{name: "test"}

	if n := c.reportOSErrors([]byte("not-json")); n != 0 {
		t.Errorf("invalid json: %d", n)
	}

	noErr, _ := json.Marshal(map[string]interface{}{"errors": false})
	if n := c.reportOSErrors(noErr); n != 0 {
		t.Errorf("errors=false: %d", n)
	}

	emptyItems, _ := json.Marshal(map[string]interface{}{"errors": true, "items": []interface{}{}})
	if n := c.reportOSErrors(emptyItems); n != 0 {
		t.Errorf("empty items: %d", n)
	}

	// item is empty map {} → for-range yields no keys → actionNode stays nil
	nilAction, _ := json.Marshal(map[string]interface{}{
		"errors": true,
		"items":  []interface{}{map[string]interface{}{}},
	})
	if n := c.reportOSErrors(nilAction); n != 0 {
		t.Errorf("empty item map: %d", n)
	}

	// actionNode is JSON array → unmarshal into struct fails
	badNode, _ := json.Marshal(map[string]interface{}{
		"errors": true,
		"items":  []interface{}{map[string]interface{}{"index": []int{1, 2}}},
	})
	if n := c.reportOSErrors(badNode); n != 0 {
		t.Errorf("bad action node: %d", n)
	}

	// error field set → counted
	withError, _ := json.Marshal(map[string]interface{}{
		"errors": true,
		"items": []interface{}{map[string]interface{}{
			"index": map[string]interface{}{
				"status": 400,
				"error":  map[string]interface{}{"type": "mapper_error", "reason": "bad field"},
			},
		}},
	})
	if n := c.reportOSErrors(withError); n != 1 {
		t.Errorf("with error field: got %d", n)
	}

	// status >= 400, no error field → logged but not counted
	badStatus, _ := json.Marshal(map[string]interface{}{
		"errors": true,
		"items": []interface{}{map[string]interface{}{
			"create": map[string]interface{}{"status": 503},
		}},
	})
	if n := c.reportOSErrors(badStatus); n != 0 {
		t.Errorf("bad status no error field: got %d", n)
	}
}

// --- Close & GrpcIsConnected ---

func TestClose(t *testing.T) {
	if err := (&Client{}).Close(); err != nil {
		t.Errorf("nil grpcConn Close: %v", err)
	}

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	if err := (&Client{grpcConn: conn}).Close(); err != nil {
		t.Errorf("Close with conn: %v", err)
	}
}

func TestGrpcIsConnected(t *testing.T) {
	if (&Client{}).GrpcIsConnected() {
		t.Error("expected false for nil grpcConn")
	}

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("conn.Close: %v", err)
		}
	}()
	if !(&Client{grpcConn: conn}).GrpcIsConnected() {
		t.Error("expected true for non-nil grpcConn")
	}
}

// --- New ---

func TestNew_BasicHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(ctx, Config{Address: srv.URL, Name: "test"}, mon)
	if err != nil || c == nil {
		t.Fatalf("New: %v", err)
	}
}

func TestNew_WithUsernamePassword(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(ctx, Config{Address: srv.URL, Name: "test", Username: "user", Password: "pass"}, mon)
	if err != nil || c == nil {
		t.Fatalf("New with creds: %v", err)
	}
}

func TestNew_WithAPIKeyHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(ctx, Config{Address: srv.URL, Name: "test", APIKey: "my-key"}, mon)
	if err != nil || c == nil {
		t.Fatalf("New with API key: %v", err)
	}
}

func TestNew_WithIndex(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(ctx, Config{Address: srv.URL, Name: "test", Index: "my-index"}, mon)
	if err != nil || c == nil {
		t.Fatalf("New with index: %v", err)
	}
}

func TestNew_WithCustomTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// timeout < pingTimeoutLimit (10s) → no clamping in ping
	c, err := New(ctx, Config{Address: srv.URL, Name: "test", ConnectionTimeout: 5 * time.Second}, mon)
	if err != nil || c == nil {
		t.Fatalf("New with custom timeout: %v", err)
	}
}

func TestNew_WithTLSInsecure(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(ctx, Config{
		Address: srv.URL,
		Name:    "test",
		TLS:     &http_client.ClientTLSConfig{InsecureSkipVerify: true},
	}, mon)
	if err != nil || c == nil {
		t.Fatalf("New with TLS insecure: %v", err)
	}
}

func TestNew_WithGRPC(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(ctx, Config{
		Address: srv.URL,
		Name:    "test",
		GRPC:    &GRPCConfig{Host: "localhost", Port: 50052},
	}, mon)
	if err != nil {
		t.Fatalf("New with gRPC: %v", err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Errorf("c.Close: %v", err)
		}
	}()
	if !c.GrpcIsConnected() {
		t.Error("expected grpc connected")
	}
}

func TestNew_PingBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	if _, err := New(context.Background(), Config{Address: srv.URL, Name: "test"}, mon); err == nil {
		t.Error("expected error for 401 ping")
	}
}

func TestNew_PingNetworkError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := srv.URL
	srv.Close()

	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	if _, err := New(context.Background(), Config{
		Address:           addr,
		Name:              "test",
		ConnectionTimeout: 100 * time.Millisecond,
	}, mon); err == nil {
		t.Error("expected error for unreachable server")
	}
}

func TestNew_InvalidTLSCert(t *testing.T) {
	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	if _, err := New(context.Background(), Config{
		Address: "http://localhost:9200",
		Name:    "test",
		TLS:     &http_client.ClientTLSConfig{CACert: "/nonexistent/cert.pem"},
	}, mon); err == nil {
		t.Error("expected error for invalid TLS cert path")
	}
}

// --- PingClusterHealth ---

func TestPingClusterHealth(t *testing.T) {
	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	t.Run("success", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"status":"green"}`)); err != nil {
				t.Errorf("failed to write resp: %v", err) 
			}
		}))
		defer srv.Close()
		pingC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", baseURL: srv.URL, httpClient: pingC, timeout: 5 * time.Second, monitoring: mon}
		if err := c.PingClusterHealth(context.Background()); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// context deadline > pingTimeoutLimit (10s) → clamped to pingTimeoutLimit
	t.Run("timeout clamped to pingTimeoutLimit", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"status":"green"}`)); err != nil {
				t.Errorf("failed to write resp: %v", err) 
			}
		}))
		defer srv.Close()
		pingC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", baseURL: srv.URL, httpClient: pingC, timeout: time.Hour, monitoring: mon}
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		if err := c.PingClusterHealth(ctx); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("bad status", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte(`{"status":"green"}`)); err != nil {
				t.Errorf("failed to write resp: %v", err) 
			}
		}))
		defer srv.Close()
		pingC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", baseURL: srv.URL, httpClient: pingC, timeout: 5 * time.Second, monitoring: mon}
		if err := c.PingClusterHealth(context.Background()); err == nil {
			t.Error("expected error for 503")
		}
	})

	t.Run("network error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, err := w.Write([]byte(`{"status":"green"}`)); err != nil {
				t.Errorf("failed to write resp: %v", err) 
			}
		}))
		addr := srv.URL
		srv.Close()
		pingC := mustHTTPClient(t, addr)
		c := &Client{name: "test", baseURL: addr, httpClient: pingC, timeout: 100 * time.Millisecond, monitoring: mon}
		if err := c.PingClusterHealth(context.Background()); err == nil {
			t.Error("expected error for unreachable server")
		}
	})
}

// --- Bulk (HTTP path) ---

func TestBulk_HTTP(t *testing.T) {
	payload := bulk_transformer.DataPayload{
		{ID: "1", Action: bulk_transformer.Index, Body: []byte(`{"field":"value"}`)},
	}

	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mon := mocks.NewMockMonitoring(ctrl)
		mon.EXPECT().IncSearchRequests("test", "success")

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"errors": false})
		}))
		defer srv.Close()

		bulkC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", httpClient: bulkC, index: "test_index", timeout: 5 * time.Second, monitoring: mon}
		if err := c.Bulk(context.Background(), payload); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("indexing errors in response", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mon := mocks.NewMockMonitoring(ctrl)
		mon.EXPECT().AddSearchErrors("test", float64(1))
		mon.EXPECT().IncSearchRequests("test", "fail")

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"errors": true,
				"items": []interface{}{map[string]interface{}{
					"index": map[string]interface{}{
						"status": 400,
						"error":  map[string]interface{}{"type": "mapper_error", "reason": "bad"},
					},
				}},
			})
		}))
		defer srv.Close()

		bulkC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", httpClient: bulkC, index: "test_index", timeout: 5 * time.Second, monitoring: mon}
		if err := c.Bulk(context.Background(), payload); err == nil {
			t.Error("expected error for indexing errors")
		}
	})

	t.Run("network error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mon := mocks.NewMockMonitoring(ctrl)
		mon.EXPECT().IncSearchRequests("test", "fail")

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		addr := srv.URL
		srv.Close()

		bulkC := mustHTTPClient(t, addr)
		c := &Client{name: "test", httpClient: bulkC, index: "test_index", timeout: 100 * time.Millisecond, monitoring: mon}
		if err := c.Bulk(context.Background(), payload); err == nil {
			t.Error("expected error for network error")
		}
	})
}

// --- Bulk (gRPC path) ---

func TestBulk_GRPC(t *testing.T) {
	payload := bulk_transformer.DataPayload{
		{ID: "1", Action: bulk_transformer.Index, Body: []byte(`{"field":"value"}`)},
	}

	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mon := mocks.NewMockMonitoring(ctrl)
		mon.EXPECT().IncSearchRequests("test", "success")

		c := &Client{
			name:       "test",
			docClient:  &fakeDocClient{resp: &opensearchpb.BulkResponse{Errors: false}},
			index:      "test_index",
			timeout:    5 * time.Second,
			monitoring: mon,
		}
		if err := c.Bulk(context.Background(), payload); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("grpc error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mon := mocks.NewMockMonitoring(ctrl)
		mon.EXPECT().IncSearchRequests("test", "fail")

		c := &Client{
			name:       "test",
			docClient:  &fakeDocClient{err: errors.New("rpc error")},
			index:      "test_index",
			timeout:    5 * time.Second,
			monitoring: mon,
		}
		if err := c.Bulk(context.Background(), payload); err == nil {
			t.Error("expected error for gRPC failure")
		}
	})

	t.Run("indexing errors in response", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mon := mocks.NewMockMonitoring(ctrl)
		mon.EXPECT().AddSearchErrors("test", float64(1))
		mon.EXPECT().IncSearchRequests("test", "fail")

		reason := "field type mismatch"
		c := &Client{
			name: "test",
			docClient: &fakeDocClient{resp: &opensearchpb.BulkResponse{
				Errors: true,
				Items: []*opensearchpb.Item{{Item: &opensearchpb.Item_Index{
					Index: &opensearchpb.ResponseItem{
						Error: &opensearchpb.ErrorCause{Type: "mapper_error", Reason: &reason},
					},
				}}},
			}},
			index:      "test_index",
			timeout:    5 * time.Second,
			monitoring: mon,
		}
		if err := c.Bulk(context.Background(), payload); err == nil {
			t.Error("expected error for gRPC indexing errors")
		}
	})
}

// --- IndexExists ---

func TestIndexExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	t.Run("exists", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		pingC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", baseURL: srv.URL, httpClient: pingC, timeout: 5 * time.Second, monitoring: mon}
		exists, err := c.IndexExists(context.Background(), "my-index")
		if err != nil || !exists {
			t.Errorf("exists: got (%v, %v)", exists, err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()
		pingC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", baseURL: srv.URL, httpClient: pingC, timeout: 5 * time.Second, monitoring: mon}
		exists, err := c.IndexExists(context.Background(), "my-index")
		if err != nil || exists {
			t.Errorf("not found: got (%v, %v)", exists, err)
		}
	})

	t.Run("unexpected status", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		pingC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", baseURL: srv.URL, httpClient: pingC, timeout: 5 * time.Second, monitoring: mon}
		if _, err := c.IndexExists(context.Background(), "my-index"); err == nil {
			t.Error("expected error for 500")
		}
	})

	t.Run("network error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		addr := srv.URL
		srv.Close()
		pingC := mustHTTPClient(t, addr)
		c := &Client{name: "test", baseURL: addr, httpClient: pingC, timeout: 100 * time.Millisecond, monitoring: mon}
		if _, err := c.IndexExists(context.Background(), "my-index"); err == nil {
			t.Error("expected error for network error")
		}
	})
}

// --- CreateIndex ---

func TestCreateIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	t.Run("created 200", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		pingC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", baseURL: srv.URL, httpClient: pingC, timeout: 5 * time.Second, monitoring: mon}
		if err := c.CreateIndex(context.Background(), "my-index", nil); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("created 201", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
		}))
		defer srv.Close()
		pingC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", baseURL: srv.URL, httpClient: pingC, timeout: 5 * time.Second, monitoring: mon}
		if err := c.CreateIndex(context.Background(), "my-index", nil); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("with body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		pingC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", baseURL: srv.URL, httpClient: pingC, timeout: 5 * time.Second, monitoring: mon}
		body := []byte(`{"mappings":{"properties":{"field":{"type":"keyword"}}}}`)
		if err := c.CreateIndex(context.Background(), "my-index", body); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("bad status", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer srv.Close()
		pingC := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", baseURL: srv.URL, httpClient: pingC, timeout: 5 * time.Second, monitoring: mon}
		if err := c.CreateIndex(context.Background(), "my-index", nil); err == nil {
			t.Error("expected error for 400")
		}
	})

	t.Run("network error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		addr := srv.URL
		srv.Close()
		pingC := mustHTTPClient(t, addr)
		c := &Client{name: "test", baseURL: addr, httpClient: pingC, timeout: 100 * time.Millisecond, monitoring: mon}
		if err := c.CreateIndex(context.Background(), "my-index", nil); err == nil {
			t.Error("expected error for network error")
		}
	})
}

// --- Integration tests (require running OpenSearch/Elasticsearch) ---

func TestNew(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	osAddr := os.Getenv("OPENSEARCH_ADDRESS")
	if osAddr == "" {
		t.Skip("OPENSEARCH_ADDRESS not set, skipping integration test")
	}

	osUser := os.Getenv("OPENSEARCH_USER")
	if osUser == "" {
		osUser = "admin"
	}

	osPassword := os.Getenv("OPENSEARCH_PASSWORD")
	if osPassword == "" {
		osPassword = "Op3nS3arch!"
	}

	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid connection with username and password",
			config: Config{
				Address:  osAddr,
				Username: osUser,
				Password: osPassword,
			},
			wantErr: false,
		},
		{
			name: "valid connection without auth (if security disabled)",
			config: Config{
				Address: osAddr,
			},
			wantErr: false,
		},
		{
			name: "invalid address",
			config: Config{
				Address:  "http://invalid-host:9300",
				Username: osUser,
				Password: osPassword,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			monitoring := mocks.NewMockMonitoring(ctrl)

			client, err := New(ctx, tt.config, monitoring)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if client != nil {
				_ = client
			}
		})
	}
}

func TestNew_WithAPIKey(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	osAddr := os.Getenv("OPENSEARCH_ADDRESS")
	if osAddr == "" {
		t.Skip("OPENSEARCH_ADDRESS not set, skipping integration test")
	}

	apiKey := os.Getenv("OPENSEARCH_API_KEY")
	if apiKey == "" {
		t.Skip("OPENSEARCH_API_KEY not set, skipping API key test")
	}

	config := Config{
		Address: osAddr,
		APIKey:  apiKey,
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)

	client, err := New(ctx, config, monitoring)
	if err != nil {
		t.Fatalf("New() with API key error = %v", err)
	}
	if client == nil {
		t.Error("New() returned nil client")
	}
}

// --- DocumentExists ---

func TestDocumentExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	mon := mocks.NewMockMonitoring(ctrl)

	t.Run("200 → exists", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		hc := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", httpClient: hc, index: "my-index", timeout: 5 * time.Second, monitoring: mon}
		exists, err := c.DocumentExists(context.Background(), "doc-1")
		if err != nil || !exists {
			t.Errorf("expected (true, nil), got (%v, %v)", exists, err)
		}
	})

	t.Run("404 → not found", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()
		hc := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", httpClient: hc, index: "my-index", timeout: 5 * time.Second, monitoring: mon}
		exists, err := c.DocumentExists(context.Background(), "doc-1")
		if err != nil || exists {
			t.Errorf("expected (false, nil), got (%v, %v)", exists, err)
		}
	})

	t.Run("unexpected status → error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		hc := mustHTTPClient(t, srv.URL)
		c := &Client{name: "test", httpClient: hc, index: "my-index", timeout: 5 * time.Second, monitoring: mon}
		if _, err := c.DocumentExists(context.Background(), "doc-1"); err == nil {
			t.Error("expected error for 500")
		}
	})

	t.Run("network error → error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		addr := srv.URL
		srv.Close()
		hc := mustHTTPClient(t, addr)
		c := &Client{name: "test", httpClient: hc, index: "my-index", timeout: 100 * time.Millisecond, monitoring: mon}
		if _, err := c.DocumentExists(context.Background(), "doc-1"); err == nil {
			t.Error("expected error for network error")
		}
	})
}

func TestNew_WithCloudID(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cloudID := os.Getenv("OPENSEARCH_CLOUD_ID")
	if cloudID == "" {
		t.Skip("OPENSEARCH_CLOUD_ID not set, skipping Cloud ID test")
	}

	config := Config{
		CloudID: cloudID,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	monitoring := mocks.NewMockMonitoring(ctrl)

	client, err := New(ctx, config, monitoring)
	if err != nil {
		t.Fatalf("New() with CloudID error = %v", err)
	}
	if client == nil {
		t.Error("New() returned nil client")
	}
}
