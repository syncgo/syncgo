//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"

	"github.com/syncgo/opensearchpb"
	"github.com/syncgo/syncgo/internal/bulk_transformer"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/search_engine_client"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

// fakeDocumentService bridges the gRPC DocumentService to OpenSearch's real
// HTTP _bulk API. OpenSearch has no native gRPC endpoint, so this is what
// lets TestOpensearchClientGRPC_* exercise the client's actual bulkGRPC code
// path (protobuf request, gRPC transport, response parsing) end to end
// against a real OpenSearch, instead of only checking the client connects.
type fakeDocumentService struct {
	opensearchpb.UnimplementedDocumentServiceServer

	httpAddr string
}

func startFakeDocumentService(t *testing.T, httpAddr string) *search_engine_client.GRPCConfig {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to listen; error: ", err)
	}

	srv := grpc.NewServer()
	opensearchpb.RegisterDocumentServiceServer(srv, &fakeDocumentService{httpAddr: httpAddr})

	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	addr := lis.Addr().(*net.TCPAddr)
	return &search_engine_client.GRPCConfig{Host: addr.IP.String(), Port: addr.Port}
}

func (s *fakeDocumentService) Bulk(ctx context.Context, req *opensearchpb.BulkRequest) (*opensearchpb.BulkResponse, error) {
	url := s.httpAddr + "/_bulk"
	if req.GetIndex() != "" {
		url = s.httpAddr + "/" + req.GetIndex() + "/_bulk"
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bulkRequestToNDJSON(req)))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var osResp struct {
		Errors bool                         `json:"errors"`
		Items  []map[string]json.RawMessage `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&osResp); err != nil {
		return nil, err
	}

	bulkResp := &opensearchpb.BulkResponse{Errors: osResp.Errors}
	for _, item := range osResp.Items {
		for _, raw := range item {
			var result struct {
				Index  string `json:"_index"`
				ID     string `json:"_id"`
				Status int32  `json:"status"`
				Error  *struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"error"`
			}
			if err := json.Unmarshal(raw, &result); err != nil {
				return nil, err
			}

			ri := &opensearchpb.ResponseItem{XIndex: result.Index, Status: result.Status, XId: &result.ID}
			if result.Error != nil {
				ri.Error = &opensearchpb.ErrorCause{Type: result.Error.Type, Reason: &result.Error.Reason}
			}
			// The client only inspects the wrapped ResponseItem's Error/Status
			// (see reportGRPCBulkErrors), not which oneof case it arrived in,
			// so any wrapper works regardless of the actual action.
			bulkResp.Items = append(bulkResp.Items, &opensearchpb.Item{Item: &opensearchpb.Item_Index{Index: ri}})
		}
	}

	return bulkResp, nil
}

// bulkRequestToNDJSON mirrors bulk_transformer.DataPayload.Bytes(), translating
// a protobuf BulkRequest into the NDJSON body OpenSearch's HTTP _bulk API expects.
func bulkRequestToNDJSON(req *opensearchpb.BulkRequest) []byte {
	var buf bytes.Buffer

	for _, body := range req.GetBulkRequestBody() {
		oc := body.GetOperationContainer()

		switch {
		case oc.GetIndex() != nil:
			op := oc.GetIndex()
			writeBulkMeta(&buf, "index", op.GetXId(), op.GetXIndex())
			buf.Write(body.GetObject())
			buf.WriteByte('\n')
		case oc.GetCreate() != nil:
			op := oc.GetCreate()
			writeBulkMeta(&buf, "create", op.GetXId(), op.GetXIndex())
			buf.Write(body.GetObject())
			buf.WriteByte('\n')
		case oc.GetUpdate() != nil:
			op := oc.GetUpdate()
			writeBulkMeta(&buf, "update", op.GetXId(), op.GetXIndex())
			buf.WriteString(`{"doc":`)
			buf.Write(body.GetObject())
			buf.WriteString("}\n")
		case oc.GetDelete() != nil:
			op := oc.GetDelete()
			writeBulkMeta(&buf, "delete", op.GetXId(), op.GetXIndex())
		}
	}

	return buf.Bytes()
}

func writeBulkMeta(buf *bytes.Buffer, action, id, index string) {
	meta := map[string]any{}
	if id != "" {
		meta["_id"] = id
	}
	if index != "" {
		meta["_index"] = index
	}

	line, _ := json.Marshal(map[string]any{action: meta})
	buf.Write(line)
	buf.WriteByte('\n')
}

func newGRPCOpensearchClient(t *testing.T, monitoring search_engine_client.Monitoring) *search_engine_client.Client {
	t.Helper()

	cfg, err := config.LoadFromYAML("config_opensearch.yaml")
	if err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to load config; error: ", err)
	}

	grpcCfg := startFakeDocumentService(t, cfg.SearchEngine.Address)

	client, err := search_engine_client.New(context.Background(), search_engine_client.Config{
		Name:              cfg.SearchEngine.Name,
		Address:           cfg.SearchEngine.Address,
		Username:          cfg.SearchEngine.Username,
		Password:          cfg.SearchEngine.Password,
		Index:             cfg.SearchEngine.Index,
		ConnectionTimeout: cfg.SearchEngine.ConnectionTimeout,
		GzipCompression:   cfg.SearchEngine.GzipCompression,
		GRPC:              grpcCfg,
	}, monitoring)
	if err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to init opensearch client; error: ", err)
	}
	if !client.GrpcIsConnected() {
		t.Fatal("e2e; opensearch; grpc; expected client to be configured for gRPC")
	}

	return client
}

func TestOpensearchClientGRPC_Create(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())
	client := newGRPCOpensearchClient(t, monitoring)

	id := uuid.New().String()
	payload := bulk_transformer.DataPayload{
		{ID: id, Action: bulk_transformer.Create, Body: []byte(`{"title":"Prisoners 2","year":2013}`)},
	}

	if err := client.Bulk(ctx, payload); err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to create document through bulk api; error: ", err)
	}

	exists, err := client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to check document existence; error: ", err)
	}
	if !exists {
		t.Fatal("e2e; opensearch; grpc; expected document to exist after create")
	}
}

func TestOpensearchClientGRPC_Index(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())
	client := newGRPCOpensearchClient(t, monitoring)

	id := uuid.New().String()
	createPayload := bulk_transformer.DataPayload{
		{ID: id, Action: bulk_transformer.Create, Body: []byte(`{"title":"Prisoners 2","year":2013}`)},
	}
	if err := client.Bulk(ctx, createPayload); err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to create document through bulk api; error: ", err)
	}

	indexPayload := bulk_transformer.DataPayload{
		{ID: id, Action: bulk_transformer.Index, Body: []byte(`{"title":"Rush","year":2013}`)},
	}
	if err := client.Bulk(ctx, indexPayload); err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to index document through bulk api; error: ", err)
	}

	exists, err := client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to check document existence; error: ", err)
	}
	if !exists {
		t.Fatal("e2e; opensearch; grpc; expected document to exist after index")
	}
}

func TestOpensearchClientGRPC_Update(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())
	client := newGRPCOpensearchClient(t, monitoring)

	id := uuid.New().String()
	createPayload := bulk_transformer.DataPayload{
		{ID: id, Action: bulk_transformer.Create, Body: []byte(`{"title":"Prisoners 2","year":2013}`)},
	}
	if err := client.Bulk(ctx, createPayload); err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to create document through bulk api; error: ", err)
	}

	updatePayload := bulk_transformer.DataPayload{
		{ID: id, Action: bulk_transformer.Update, Body: []byte(`{"title":"World War Z"}`)},
	}
	if err := client.Bulk(ctx, updatePayload); err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to update document through bulk api; error: ", err)
	}

	exists, err := client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to check document existence; error: ", err)
	}
	if !exists {
		t.Fatal("e2e; opensearch; grpc; expected document to exist after update")
	}
}

func TestOpensearchClientGRPC_Delete(t *testing.T) {
	ctx := context.Background()

	monitoring := metrics.New(prometheus.NewRegistry())
	client := newGRPCOpensearchClient(t, monitoring)

	id := uuid.New().String()
	createPayload := bulk_transformer.DataPayload{
		{ID: id, Action: bulk_transformer.Create, Body: []byte(`{"title":"Prisoners 2","year":2013}`)},
	}
	if err := client.Bulk(ctx, createPayload); err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to create document through bulk api; error: ", err)
	}

	deletePayload := bulk_transformer.DataPayload{
		{ID: id, Action: bulk_transformer.Delete},
	}
	if err := client.Bulk(ctx, deletePayload); err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to delete document through bulk api; error: ", err)
	}

	exists, err := client.DocumentExists(ctx, id)
	if err != nil {
		t.Fatal("e2e; opensearch; grpc; failed to check document existence; error: ", err)
	}
	if exists {
		t.Fatal("e2e; opensearch; grpc; expected document to be gone after delete")
	}
}
