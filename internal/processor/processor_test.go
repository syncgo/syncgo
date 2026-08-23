package processor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/syncgo/syncgo/internal/batcher"
	batchermocks "github.com/syncgo/syncgo/internal/batcher/mocks"
	"github.com/syncgo/syncgo/internal/bulk_transformer"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/search_engine_client"

	"go.uber.org/mock/gomock"
)

type testBulkSender struct {
	mu       sync.Mutex
	payloads [][]bulk_transformer.Data
}

func (s *testBulkSender) Bulk(_ context.Context, payload bulk_transformer.DataPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := make([]bulk_transformer.Data, len(payload))
	copy(cp, payload)
	s.payloads = append(s.payloads, cp)

	return nil
}

func testData(v string) bulk_transformer.Data {
	return bulk_transformer.Data{
		ID:     v,
		Action: bulk_transformer.Index,
		Body:   []byte(v),
	}
}

func TestProcessor_Shutdown_FlushesBatcher(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockSender := batchermocks.NewMockBulkSender(ctrl)
	mockSender.EXPECT().Bulk(gomock.Any(), bulk_transformer.DataPayload{testData("a")}).Return(nil)

	b := batcher.NewBatcher(context.Background(), 10, 0, mockSender)
	b.Add(testData("a"))
	b.Commit()

	p := &Processor{batcher: b}
	if err := p.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestProcessor_Shutdown_ClosesClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"green"}`))
	}))
	t.Cleanup(srv.Close)

	client, err := search_engine_client.New(context.Background(), search_engine_client.Config{
		Name:              "test",
		Address:           srv.URL,
		ConnectionTimeout: 5 * time.Second,
	}, metrics.New())
	if err != nil {
		t.Fatalf("search_engine_client.New: %v", err)
	}

	p := &Processor{client: client}
	if err := p.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestProcessor_Shutdown_MetricsDisabled(t *testing.T) {
	p := &Processor{}
	if err := p.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestProcessor_Shutdown_Idempotent(t *testing.T) {
	sender := &testBulkSender{}
	b := batcher.NewBatcher(context.Background(), 10, 0, sender)
	b.Add(testData("a"))
	b.Commit()

	p := &Processor{batcher: b}

	if err := p.Shutdown(); err != nil {
		t.Fatalf("first Shutdown() error = %v", err)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}

	sender.mu.Lock()
	callCount := len(sender.payloads)
	sender.mu.Unlock()
	if callCount != 1 {
		t.Fatalf("expected 1 bulk call after idempotent shutdown, got %d", callCount)
	}
}
