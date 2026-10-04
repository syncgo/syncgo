package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()

	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("failed to read counter: %v", err)
	}

	return m.GetCounter().GetValue()
}

func TestMetrics_FlushCounters(t *testing.T) {
	m := New()

	if got := counterValue(t, m.flushRetriesTotal); got != 0 {
		t.Fatalf("expected 0 retries initially, got %v", got)
	}
	if got := counterValue(t, m.flushFailuresTotal); got != 0 {
		t.Fatalf("expected 0 failures initially, got %v", got)
	}

	m.IncFlushRetry()
	m.IncFlushRetry()
	m.IncFlushFailure()

	if got := counterValue(t, m.flushRetriesTotal); got != 2 {
		t.Fatalf("expected 2 retries, got %v", got)
	}
	if got := counterValue(t, m.flushFailuresTotal); got != 1 {
		t.Fatalf("expected 1 failure, got %v", got)
	}
}
