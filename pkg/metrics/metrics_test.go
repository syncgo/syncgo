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

func TestMetrics_BulkRetryCounters(t *testing.T) {
	m := DefaultMetrics
	retriesBefore := counterValue(t, m.bulkRetriesTotal)
	exhaustedBefore := counterValue(t, m.bulkExhaustedTotal)

	m.IncBulkRetry()
	m.IncBulkRetry()
	m.IncBulkExhausted()

	if got := counterValue(t, m.bulkRetriesTotal) - retriesBefore; got != 2 {
		t.Fatalf("expected 2 retries, got %v", got)
	}
	if got := counterValue(t, m.bulkExhaustedTotal) - exhaustedBefore; got != 1 {
		t.Fatalf("expected 1 exhausted, got %v", got)
	}
}
