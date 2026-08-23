package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type SearchMetrics struct {
	requestsTotal *prometheus.CounterVec
	errorsTotal   *prometheus.CounterVec
	bulkDuration  *prometheus.HistogramVec

	batcherBufferSize    prometheus.Gauge
	batcherFlushedItems  *prometheus.CounterVec
	batcherFlushDuration prometheus.Histogram

	replicationEventsTotal       *prometheus.CounterVec
	replicationTransactionsTotal prometheus.Counter
	replicationErrorsTotal       *prometheus.CounterVec
	replicationLagSeconds        prometheus.Gauge
}

func New() *SearchMetrics {
	searchMetrics := &SearchMetrics{
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "syncgo_search_requests_total",
				Help: "Total number of bulk requests sent to the search backend.",
			},
			[]string{"backend", "status"},
		),
		errorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "syncgo_search_errors_total",
				Help: "Total number of indexing errors reported by the search backend.",
			},
			[]string{"backend"},
		),
		bulkDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "syncgo_search_bulk_duration_seconds",
				Help:    "Duration of bulk requests sent to the search backend.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"backend", "status"},
		),

		batcherBufferSize: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "syncgo_batcher_buffer_size",
				Help: "Current number of rows buffered by the batcher, including uncommitted ones.",
			},
		),
		batcherFlushedItems: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "syncgo_batcher_flushed_items_total",
				Help: "Total number of committed rows flushed from the batcher to the search backend.",
			},
			[]string{"result"},
		),
		batcherFlushDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "syncgo_batcher_flush_duration_seconds",
				Help:    "Duration of a batcher flush, including the downstream bulk send.",
				Buckets: prometheus.DefBuckets,
			},
		),

		replicationEventsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "syncgo_replication_events_total",
				Help: "Total number of change events decoded from the PostgreSQL logical replication stream.",
			},
			[]string{"operation"},
		),
		replicationTransactionsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "syncgo_replication_transactions_total",
				Help: "Total number of committed transactions processed from the replication stream.",
			},
		),
		replicationErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "syncgo_replication_errors_total",
				Help: "Total number of errors encountered while decoding or processing replication messages.",
			},
			[]string{"stage"},
		),
		replicationLagSeconds: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "syncgo_replication_lag_seconds",
				Help: "Replication lag in seconds, computed as the time since the last WAL message reported by PostgreSQL.",
			},
		),
	}

	prometheus.MustRegister(
		searchMetrics.requestsTotal,
		searchMetrics.errorsTotal,
		searchMetrics.bulkDuration,
		searchMetrics.batcherBufferSize,
		searchMetrics.batcherFlushedItems,
		searchMetrics.batcherFlushDuration,
		searchMetrics.replicationEventsTotal,
		searchMetrics.replicationTransactionsTotal,
		searchMetrics.replicationErrorsTotal,
		searchMetrics.replicationLagSeconds,
	)

	return searchMetrics
}

func (m *SearchMetrics) IncSearchRequests(backend, status string) {
	m.requestsTotal.WithLabelValues(backend, status).Inc()
}

func (m *SearchMetrics) AddSearchErrors(backend string, count float64) {
	if count <= 0 {
		return
	}
	m.errorsTotal.WithLabelValues(backend).Add(count)
}

func (m *SearchMetrics) ObserveSearchBulkDuration(backend, status string, seconds float64) {
	m.bulkDuration.WithLabelValues(backend, status).Observe(seconds)
}

func (m *SearchMetrics) SetBatcherBufferSize(n int) {
	m.batcherBufferSize.Set(float64(n))
}

func (m *SearchMetrics) AddBatcherFlushedItems(result string, count float64) {
	if count <= 0 {
		return
	}
	m.batcherFlushedItems.WithLabelValues(result).Add(count)
}

func (m *SearchMetrics) ObserveBatcherFlushDuration(seconds float64) {
	m.batcherFlushDuration.Observe(seconds)
}

func (m *SearchMetrics) IncReplicationEvents(operation string) {
	m.replicationEventsTotal.WithLabelValues(operation).Inc()
}

func (m *SearchMetrics) IncReplicationTransactions() {
	m.replicationTransactionsTotal.Inc()
}

func (m *SearchMetrics) IncReplicationErrors(stage string) {
	m.replicationErrorsTotal.WithLabelValues(stage).Inc()
}

func (m *SearchMetrics) SetReplicationLag(seconds float64) {
	m.replicationLagSeconds.Set(seconds)
}
