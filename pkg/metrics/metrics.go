package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	requestsTotal *prometheus.CounterVec
	errorsTotal   *prometheus.CounterVec
	bulkDuration  *prometheus.HistogramVec

	batcherBufferSize prometheus.Gauge

	replicationEventsTotal       *prometheus.CounterVec
	replicationTransactionsTotal prometheus.Counter
	replicationErrorsTotal       *prometheus.CounterVec
	replicationLagSeconds        prometheus.Gauge
}

func New() *Metrics {
	metrics := &Metrics{
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
				Help: "Current number of buffered rows in the batcher, including uncommitted ones.",
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
		metrics.requestsTotal,
		metrics.errorsTotal,
		metrics.bulkDuration,
		metrics.batcherBufferSize,
		metrics.replicationEventsTotal,
		metrics.replicationTransactionsTotal,
		metrics.replicationErrorsTotal,
		metrics.replicationLagSeconds,
	)

	return metrics
}

func (m *Metrics) IncSearchRequests(backend, status string) {
	m.requestsTotal.WithLabelValues(backend, status).Inc()
}

func (m *Metrics) AddSearchErrors(backend string, count float64) {
	if count <= 0 {
		return
	}
	m.errorsTotal.WithLabelValues(backend).Add(count)
}

func (m *Metrics) ObserveSearchBulkDuration(backend, status string, seconds float64) {
	m.bulkDuration.WithLabelValues(backend, status).Observe(seconds)
}

func (m *Metrics) SetBatcherBufferSize(n int) {
	m.batcherBufferSize.Set(float64(n))
}

func (m *Metrics) IncReplicationEvents(operation string) {
	m.replicationEventsTotal.WithLabelValues(operation).Inc()
}

func (m *Metrics) IncReplicationTransactions() {
	m.replicationTransactionsTotal.Inc()
}

func (m *Metrics) IncReplicationErrors(stage string) {
	m.replicationErrorsTotal.WithLabelValues(stage).Inc()
}

func (m *Metrics) SetReplicationLag(seconds float64) {
	m.replicationLagSeconds.Set(seconds)
}
