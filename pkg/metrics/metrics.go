package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type searchMetrics struct {
	requestsTotal *prometheus.CounterVec
	errorsTotal   *prometheus.CounterVec
}

var search searchMetrics

func init() {
	search.requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "syncgo_search_requests_total",
			Help: "Total number of bulk requests sent to the search backend.",
		},
		[]string{"backend", "status"}, // status: "success" | "fail"
	)
	search.errorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "syncgo_search_errors_total",
			Help: "Total number of indexing errors reported by the search backend.",
		},
		[]string{"backend"},
	)
	prometheus.MustRegister(search.requestsTotal, search.errorsTotal)
}

// IncSearchRequests increments the request counter for the given backend and status.
// Status should be "success" (no errors for that request) or "fail".
func IncSearchRequests(backend, status string) {
	search.requestsTotal.WithLabelValues(backend, status).Inc()
}

// AddSearchErrors adds the given count to the errors counter for the backend.
func AddSearchErrors(backend string, count float64) {
	if count <= 0 {
		return
	}
	search.errorsTotal.WithLabelValues(backend).Add(count)
}
