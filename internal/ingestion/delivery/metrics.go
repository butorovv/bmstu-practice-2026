package delivery

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	httpRequests     *prometheus.CounterVec
	httpDuration     *prometheus.HistogramVec
	eventsPublished  prometheus.Counter
	publishErrors    prometheus.Counter
	backpressure     prometheus.Counter
	readinessCheckUp *prometheus.GaugeVec
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ingestion",
			Name:      "http_requests_total",
			Help:      "Total number of HTTP requests handled by the ingestion service.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "ingestion",
			Name:      "http_request_duration_seconds",
			Help:      "Duration of HTTP requests handled by the ingestion service.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route"}),
		eventsPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ingestion",
			Name:      "telemetry_events_published_total",
			Help:      "Total number of telemetry events successfully published.",
		}),
		publishErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ingestion",
			Name:      "telemetry_publish_errors_total",
			Help:      "Total number of telemetry event publication failures.",
		}),
		backpressure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ingestion",
			Name:      "publisher_backpressure_total",
			Help:      "Total number of telemetry batches rejected due to publisher backpressure.",
		}),
		readinessCheckUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "ingestion",
			Name:      "readiness_check_up",
			Help:      "Whether an ingestion dependency passed its latest readiness check.",
		}, []string{"dependency"}),
	}

	registerer.MustRegister(
		metrics.httpRequests,
		metrics.httpDuration,
		metrics.eventsPublished,
		metrics.publishErrors,
		metrics.backpressure,
		metrics.readinessCheckUp,
	)

	return metrics
}

func MetricsHandler(gatherer prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
}

func (m *Metrics) observeRequest(method string, route string, status int, duration time.Duration) {
	if m == nil {
		return
	}

	m.httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(duration.Seconds())
}

func (m *Metrics) observePublishedEvent() {
	if m != nil {
		m.eventsPublished.Inc()
	}
}

func (m *Metrics) observePublishError() {
	if m != nil {
		m.publishErrors.Inc()
	}
}

func (m *Metrics) observeBackpressure() {
	if m != nil {
		m.backpressure.Inc()
	}
}

func (m *Metrics) setReadiness(dependency string, ready bool) {
	if m == nil {
		return
	}

	value := 0.0
	if ready {
		value = 1
	}
	m.readinessCheckUp.WithLabelValues(dependency).Set(value)
}
