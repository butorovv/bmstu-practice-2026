package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegistryExposesPrometheusText(t *testing.T) {
	registry := NewRegistry()
	registry.IncCounter("processing_kafka_messages_total", Labels{"status": "processed"})
	registry.SetGauge("processing_kafka_consumer_lag", Labels{"topic": "telemetry.raw", "partition": "0"}, 42)
	registry.ObserveHistogram("processing_processing_duration_seconds", nil, 0.12)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

	registry.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	assertContains(t, body, "# TYPE processing_kafka_messages_total counter")
	assertContains(t, body, `processing_kafka_messages_total{status="processed"} 1`)
	assertContains(t, body, `processing_kafka_consumer_lag{partition="0",topic="telemetry.raw"} 42`)
	assertContains(t, body, `processing_processing_duration_seconds_bucket{le="0.25"} 1`)
	assertContains(t, body, "processing_processing_duration_seconds_count 1")
}

func assertContains(t *testing.T, body string, want string) {
	t.Helper()

	if !strings.Contains(body, want) {
		t.Fatalf("body does not contain %q:\n%s", want, body)
	}
}
