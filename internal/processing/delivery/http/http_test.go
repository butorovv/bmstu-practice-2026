package http

import (
	"encoding/json"
	nethttp "net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthReturnsOK(t *testing.T) {
	router := NewRouter(NewHandler())

	req := httptest.NewRequest(nethttp.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != nethttp.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, nethttp.StatusOK)
	}

	var response healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "ok" {
		t.Fatalf("response status = %q, want %q", response.Status, "ok")
	}
	if response.Service != "processing" {
		t.Fatalf("response service = %q, want %q", response.Service, "processing")
	}
}

func TestRouterDoesNotExposeTelemetryPost(t *testing.T) {
	router := NewRouter(NewHandler())

	req := httptest.NewRequest(nethttp.MethodPost, "/api/v1/telemetry", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code == nethttp.StatusOK || rec.Code == nethttp.StatusAccepted {
		t.Fatalf("POST /api/v1/telemetry status = %d, want non-success", rec.Code)
	}
}
