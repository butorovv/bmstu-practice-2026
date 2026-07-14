package delivery

import (
	"log/slog"
	"net/http"
	"time"
)

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func observeHTTP(next http.Handler, logger *slog.Logger, metrics *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		wrapped := &responseWriter{ResponseWriter: w}

		next.ServeHTTP(wrapped, r)

		status := wrapped.status
		if status == 0 {
			status = http.StatusOK
		}
		route := ingestionRoute(r)
		duration := time.Since(started)

		metrics.observeRequest(r.Method, route, status, duration)
		logger.InfoContext(
			r.Context(),
			"http request completed",
			"method", r.Method,
			"route", route,
			"status", status,
			"duration", duration,
		)
	})
}

func ingestionRoute(r *http.Request) string {
	switch r.URL.Path {
	case "/api/v1/telemetry", "/health", "/ready", "/metrics":
		return r.Method + " " + r.URL.Path
	default:
		return "unmatched"
	}
}
