package http

import (
	"encoding/json"
	nethttp "net/http"
	"strconv"
	"time"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
)

type Handler struct {
	telemetryRepo usecase.TelemetryReader
	alertRepo     usecase.AlertReader
}

type healthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func NewHandler(telemetryRepo usecase.TelemetryReader, alertRepo usecase.AlertReader) *Handler {
	return &Handler{
		telemetryRepo: telemetryRepo,
		alertRepo:     alertRepo,
	}
}

func NewRouter(handler *Handler, metricsHandlers ...nethttp.Handler) nethttp.Handler {
	mux := nethttp.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health)
	mux.HandleFunc("GET /alerts", handler.Alerts)
	mux.HandleFunc("GET /alerts/{patient_id}", handler.PatientAlerts)
	mux.HandleFunc("GET /telemetry", handler.Telemetry)
	mux.HandleFunc("GET /telemetry/{patient_id}", handler.PatientTelemetry)
	if len(metricsHandlers) > 0 && metricsHandlers[0] != nil {
		mux.Handle("GET /metrics", metricsHandlers[0])
	}
	return mux
}

func (h *Handler) Health(w nethttp.ResponseWriter, r *nethttp.Request) {
	writeJSON(w, nethttp.StatusOK, healthResponse{
		Status:  "ok",
		Service: "processing",
	})
}

func (h *Handler) Alerts(w nethttp.ResponseWriter, r *nethttp.Request) {
	filter, ok := parseAlertFilter(w, r, "")
	if !ok {
		return
	}

	alerts, err := h.alertRepo.ListAlerts(r.Context(), filter)
	if err != nil {
		writeError(w, nethttp.StatusInternalServerError, "alerts_unavailable", "alerts are unavailable")
		return
	}

	writeJSON(w, nethttp.StatusOK, emptyAlertsAsArray(alerts))
}

func (h *Handler) PatientAlerts(w nethttp.ResponseWriter, r *nethttp.Request) {
	filter, ok := parseAlertFilter(w, r, r.PathValue("patient_id"))
	if !ok {
		return
	}

	alerts, err := h.alertRepo.ListAlerts(r.Context(), filter)
	if err != nil {
		writeError(w, nethttp.StatusInternalServerError, "alerts_unavailable", "alerts are unavailable")
		return
	}

	writeJSON(w, nethttp.StatusOK, emptyAlertsAsArray(alerts))
}

func (h *Handler) Telemetry(w nethttp.ResponseWriter, r *nethttp.Request) {
	filter, ok := parseTelemetryFilter(w, r, "")
	if !ok {
		return
	}

	events, err := h.telemetryRepo.ListTelemetry(r.Context(), filter)
	if err != nil {
		writeError(w, nethttp.StatusInternalServerError, "telemetry_unavailable", "telemetry is unavailable")
		return
	}

	writeJSON(w, nethttp.StatusOK, emptyTelemetryAsArray(events))
}

func (h *Handler) PatientTelemetry(w nethttp.ResponseWriter, r *nethttp.Request) {
	filter, ok := parseTelemetryFilter(w, r, r.PathValue("patient_id"))
	if !ok {
		return
	}

	events, err := h.telemetryRepo.ListTelemetry(r.Context(), filter)
	if err != nil {
		writeError(w, nethttp.StatusInternalServerError, "telemetry_unavailable", "telemetry is unavailable")
		return
	}

	writeJSON(w, nethttp.StatusOK, emptyTelemetryAsArray(events))
}

func writeError(w nethttp.ResponseWriter, statusCode int, code string, message string) {
	writeJSON(w, statusCode, errorResponse{
		Error:   code,
		Message: message,
	})
}

func writeJSON(w nethttp.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func emptyAlertsAsArray(alerts []model.Alert) []model.Alert {
	if alerts == nil {
		return []model.Alert{}
	}

	return alerts
}

func emptyTelemetryAsArray(events []model.TelemetryEvent) []model.TelemetryEvent {
	if events == nil {
		return []model.TelemetryEvent{}
	}

	return events
}

func parseAlertFilter(
	w nethttp.ResponseWriter,
	r *nethttp.Request,
	pathPatientID string,
) (usecase.AlertFilter, bool) {
	filter, ok := parseCommonFilter(w, r, pathPatientID)
	if !ok {
		return usecase.AlertFilter{}, false
	}

	return usecase.AlertFilter(filter), true
}

func parseTelemetryFilter(
	w nethttp.ResponseWriter,
	r *nethttp.Request,
	pathPatientID string,
) (usecase.TelemetryFilter, bool) {
	filter, ok := parseCommonFilter(w, r, pathPatientID)
	if !ok {
		return usecase.TelemetryFilter{}, false
	}

	return usecase.TelemetryFilter(filter), true
}

type commonFilter struct {
	PatientID string
	From      *time.Time
	To        *time.Time
	Limit     int
}

func parseCommonFilter(
	w nethttp.ResponseWriter,
	r *nethttp.Request,
	pathPatientID string,
) (commonFilter, bool) {
	query := r.URL.Query()
	patientID := pathPatientID
	if patientID == "" {
		patientID = query.Get("patient_id")
	}

	from, ok := parseOptionalTime(w, query.Get("from"), "from")
	if !ok {
		return commonFilter{}, false
	}
	to, ok := parseOptionalTime(w, query.Get("to"), "to")
	if !ok {
		return commonFilter{}, false
	}
	limit, ok := parseOptionalLimit(w, query.Get("limit"))
	if !ok {
		return commonFilter{}, false
	}

	return commonFilter{
		PatientID: patientID,
		From:      from,
		To:        to,
		Limit:     limit,
	}, true
}

func parseOptionalTime(
	w nethttp.ResponseWriter,
	value string,
	name string,
) (*time.Time, bool) {
	if value == "" {
		return nil, true
	}

	timestamp, err := time.Parse(time.RFC3339, value)
	if err != nil {
		writeError(w, nethttp.StatusBadRequest, "invalid_query", name+" must be RFC3339")
		return nil, false
	}
	timestamp = timestamp.UTC()

	return &timestamp, true
}

func parseOptionalLimit(w nethttp.ResponseWriter, value string) (int, bool) {
	if value == "" {
		return 0, true
	}

	limit, err := strconv.Atoi(value)
	if err != nil || limit < 0 {
		writeError(w, nethttp.StatusBadRequest, "invalid_query", "limit must be a non-negative integer")
		return 0, false
	}

	return limit, true
}
