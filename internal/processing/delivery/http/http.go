package http

import (
	"encoding/json"
	nethttp "net/http"

	"github.com/butorovv/bmstu-practice-2026/internal/processing/model"
	"github.com/butorovv/bmstu-practice-2026/internal/processing/usecase"
)

type Handler struct {
	alertRepo usecase.AlertRepository
}

type healthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func NewHandler(alertRepo usecase.AlertRepository) *Handler {
	return &Handler{
		alertRepo: alertRepo,
	}
}

func NewRouter(handler *Handler) nethttp.Handler {
	mux := nethttp.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health)
	mux.HandleFunc("GET /alerts", handler.Alerts)
	mux.HandleFunc("GET /alerts/{patient_id}", handler.PatientAlerts)
	return mux
}

func (h *Handler) Health(w nethttp.ResponseWriter, r *nethttp.Request) {
	writeJSON(w, nethttp.StatusOK, healthResponse{
		Status:  "ok",
		Service: "processing",
	})
}

func (h *Handler) Alerts(w nethttp.ResponseWriter, r *nethttp.Request) {
	alerts, err := h.alertRepo.GetRecentAlerts(r.Context())
	if err != nil {
		writeError(w, nethttp.StatusInternalServerError, "alerts_unavailable", "alerts are unavailable")
		return
	}

	writeJSON(w, nethttp.StatusOK, emptyAlertsAsArray(alerts))
}

func (h *Handler) PatientAlerts(w nethttp.ResponseWriter, r *nethttp.Request) {
	alerts, err := h.alertRepo.GetAlertsByPatientID(r.Context(), r.PathValue("patient_id"))
	if err != nil {
		writeError(w, nethttp.StatusInternalServerError, "alerts_unavailable", "alerts are unavailable")
		return
	}

	writeJSON(w, nethttp.StatusOK, emptyAlertsAsArray(alerts))
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
