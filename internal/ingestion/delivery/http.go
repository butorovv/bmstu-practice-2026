package delivery

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/publisher"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/usecase"
	"github.com/butorovv/bmstu-practice-2026/internal/ingestion/validator"
)

type Handler struct {
	publisher publisher.Publisher
	validator validator.Validator
}

type acceptedResponse struct {
	Status               string `json:"status"`
	AcceptedMeasurements int    `json:"accepted_measurements"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type healthResponse struct {
	Status string `json:"status"`
}

func NewHandler(pub publisher.Publisher, val validator.Validator) *Handler {
	return &Handler{
		publisher: pub,
		validator: val,
	}
}

func NewRouter(handler *Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/telemetry", handler.AcceptTelemetry)
	mux.HandleFunc("GET /health", handler.Health)
	return mux
}

func (h *Handler) AcceptTelemetry(w http.ResponseWriter, r *http.Request) {
	var batch usecase.TelemetryBatch

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&batch); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_batch", "invalid JSON body")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_batch", "invalid JSON body")
		return
	}

	if err := h.validator.ValidateBatch(batch); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_batch", err.Error())
		return
	}

	events := usecase.BuildEvents(batch)
	for _, event := range events {
		if err := h.publisher.Publish(r.Context(), event); err != nil {
			writeError(w, http.StatusServiceUnavailable, "publisher_unavailable", "publisher is unavailable")
			return
		}
	}

	writeJSON(w, http.StatusAccepted, acceptedResponse{
		Status:               "accepted",
		AcceptedMeasurements: len(events),
	})
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func writeError(w http.ResponseWriter, statusCode int, code string, message string) {
	writeJSON(w, statusCode, errorResponse{
		Error:   code,
		Message: message,
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
