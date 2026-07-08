package http

import (
	"encoding/json"
	nethttp "net/http"
)

type Handler struct{}

type healthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

func NewHandler() *Handler {
	return &Handler{}
}

func NewRouter(handler *Handler) nethttp.Handler {
	mux := nethttp.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health)
	return mux
}

func (h *Handler) Health(w nethttp.ResponseWriter, r *nethttp.Request) {
	writeJSON(w, nethttp.StatusOK, healthResponse{
		Status:  "ok",
		Service: "processing",
	})
}

func writeJSON(w nethttp.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
