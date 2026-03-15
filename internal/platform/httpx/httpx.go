package httpx

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func RegisterBaseRoutes(mux *http.ServeMux, service string) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		JSON(w, http.StatusOK, map[string]any{
			"service": service,
			"status":  "ok",
		})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "service_up{service=%q} 1\n", service)
	})
}

func JSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func ReadJSON(r *http.Request, payload any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(payload)
}
