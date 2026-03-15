package main

import (
	"fmt"
	"net/http"
	"sort"
	"time"

	"personal-finance-os/internal/ledger"
	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/runtime"
)

func main() {
	const serviceName = "ledger-service"

	logger := logging.New(serviceName)
	store := ledger.NewStore()
	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)

	mux.HandleFunc("GET /api/v1/transactions", func(w http.ResponseWriter, _ *http.Request) {
		transactions := store.List()
		sort.Slice(transactions, func(i, j int) bool {
			return transactions[i].OccurredAt.After(transactions[j].OccurredAt)
		})
		httpx.JSON(w, http.StatusOK, map[string]any{"transactions": transactions})
	})
	mux.HandleFunc("POST /api/v1/transactions", func(w http.ResponseWriter, r *http.Request) {
		var transaction ledger.Transaction
		if err := httpx.ReadJSON(r, &transaction); err != nil {
			httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
			return
		}
		if transaction.ID == "" {
			transaction.ID = fmt.Sprintf("txn-%d", time.Now().UnixNano())
		}
		if transaction.OccurredAt.IsZero() {
			transaction.OccurredAt = time.Now().UTC()
		}
		store.Add(transaction)
		httpx.JSON(w, http.StatusCreated, transaction)
	})
	mux.HandleFunc("GET /api/v1/categories", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]any{
			"categories": []string{"housing", "groceries", "transport", "subscriptions", "food", "salary"},
		})
	})
	mux.HandleFunc("GET /api/v1/recurring", func(w http.ResponseWriter, _ *http.Request) {
		patterns := ledger.DetectRecurring(store.List())
		httpx.JSON(w, http.StatusOK, map[string]any{"patterns": patterns})
	})

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8084"),
		Handler:  mux,
		Logger:   logger,
	}); err != nil {
		panic(err)
	}
}
