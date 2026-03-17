package clickhousex

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNormalizeDSN(t *testing.T) {
	t.Parallel()

	baseURL, database, username, password, err := normalizeDSN("clickhouse://finance:finance@clickhouse:9000/finance_os")
	if err != nil {
		t.Fatalf("normalizeDSN returned error: %v", err)
	}
	if baseURL != "http://clickhouse:8123" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
	if database != "finance_os" {
		t.Fatalf("unexpected database: %s", database)
	}
	if username != "finance" || password != "finance" {
		t.Fatalf("unexpected credentials: %s / %s", username, password)
	}
}

func TestInsertJSONEachRow(t *testing.T) {
	t.Parallel()

	var capturedQuery string
	var capturedBody string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.Query().Get("query")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}
		capturedBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &Client{
		baseURL: server.URL,
		httpClient: &http.Client{
			Timeout: time.Second,
		},
	}

	err := client.InsertJSONEachRow(context.Background(), "transaction_events", []any{
		map[string]any{"event_id": "txn-1", "amount_cents": 450},
	})
	if err != nil {
		t.Fatalf("InsertJSONEachRow returned error: %v", err)
	}
	if capturedQuery != "INSERT INTO transaction_events FORMAT JSONEachRow" {
		t.Fatalf("unexpected query: %s", capturedQuery)
	}
	if !strings.Contains(capturedBody, "\"event_id\":\"txn-1\"") {
		t.Fatalf("unexpected body: %s", capturedBody)
	}
}

func TestQueryJSONAppendsFormat(t *testing.T) {
	t.Parallel()

	var capturedQuery string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.Query().Get("query")
		_, _ = w.Write([]byte(`{"data":[{"value":1}]}`))
	}))
	defer server.Close()

	client := &Client{
		baseURL: server.URL,
		httpClient: &http.Client{
			Timeout: time.Second,
		},
	}

	body, err := client.QueryJSON(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("QueryJSON returned error: %v", err)
	}
	if capturedQuery != "SELECT 1 FORMAT JSON" {
		t.Fatalf("unexpected query: %s", capturedQuery)
	}
	if string(body) != `{"data":[{"value":1}]}` {
		t.Fatalf("unexpected body: %s", string(body))
	}
}
