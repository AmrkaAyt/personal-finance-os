//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type tokenPair struct {
	AccessToken string `json:"access_token"`
}

type importResponse struct {
	ImportID string `json:"import_id"`
	Status   string `json:"status"`
}

type rawImportStatus struct {
	ImportID string `json:"import_id"`
	Status   string `json:"status"`
}

type parsedImport struct {
	ImportID     string           `json:"import_id"`
	Status       string           `json:"status"`
	Transactions []map[string]any `json:"transactions"`
}

type analyticsResponse struct {
	Result struct {
		Data []map[string]any `json:"data"`
	} `json:"result"`
}

func TestGatewayImportPipeline(t *testing.T) {
	skipIfIntegrationDisabled(t)

	client := &http.Client{Timeout: 15 * time.Second}
	baseURL := integrationBaseURL()
	token := loginDemoUser(t, client, baseURL)
	importID := uploadSampleStatement(t, client, baseURL, token)

	waitFor(t, 30*time.Second, 1*time.Second, "raw import parsed", func() (bool, error) {
		status, err := getRawImportStatus(client, baseURL, token, importID)
		if err != nil {
			return false, err
		}
		return strings.TrimSpace(status.Status) == "parsed", nil
	})

	waitFor(t, 30*time.Second, 1*time.Second, "parsed import available", func() (bool, error) {
		parsed, err := getParsedImport(client, baseURL, token, importID)
		if err != nil {
			return false, err
		}
		return strings.TrimSpace(parsed.Status) == "parsed" && len(parsed.Transactions) > 0, nil
	})
}

func TestGatewayManualTransactionDrivesAnalyticsAndAlerts(t *testing.T) {
	skipIfIntegrationDisabled(t)

	client := &http.Client{Timeout: 15 * time.Second}
	baseURL := integrationBaseURL()
	token := loginDemoUser(t, client, baseURL)

	now := time.Now().UTC()
	userID := fmt.Sprintf("integration-user-%d", now.UnixNano())
	merchant := fmt.Sprintf("integration-merchant-%d", now.UnixNano())
	category := "integration_test"

	payload := map[string]any{
		"user_id":      userID,
		"account_id":   "integration-account",
		"merchant":     merchant,
		"category":     category,
		"currency":     "USD",
		"amount_cents": -6000,
		"occurred_at":  now.Format(time.RFC3339Nano),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal transaction payload: %v", err)
	}

	request, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/transactions", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create transaction request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("create transaction request failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create transaction status = %d, want %d", response.StatusCode, http.StatusCreated)
	}

	day := now.Format("2006-01-02")

	waitFor(t, 30*time.Second, 1*time.Second, "analytics daily spend projection", func() (bool, error) {
		analytics, err := getAnalytics(client, baseURL, token, fmt.Sprintf("/api/v1/analytics/projections/daily-spend?user_id=%s&from=%s&to=%s&category=%s", userID, day, day, category))
		if err != nil {
			return false, err
		}
		for _, row := range analytics.Result.Data {
			if stringValue(row["category"]) == category {
				return true, nil
			}
		}
		return false, nil
	})

	waitFor(t, 30*time.Second, 1*time.Second, "analytics alert projection", func() (bool, error) {
		analytics, err := getAnalytics(client, baseURL, token, fmt.Sprintf("/api/v1/analytics/projections/alerts?user_id=%s&from=%s&to=%s&severity=warning", userID, day, day))
		if err != nil {
			return false, err
		}
		var foundLarge, foundNewMerchant bool
		for _, row := range analytics.Result.Data {
			switch stringValue(row["type"]) {
			case "large_transaction":
				foundLarge = true
			case "new_merchant":
				foundNewMerchant = true
			}
		}
		return foundLarge && foundNewMerchant, nil
	})
}

func integrationBaseURL() string {
	if baseURL := strings.TrimSpace(os.Getenv("INTEGRATION_BASE_URL")); baseURL != "" {
		return strings.TrimRight(baseURL, "/")
	}
	return "http://localhost:8080"
}

func skipIfIntegrationDisabled(t *testing.T) {
	t.Helper()
	if os.Getenv("INTEGRATION_TESTS") != "1" {
		t.Skip("integration tests disabled; set INTEGRATION_TESTS=1")
	}
}

func loginDemoUser(t *testing.T, client *http.Client, baseURL string) string {
	t.Helper()

	body := bytes.NewBufferString(`{"username":"demo","password":"demo"}`)
	request, err := http.NewRequest(http.MethodPost, baseURL+"/auth/login", body)
	if err != nil {
		t.Fatalf("create login request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("login request failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("login status = %d, want %d, body=%s", response.StatusCode, http.StatusOK, strings.TrimSpace(string(payload)))
	}

	var pair tokenPair
	if err := json.NewDecoder(response.Body).Decode(&pair); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	if strings.TrimSpace(pair.AccessToken) == "" {
		t.Fatal("login returned empty access token")
	}
	return pair.AccessToken
}

func uploadSampleStatement(t *testing.T, client *http.Client, baseURL, token string) string {
	t.Helper()

	filePath := filepath.Join("..", "..", "examples", "sample-statement.csv")
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("open sample statement: %v", err)
	}
	defer file.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		t.Fatalf("create multipart file: %v", err)
	}
	if _, err := io.Copy(part, file); err != nil {
		t.Fatalf("copy sample statement: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	request, err := http.NewRequest(http.MethodPost, baseURL+"/imports/raw", &body)
	if err != nil {
		t.Fatalf("create import request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", writer.FormDataContentType())

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("import request failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("import status = %d, want %d, body=%s", response.StatusCode, http.StatusAccepted, strings.TrimSpace(string(payload)))
	}

	var result importResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode import response: %v", err)
	}
	if strings.TrimSpace(result.ImportID) == "" {
		t.Fatal("import response returned empty import_id")
	}
	return result.ImportID
}

func getRawImportStatus(client *http.Client, baseURL, token, importID string) (rawImportStatus, error) {
	request, err := http.NewRequest(http.MethodGet, baseURL+"/imports/"+importID, nil)
	if err != nil {
		return rawImportStatus{}, err
	}
	request.Header.Set("Authorization", "Bearer "+token)

	response, err := client.Do(request)
	if err != nil {
		return rawImportStatus{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		return rawImportStatus{}, fmt.Errorf("raw import status = %d body=%s", response.StatusCode, strings.TrimSpace(string(payload)))
	}

	var item rawImportStatus
	err = json.NewDecoder(response.Body).Decode(&item)
	return item, err
}

func getParsedImport(client *http.Client, baseURL, token, importID string) (parsedImport, error) {
	request, err := http.NewRequest(http.MethodGet, baseURL+"/parser/results/"+importID, nil)
	if err != nil {
		return parsedImport{}, err
	}
	request.Header.Set("Authorization", "Bearer "+token)

	response, err := client.Do(request)
	if err != nil {
		return parsedImport{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		return parsedImport{}, fmt.Errorf("parsed import status = %d body=%s", response.StatusCode, strings.TrimSpace(string(payload)))
	}

	var item parsedImport
	err = json.NewDecoder(response.Body).Decode(&item)
	return item, err
}

func getAnalytics(client *http.Client, baseURL, token, path string) (analyticsResponse, error) {
	request, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
	if err != nil {
		return analyticsResponse{}, err
	}
	request.Header.Set("Authorization", "Bearer "+token)

	response, err := client.Do(request)
	if err != nil {
		return analyticsResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		return analyticsResponse{}, fmt.Errorf("analytics status = %d body=%s", response.StatusCode, strings.TrimSpace(string(payload)))
	}

	var item analyticsResponse
	err = json.NewDecoder(response.Body).Decode(&item)
	return item, err
}

func waitFor(t *testing.T, timeout, interval time.Duration, name string, fn func() (bool, error)) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := fn()
		if err == nil && ok {
			return
		}
		lastErr = err
		time.Sleep(interval)
	}
	if lastErr != nil {
		t.Fatalf("%s did not complete in %s: %v", name, timeout, lastErr)
	}
	t.Fatalf("%s did not complete in %s", name, timeout)
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprintf("%v", typed)
	}
}
