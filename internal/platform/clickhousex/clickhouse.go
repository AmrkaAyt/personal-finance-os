package clickhousex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	database   string
	username   string
	password   string
	httpClient *http.Client
}

func New(rawDSN string, timeout time.Duration) (*Client, error) {
	baseURL, database, username, password, err := normalizeDSN(rawDSN)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Client{
		baseURL:  baseURL,
		database: database,
		username: username,
		password: password,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodPost, "SELECT 1", nil, "")
	return err
}

func (c *Client) Exec(ctx context.Context, query string) error {
	_, err := c.do(ctx, http.MethodPost, query, nil, "")
	return err
}

func (c *Client) InsertJSONEachRow(ctx context.Context, table string, rows []any) error {
	if len(rows) == 0 {
		return nil
	}

	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}

	query := fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", sanitizeIdentifier(table))
	_, err := c.do(ctx, http.MethodPost, query, body.Bytes(), "application/x-ndjson")
	return err
}

func (c *Client) QueryJSON(ctx context.Context, query string) ([]byte, error) {
	if !strings.Contains(strings.ToUpper(query), "FORMAT ") {
		query += " FORMAT JSON"
	}
	return c.do(ctx, http.MethodPost, query, nil, "")
}

func (c *Client) do(ctx context.Context, method, query string, body []byte, contentType string) ([]byte, error) {
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("clickhouse query is empty")
	}

	requestURL, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, err
	}

	values := requestURL.Query()
	values.Set("query", strings.TrimSpace(query))
	if c.database != "" {
		values.Set("database", c.database)
	}
	requestURL.RawQuery = values.Encode()

	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if c.username != "" {
		request.SetBasicAuth(c.username, c.password)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("clickhouse request failed: status=%d body=%s", response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return responseBody, nil
}

func normalizeDSN(rawDSN string) (string, string, string, string, error) {
	dsn := strings.TrimSpace(rawDSN)
	if dsn == "" {
		dsn = "http://localhost:8123"
	}
	if !strings.Contains(dsn, "://") {
		dsn = "http://" + dsn
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", "", "", "", err
	}

	scheme := strings.ToLower(parsed.Scheme)
	switch scheme {
	case "", "http", "https":
	case "clickhouse", "tcp":
		if parsed.Scheme == "tcp" {
			scheme = "http"
		} else {
			scheme = "http"
		}
	default:
		return "", "", "", "", fmt.Errorf("unsupported clickhouse scheme %q", parsed.Scheme)
	}

	host := parsed.Hostname()
	if host == "" {
		host = "localhost"
	}
	port := parsed.Port()
	switch {
	case port == "":
		port = defaultPortForScheme(scheme)
	case port == "9000":
		port = "8123"
	}

	normalized := url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, port),
		Path:   "/",
	}

	database := strings.Trim(parsed.Path, "/")
	if database == "" {
		database = strings.TrimSpace(parsed.Query().Get("database"))
	}

	username := ""
	password := ""
	if parsed.User != nil {
		username = parsed.User.Username()
		password, _ = parsed.User.Password()
	}

	return strings.TrimRight(normalized.String(), "/"), database, username, password, nil
}

func defaultPortForScheme(scheme string) string {
	if scheme == "https" {
		return "8443"
	}
	return "8123"
}

func sanitizeIdentifier(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "unknown"
	}
	var builder strings.Builder
	for _, char := range trimmed {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' || char == '.' {
			builder.WriteRune(char)
		}
	}
	if builder.Len() == 0 {
		return "unknown"
	}
	return builder.String()
}
