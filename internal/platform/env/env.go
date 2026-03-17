package env

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	sharedOnce    sync.Once
	serviceLoadMu sync.Mutex
	loadedService string
	originalEnv   = captureExistingEnv()
)

func LoadService(service string) {
	ensureLoaded()

	service = strings.TrimSpace(service)
	if service == "" {
		return
	}

	serviceLoadMu.Lock()
	defer serviceLoadMu.Unlock()
	if loadedService == service {
		return
	}

	for _, path := range serviceFiles(service) {
		_ = loadFile(path, true)
	}
	for _, path := range envOverrideFiles() {
		_ = loadFile(path, true)
	}
	loadedService = service
}

func String(key, fallback string) string {
	ensureLoaded()
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func Strings(key string, fallback []string) []string {
	ensureLoaded()
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return fallback
	}
	return result
}

func Bool(key string, fallback bool) bool {
	ensureLoaded()
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func Int(key string, fallback int) int {
	ensureLoaded()
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func Duration(key string, fallback time.Duration) time.Duration {
	ensureLoaded()
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func ensureLoaded() {
	sharedOnce.Do(func() {
		for _, path := range sharedFiles() {
			_ = loadFile(path, false)
		}
	})
}

func sharedFiles() []string {
	seen := make(map[string]struct{})
	paths := make([]string, 0, 2)

	add := func(path string) {
		trimmed := strings.TrimSpace(path)
		if trimmed == "" {
			return
		}
		absolute, err := filepath.Abs(trimmed)
		if err != nil {
			return
		}
		if _, exists := seen[absolute]; exists {
			return
		}
		seen[absolute] = struct{}{}
		paths = append(paths, absolute)
	}

	root := findProjectRoot()
	if root == "" {
		if wd, err := os.Getwd(); err == nil {
			root = wd
		}
	}
	if root != "" {
		add(filepath.Join(root, ".env"))
		add(filepath.Join(root, ".env.local"))
	}

	return paths
}

func serviceFiles(service string) []string {
	root := findProjectRoot()
	if root == "" {
		if wd, err := os.Getwd(); err == nil {
			root = wd
		}
	}
	if root == "" {
		return nil
	}
	return []string{
		filepath.Join(root, "env", service+".env"),
		filepath.Join(root, "env", service+".local.env"),
	}
}

func envOverrideFiles() []string {
	paths := make([]string, 0, 2)
	for _, path := range strings.Split(os.Getenv("ENV_FILE"), ",") {
		trimmed := strings.TrimSpace(path)
		if trimmed != "" {
			paths = append(paths, trimmed)
		}
	}
	return paths
}

func findProjectRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}

	current := wd
	for {
		if fileExists(filepath.Join(current, "go.mod")) {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			return ""
		}
		current = parent
	}
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func loadFile(path string, override bool) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if !shouldSet(key, override) {
			continue
		}
		_ = os.Setenv(key, normalizeValue(value))
	}
	return scanner.Err()
}

func normalizeValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) >= 2 {
		if (trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"') || (trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'') {
			return trimmed[1 : len(trimmed)-1]
		}
	}
	return trimmed
}

func captureExistingEnv() map[string]struct{} {
	result := make(map[string]struct{})
	for _, item := range os.Environ() {
		key, _, ok := strings.Cut(item, "=")
		if ok && key != "" {
			result[key] = struct{}{}
		}
	}
	return result
}

func shouldSet(key string, override bool) bool {
	if _, exists := originalEnv[key]; exists {
		return false
	}
	if _, exists := os.LookupEnv(key); !exists {
		return true
	}
	return override
}
