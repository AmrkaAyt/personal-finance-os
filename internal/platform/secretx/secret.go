package secretx

import (
	"fmt"
	"os"
	"strings"
)

func RefOrEnv(ref, envVar string) string {
	trimmed := strings.TrimSpace(ref)
	if trimmed != "" {
		return trimmed
	}
	if strings.TrimSpace(os.Getenv(envVar)) == "" {
		return ""
	}
	return "env:" + strings.TrimSpace(envVar)
}

func Resolve(ref string) (string, error) {
	trimmed := strings.TrimSpace(ref)
	if trimmed == "" {
		return "", nil
	}

	scheme, value, ok := strings.Cut(trimmed, ":")
	if !ok {
		return "", fmt.Errorf("invalid secret ref %q", trimmed)
	}

	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "env":
		name := strings.TrimSpace(value)
		if name == "" {
			return "", fmt.Errorf("invalid env secret ref %q", trimmed)
		}
		resolved := os.Getenv(name)
		if strings.TrimSpace(resolved) == "" {
			return "", fmt.Errorf("env secret %q is empty", name)
		}
		return strings.TrimSpace(resolved), nil
	case "file":
		path := strings.TrimSpace(value)
		if path == "" {
			return "", fmt.Errorf("invalid file secret ref %q", trimmed)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		resolved := strings.TrimSpace(string(body))
		if resolved == "" {
			return "", fmt.Errorf("file secret %q is empty", path)
		}
		return resolved, nil
	default:
		return "", fmt.Errorf("unsupported secret ref scheme %q", scheme)
	}
}
