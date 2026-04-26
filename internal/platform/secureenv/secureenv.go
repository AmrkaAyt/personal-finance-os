package secureenv

import (
	"fmt"
	"log/slog"
	"strings"

	"personal-finance-os/internal/platform/appenv"
)

type Check func() error

func Enforce(service string, logger *slog.Logger, checks ...Check) error {
	mode := appenv.Current()
	if appenv.IsLocalLike() {
		if logger != nil {
			logger.Info("security env validation skipped in local-like mode", "service", service, "app_env", mode)
		}
		return nil
	}

	for _, check := range checks {
		if check == nil {
			continue
		}
		if err := check(); err != nil {
			return fmt.Errorf("%s: security env validation failed for app_env=%s: %w", strings.TrimSpace(service), mode, err)
		}
	}
	return nil
}

func RequireNonEmpty(name, value string) Check {
	return func() error {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must be set", name)
		}
		return nil
	}
}

func RejectAnyOf(name, value string, blocked ...string) Check {
	return func() error {
		trimmed := strings.TrimSpace(value)
		for _, candidate := range blocked {
			if trimmed == strings.TrimSpace(candidate) {
				return fmt.Errorf("%s uses insecure default value", name)
			}
		}
		return nil
	}
}

func RejectContains(name, value string, blocked ...string) Check {
	return func() error {
		trimmed := strings.TrimSpace(strings.ToLower(value))
		for _, candidate := range blocked {
			if candidate != "" && strings.Contains(trimmed, strings.TrimSpace(strings.ToLower(candidate))) {
				return fmt.Errorf("%s contains insecure fragment %q", name, candidate)
			}
		}
		return nil
	}
}

func RejectPrefix(name, value string, blockedPrefixes ...string) Check {
	return func() error {
		trimmed := strings.TrimSpace(strings.ToLower(value))
		for _, prefix := range blockedPrefixes {
			normalized := strings.TrimSpace(strings.ToLower(prefix))
			if normalized != "" && strings.HasPrefix(trimmed, normalized) {
				return fmt.Errorf("%s uses disallowed secret source %q", name, prefix)
			}
		}
		return nil
	}
}

func RequireFalse(name string, value bool) Check {
	return func() error {
		if value {
			return fmt.Errorf("%s must be false outside local", name)
		}
		return nil
	}
}
