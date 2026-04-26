package appenv

import (
	"os"
	"strings"
)

func Current() string {
	value := strings.TrimSpace(strings.ToLower(os.Getenv("APP_ENV")))
	if value == "" {
		return "local"
	}
	return value
}

func IsLocalLike() bool {
	switch Current() {
	case "local", "dev", "development", "test":
		return true
	default:
		return false
	}
}
