package migratex

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Migration struct {
	Version    string
	Name       string
	Path       string
	Statements []string
}

func LoadDir(root, dir string, replacements map[string]string) ([]Migration, error) {
	base := strings.TrimSpace(root)
	if base == "" {
		base = "."
	}
	targetDir := filepath.Join(base, filepath.FromSlash(strings.TrimSpace(dir)))
	entries, err := os.ReadDir(targetDir)
	if err != nil {
		return nil, err
	}

	migrations := make([]Migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}

		version, name, err := parseFilename(entry.Name())
		if err != nil {
			return nil, err
		}
		path := filepath.Join(targetDir, entry.Name())
		body, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}

		statements := splitStatements(applyReplacements(string(body), replacements))
		if len(statements) == 0 {
			return nil, fmt.Errorf("migration %s contains no statements", entry.Name())
		}

		migrations = append(migrations, Migration{
			Version:    version,
			Name:       name,
			Path:       path,
			Statements: statements,
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		if migrations[i].Version == migrations[j].Version {
			return migrations[i].Name < migrations[j].Name
		}
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

func parseFilename(name string) (string, string, error) {
	trimmed := strings.TrimSuffix(strings.TrimSpace(name), ".sql")
	version, rest, ok := strings.Cut(trimmed, "_")
	if !ok || strings.TrimSpace(version) == "" || strings.TrimSpace(rest) == "" {
		return "", "", fmt.Errorf("invalid migration filename %q", name)
	}
	return strings.TrimSpace(version), strings.TrimSpace(rest), nil
}

func applyReplacements(body string, replacements map[string]string) string {
	result := body
	for key, value := range replacements {
		result = strings.ReplaceAll(result, key, value)
	}
	return result
}

func splitStatements(body string) []string {
	parts := strings.Split(body, ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		statements = append(statements, trimmed)
	}
	return statements
}
