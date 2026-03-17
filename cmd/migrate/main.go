package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"personal-finance-os/internal/platform/clickhousex"
	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/migratex"
	"personal-finance-os/internal/platform/postgresx"
	"personal-finance-os/internal/platform/startupx"
)

func main() {
	const serviceName = "migrate"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	requestTimeout := env.Duration("REQUEST_TIMEOUT", 15*time.Second)
	startupTimeout := env.Duration("STARTUP_TIMEOUT", time.Minute)
	root := env.String("MIGRATIONS_ROOT", ".")
	target := strings.ToLower(env.String("MIGRATE_TARGET", "all"))

	startupCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	if shouldRun(target, "postgres") {
		postgresDSN := env.String("POSTGRES_DSN", "postgres://finance:finance@localhost:5432/finance?sslmode=disable")
		pool, err := startupx.RetryValue(startupCtx, logger, "postgres connect for migrations", func(ctx context.Context) (*pgxpool.Pool, error) {
			return postgresx.Connect(ctx, postgresDSN)
		})
		if err != nil {
			panic(err)
		}
		if err := applyPostgresMigrations(startupCtx, logger, pool, root); err != nil {
			pool.Close()
			panic(err)
		}
		pool.Close()
	}

	if shouldRun(target, "clickhouse") {
		clickhouseDSN := env.String("CLICKHOUSE_DSN", "http://finance:finance@localhost:8123")
		clickhouseDatabase := sanitizeIdentifier(env.String("CLICKHOUSE_DATABASE", "finance_os"))
		client, err := clickhousex.New(clickhouseDSN, requestTimeout)
		if err != nil {
			panic(err)
		}
		if err := startupx.Retry(startupCtx, logger, "clickhouse connect for migrations", func(ctx context.Context) error {
			return client.Ping(ctx)
		}); err != nil {
			panic(err)
		}
		if err := applyClickHouseMigrations(startupCtx, logger, client, root, clickhouseDatabase); err != nil {
			panic(err)
		}
	}

	logger.Info("migrations completed", "target", target)
}

func applyPostgresMigrations(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, root string) error {
	if _, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version text PRIMARY KEY,
    name text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)`); err != nil {
		return err
	}

	applied, err := postgresAppliedVersions(ctx, pool)
	if err != nil {
		return err
	}

	migrations, err := migratex.LoadDir(root, "migrations/postgres", nil)
	if err != nil {
		return err
	}

	for _, migration := range migrations {
		if _, exists := applied[migration.Version]; exists {
			continue
		}

		tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return err
		}
		defer func() {
			_ = tx.Rollback(ctx)
		}()

		for _, statement := range migration.Statements {
			if _, err := tx.Exec(ctx, statement); err != nil {
				return fmt.Errorf("postgres migration %s failed: %w", migration.Version, err)
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, migration.Version, migration.Name); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		logger.Info("postgres migration applied", "version", migration.Version, "name", migration.Name)
	}

	return nil
}

func postgresAppliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[string]struct{}, error) {
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]struct{})
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		result[strings.TrimSpace(version)] = struct{}{}
	}
	return result, rows.Err()
}

func applyClickHouseMigrations(ctx context.Context, logger *slog.Logger, client *clickhousex.Client, root, database string) error {
	if err := client.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)); err != nil {
		return err
	}
	if err := client.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.schema_migrations (
    version String,
    name String,
    applied_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version
`, database)); err != nil {
		return err
	}

	applied, err := clickhouseAppliedVersions(ctx, client, database)
	if err != nil {
		return err
	}

	migrations, err := migratex.LoadDir(root, "migrations/clickhouse", map[string]string{"{{DATABASE}}": database})
	if err != nil {
		return err
	}

	for _, migration := range migrations {
		if _, exists := applied[migration.Version]; exists {
			continue
		}

		for _, statement := range migration.Statements {
			if err := client.Exec(ctx, statement); err != nil {
				return fmt.Errorf("clickhouse migration %s failed: %w", migration.Version, err)
			}
		}
		insert := fmt.Sprintf(
			"INSERT INTO %s.schema_migrations (version, name, applied_at) VALUES ('%s', '%s', now64(3))",
			database,
			escapeSQLString(migration.Version),
			escapeSQLString(migration.Name),
		)
		if err := client.Exec(ctx, insert); err != nil {
			return err
		}
		logger.Info("clickhouse migration applied", "version", migration.Version, "name", migration.Name)
	}

	return nil
}

func clickhouseAppliedVersions(ctx context.Context, client *clickhousex.Client, database string) (map[string]struct{}, error) {
	body, err := client.QueryJSON(ctx, fmt.Sprintf("SELECT version FROM %s.schema_migrations", database))
	if err != nil {
		return nil, err
	}

	var payload struct {
		Data []struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}

	result := make(map[string]struct{}, len(payload.Data))
	for _, item := range payload.Data {
		result[strings.TrimSpace(item.Version)] = struct{}{}
	}
	return result, nil
}

func shouldRun(target, candidate string) bool {
	switch strings.TrimSpace(target) {
	case "", "all":
		return true
	default:
		return strings.EqualFold(target, candidate)
	}
}

func sanitizeIdentifier(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "default"
	}
	var builder strings.Builder
	for _, char := range trimmed {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' {
			builder.WriteRune(char)
		}
	}
	if builder.Len() == 0 {
		return "default"
	}
	return builder.String()
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "'", "''")
}
