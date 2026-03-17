package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"personal-finance-os/internal/imports"
	"personal-finance-os/internal/platform/cryptox"
	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/mongox"
	"personal-finance-os/internal/platform/postgresx"
	"personal-finance-os/internal/platform/startupx"
)

type maintenanceService struct {
	logger             *slog.Logger
	rawCollection      *mongo.Collection
	parsedCollection   *mongo.Collection
	postgresPool       *pgxpool.Pool
	keyring            *cryptox.Keyring
	requestTimeout     time.Duration
	target             string
	dryRun             bool
	rotateToCurrentKey bool
	backfillMissingKID bool
}

type maintenanceStats struct {
	RawImportsEncrypted   int64 `json:"raw_imports_encrypted"`
	RawImportsRotated     int64 `json:"raw_imports_rotated"`
	RawImportsKeyBackfill int64 `json:"raw_imports_key_backfill"`
	ParsedImportsScrubbed int64 `json:"parsed_imports_scrubbed"`
	PostgresRowsScrubbed  int64 `json:"postgres_rows_scrubbed"`
}

func main() {
	const serviceName = "sensitive-data-maintenance"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	requestTimeout := env.Duration("REQUEST_TIMEOUT", 30*time.Second)
	startupTimeout := env.Duration("STARTUP_TIMEOUT", time.Minute)
	mongoURI := env.String("MONGO_URI", "mongodb://localhost:27017")
	mongoDatabase := env.String("MONGO_DATABASE", "finance_os")
	rawCollectionName := env.String("MONGO_RAW_COLLECTION", "raw_imports")
	parsedCollectionName := env.String("MONGO_PARSED_COLLECTION", "parsed_imports")
	postgresDSN := env.String("POSTGRES_DSN", "postgres://finance:finance@localhost:5432/finance?sslmode=disable")
	target := strings.ToLower(env.String("SENSITIVE_DATA_TARGET", "all"))
	dryRun := env.Bool("SENSITIVE_DATA_DRY_RUN", true)
	rotateToCurrent := env.Bool("SENSITIVE_DATA_ROTATE_TO_CURRENT", false)
	backfillMissingKID := env.Bool("SENSITIVE_DATA_BACKFILL_MISSING_KID", false)

	keyring, err := cryptox.NewKeyring(
		env.String("DATA_ENCRYPTION_KEY_ID", "local-v1"),
		env.String("DATA_ENCRYPTION_KEY_B64", ""),
		env.String("DATA_ENCRYPTION_LEGACY_KEYS", ""),
	)
	if err != nil {
		panic(err)
	}

	startupCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	mongoClient, err := startupx.RetryValue(startupCtx, logger, "mongodb connect", func(ctx context.Context) (*mongo.Client, error) {
		return mongox.Connect(ctx, mongoURI)
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = mongoClient.Disconnect(context.Background())
	}()

	postgresPool, err := startupx.RetryValue(startupCtx, logger, "postgres connect", func(ctx context.Context) (*pgxpool.Pool, error) {
		return postgresx.Connect(ctx, postgresDSN)
	})
	if err != nil {
		panic(err)
	}
	defer postgresPool.Close()

	svc := &maintenanceService{
		logger:             logger,
		rawCollection:      mongoClient.Database(mongoDatabase).Collection(rawCollectionName),
		parsedCollection:   mongoClient.Database(mongoDatabase).Collection(parsedCollectionName),
		postgresPool:       postgresPool,
		keyring:            keyring,
		requestTimeout:     requestTimeout,
		target:             target,
		dryRun:             dryRun,
		rotateToCurrentKey: rotateToCurrent,
		backfillMissingKID: backfillMissingKID,
	}

	stats, err := svc.run(startupCtx)
	if err != nil {
		panic(err)
	}
	logger.Info("sensitive data maintenance completed",
		"target", target,
		"dry_run", dryRun,
		"rotate_to_current", rotateToCurrent,
		"backfill_missing_kid", backfillMissingKID,
		"raw_imports_encrypted", stats.RawImportsEncrypted,
		"raw_imports_rotated", stats.RawImportsRotated,
		"raw_imports_key_backfill", stats.RawImportsKeyBackfill,
		"parsed_imports_scrubbed", stats.ParsedImportsScrubbed,
		"postgres_rows_scrubbed", stats.PostgresRowsScrubbed,
	)
}

func (s *maintenanceService) run(ctx context.Context) (maintenanceStats, error) {
	stats := maintenanceStats{}

	if shouldRunTarget(s.target, "mongo", "all") {
		mongoStats, err := s.runMongoMaintenance(ctx)
		if err != nil {
			return stats, err
		}
		stats.RawImportsEncrypted = mongoStats.RawImportsEncrypted
		stats.RawImportsRotated = mongoStats.RawImportsRotated
		stats.RawImportsKeyBackfill = mongoStats.RawImportsKeyBackfill
		stats.ParsedImportsScrubbed = mongoStats.ParsedImportsScrubbed
	}

	if shouldRunTarget(s.target, "postgres", "all") {
		rows, err := s.scrubPostgresRawLines(ctx)
		if err != nil {
			return stats, err
		}
		stats.PostgresRowsScrubbed = rows
	}

	return stats, nil
}

func (s *maintenanceService) runMongoMaintenance(ctx context.Context) (maintenanceStats, error) {
	stats := maintenanceStats{}

	rawFilter := bson.M{
		"$or": bson.A{
			bson.M{"content": bson.M{"$exists": true, "$ne": nil}},
			bson.M{"content_enc": bson.M{"$exists": true, "$ne": nil}, "content_kid": bson.M{"$in": bson.A{"", nil}}},
		},
	}
	cursor, err := s.rawCollection.Find(ctx, rawFilter)
	if err != nil {
		return stats, err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var raw imports.RawImport
		if err := cursor.Decode(&raw); err != nil {
			return stats, err
		}

		updateSet := bson.M{"updated_at": time.Now().UTC()}
		updateUnset := bson.M{}

		switch {
		case len(raw.Content) > 0:
			plaintext := raw.Content
			if len(raw.ContentEnc) > 0 && s.rotateToCurrentKey {
				decrypted, err := s.keyring.Decrypt(raw.ContentEnc, raw.ContentNnc, raw.ContentKID)
				if err == nil {
					plaintext = decrypted
				}
			}
			ciphertext, nonce, keyID, err := s.keyring.Encrypt(plaintext)
			if err != nil {
				return stats, err
			}
			updateSet["content_enc"] = ciphertext
			updateSet["content_nnc"] = nonce
			updateSet["content_kid"] = keyID
			updateUnset["content"] = ""
			stats.RawImportsEncrypted++
		case len(raw.ContentEnc) > 0 && strings.TrimSpace(raw.ContentKID) == "" && s.backfillMissingKID:
			if _, err := s.keyring.Decrypt(raw.ContentEnc, raw.ContentNnc, ""); err != nil {
				return stats, fmt.Errorf("cannot backfill content_kid for import %s: %w", raw.ImportID, err)
			}
			updateSet["content_kid"] = s.keyring.CurrentKeyID()
			stats.RawImportsKeyBackfill++
		}

		if len(raw.ContentEnc) > 0 && s.rotateToCurrentKey && strings.TrimSpace(raw.ContentKID) != "" && strings.TrimSpace(raw.ContentKID) != s.keyring.CurrentKeyID() {
			plaintext, err := s.keyring.Decrypt(raw.ContentEnc, raw.ContentNnc, raw.ContentKID)
			if err != nil {
				return stats, err
			}
			ciphertext, nonce, keyID, err := s.keyring.Encrypt(plaintext)
			if err != nil {
				return stats, err
			}
			updateSet["content_enc"] = ciphertext
			updateSet["content_nnc"] = nonce
			updateSet["content_kid"] = keyID
			stats.RawImportsRotated++
		}

		if len(updateSet) == 1 && len(updateUnset) == 0 {
			continue
		}
		if s.dryRun {
			continue
		}

		update := bson.M{"$set": updateSet}
		if len(updateUnset) > 0 {
			update["$unset"] = updateUnset
		}
		if _, err := s.rawCollection.UpdateByID(ctx, raw.ID, update); err != nil {
			return stats, err
		}
	}
	if err := cursor.Err(); err != nil {
		return stats, err
	}

	parsedCursor, err := s.parsedCollection.Find(ctx, bson.M{"transactions.raw_line": bson.M{"$exists": true, "$ne": ""}})
	if err != nil {
		return stats, err
	}
	defer parsedCursor.Close(ctx)

	for parsedCursor.Next(ctx) {
		var parsed imports.ParsedImport
		if err := parsedCursor.Decode(&parsed); err != nil {
			return stats, err
		}
		changed := false
		for i := range parsed.Transactions {
			if strings.TrimSpace(parsed.Transactions[i].RawLine) == "" {
				continue
			}
			parsed.Transactions[i].RawLine = ""
			changed = true
		}
		if !changed {
			continue
		}
		stats.ParsedImportsScrubbed++
		if s.dryRun {
			continue
		}
		parsed.UpdatedAt = time.Now().UTC()
		if _, err := s.parsedCollection.UpdateByID(ctx, parsed.ID, bson.M{
			"$set": bson.M{
				"transactions": parsed.Transactions,
				"updated_at":   parsed.UpdatedAt,
			},
		}); err != nil {
			return stats, err
		}
	}
	if err := parsedCursor.Err(); err != nil {
		return stats, err
	}

	return stats, nil
}

func (s *maintenanceService) scrubPostgresRawLines(ctx context.Context) (int64, error) {
	if s.dryRun {
		var count int64
		if err := s.postgresPool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE raw_line <> ''`).Scan(&count); err != nil {
			return 0, err
		}
		return count, nil
	}

	tag, err := s.postgresPool.Exec(ctx, `UPDATE transactions SET raw_line = '', updated_at = now() WHERE raw_line <> ''`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func shouldRunTarget(target string, candidates ...string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(target))
	for _, candidate := range candidates {
		if trimmed == strings.TrimSpace(strings.ToLower(candidate)) {
			return true
		}
	}
	return false
}
