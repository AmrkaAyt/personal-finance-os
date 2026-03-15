package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"time"

	parserdomain "personal-finance-os/internal/parser"
)

const (
	DefaultUserID    = "user-demo"
	DefaultAccountID = "imported-default"
)

var DefaultCategories = []string{
	"food",
	"groceries",
	"housing",
	"income",
	"subscriptions",
	"transport",
	"uncategorized",
}

type Transaction struct {
	ID             string    `json:"id"`
	UserID         string    `json:"user_id"`
	AccountID      string    `json:"account_id"`
	SourceImportID string    `json:"source_import_id"`
	Fingerprint    string    `json:"fingerprint,omitempty"`
	Merchant       string    `json:"merchant"`
	Category       string    `json:"category"`
	AmountCents    int64     `json:"amount_cents"`
	Currency       string    `json:"currency"`
	OccurredAt     time.Time `json:"occurred_at"`
	RawLine        string    `json:"raw_line,omitempty"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
}

type RecurringPattern struct {
	Merchant     string `json:"merchant"`
	Category     string `json:"category"`
	AmountCents  int64  `json:"amount_cents"`
	IntervalDays int    `json:"interval_days"`
	Count        int    `json:"count"`
}

type TransactionUpsertedEvent struct {
	TransactionID   string    `json:"transaction_id"`
	UserID          string    `json:"user_id"`
	AccountID       string    `json:"account_id"`
	SourceImportID  string    `json:"source_import_id"`
	Merchant        string    `json:"merchant"`
	Category        string    `json:"category"`
	AmountCents     int64     `json:"amount_cents"`
	Currency        string    `json:"currency"`
	OccurredAt      time.Time `json:"occurred_at"`
	TransactionHash string    `json:"transaction_hash"`
}

func TransactionFingerprintFromLedger(transaction Transaction) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(transaction.SourceImportID),
		normalizeText(transaction.Merchant),
		normalizeCategory(transaction.Category),
		normalizeCurrency(transaction.Currency),
		strconv.FormatInt(transaction.AmountCents, 10),
		transaction.OccurredAt.UTC().Format(time.RFC3339Nano),
		strings.TrimSpace(transaction.RawLine),
	}, "|")))
	return hex.EncodeToString(sum[:])
}

func NewTransactionFromParsed(userID, accountID, importID string, transaction parserdomain.Transaction) Transaction {
	occurredAt := time.Now().UTC()
	if transaction.OccurredAt != nil {
		occurredAt = transaction.OccurredAt.UTC()
	}

	fingerprint := TransactionFingerprint(importID, transaction)
	return Transaction{
		ID:             buildTransactionID(fingerprint),
		UserID:         firstNonEmpty(userID, DefaultUserID),
		AccountID:      firstNonEmpty(accountID, DefaultAccountID),
		SourceImportID: importID,
		Fingerprint:    fingerprint,
		Merchant:       normalizeText(transaction.Merchant),
		Category:       normalizeCategory(transaction.Category),
		AmountCents:    transaction.AmountCents,
		Currency:       normalizeCurrency(transaction.Currency),
		OccurredAt:     occurredAt,
		RawLine:        strings.TrimSpace(transaction.RawLine),
	}
}

func NewTransactionUpsertedEvent(transaction Transaction) TransactionUpsertedEvent {
	return TransactionUpsertedEvent{
		TransactionID:   transaction.ID,
		UserID:          transaction.UserID,
		AccountID:       transaction.AccountID,
		SourceImportID:  transaction.SourceImportID,
		Merchant:        transaction.Merchant,
		Category:        transaction.Category,
		AmountCents:     transaction.AmountCents,
		Currency:        transaction.Currency,
		OccurredAt:      transaction.OccurredAt,
		TransactionHash: transaction.Fingerprint,
	}
}

func TransactionFingerprint(importID string, transaction parserdomain.Transaction) string {
	occurredAt := ""
	if transaction.OccurredAt != nil {
		occurredAt = transaction.OccurredAt.UTC().Format(time.RFC3339Nano)
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(importID),
		normalizeText(transaction.Merchant),
		normalizeCategory(transaction.Category),
		normalizeCurrency(transaction.Currency),
		strconv.FormatInt(transaction.AmountCents, 10),
		occurredAt,
		strings.TrimSpace(transaction.RawLine),
	}, "|")))
	return hex.EncodeToString(sum[:])
}

func DetectRecurring(transactions []Transaction) []RecurringPattern {
	groups := make(map[string][]Transaction)
	for _, transaction := range transactions {
		key := transaction.Merchant + "|" + transaction.Category + "|" + transaction.Currency + "|" + formatAmount(transaction.AmountCents)
		groups[key] = append(groups[key], transaction)
	}

	patterns := make([]RecurringPattern, 0)
	for _, items := range groups {
		if len(items) < 2 {
			continue
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].OccurredAt.Before(items[j].OccurredAt)
		})

		totalGap := 0
		validGaps := 0
		for i := 1; i < len(items); i++ {
			gap := int(items[i].OccurredAt.Sub(items[i-1].OccurredAt).Hours() / 24)
			if gap >= 25 && gap <= 35 {
				totalGap += gap
				validGaps++
			}
		}
		if validGaps == 0 {
			continue
		}
		patterns = append(patterns, RecurringPattern{
			Merchant:     items[0].Merchant,
			Category:     items[0].Category,
			AmountCents:  items[0].AmountCents,
			IntervalDays: totalGap / validGaps,
			Count:        len(items),
		})
	}

	sort.Slice(patterns, func(i, j int) bool {
		if patterns[i].Count == patterns[j].Count {
			return patterns[i].Merchant < patterns[j].Merchant
		}
		return patterns[i].Count > patterns[j].Count
	})
	return patterns
}

func buildTransactionID(fingerprint string) string {
	if len(fingerprint) > 24 {
		return "txn-" + fingerprint[:24]
	}
	return "txn-" + fingerprint
}

func normalizeText(value string) string {
	return strings.TrimSpace(value)
}

func normalizeCategory(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return "uncategorized"
	}
	return normalized
}

func normalizeCurrency(value string) string {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	if normalized == "" {
		return "UNKNOWN"
	}
	return normalized
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func formatAmount(amount int64) string {
	return strconv.FormatInt(amount, 10)
}
