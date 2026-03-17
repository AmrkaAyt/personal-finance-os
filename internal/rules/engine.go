package rules

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"personal-finance-os/internal/ledger"
)

const (
	AlertTypeLargeTransaction = "large_transaction"
	AlertTypeNewMerchant      = "new_merchant"
	AlertTypeBudgetWarning    = "budget_warning"
	AlertTypeBudgetCritical   = "budget_critical"
)

type Config struct {
	LargeTransactionThresholdCents  int64
	NewMerchantThresholdCents       int64
	BudgetWarningRatio              float64
	BudgetCriticalRatio             float64
	BudgetLimitsCents               map[string]int64
	DefaultChatID                   string
	NotifyImportedLargeTransactions bool
	NotifyImportedNewMerchants      bool
}

type Alert struct {
	ID                string    `json:"id"`
	UserID            string    `json:"user_id"`
	Type              string    `json:"type"`
	Severity          string    `json:"severity"`
	Message           string    `json:"message"`
	Category          string    `json:"category,omitempty"`
	Merchant          string    `json:"merchant,omitempty"`
	AmountCents       int64     `json:"amount_cents,omitempty"`
	CurrentSpendCents int64     `json:"current_spend_cents,omitempty"`
	LimitCents        int64     `json:"limit_cents,omitempty"`
	TransactionID     string    `json:"transaction_id,omitempty"`
	SourceImportID    string    `json:"source_import_id,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
}

type NotificationJob struct {
	Alert     Alert     `json:"alert"`
	Channel   string    `json:"channel"`
	ChatID    string    `json:"chat_id,omitempty"`
	Attempt   int       `json:"attempt"`
	CreatedAt time.Time `json:"created_at"`
	LastError string    `json:"last_error,omitempty"`
	IsDryRun  bool      `json:"is_dry_run"`
}

type StateStore interface {
	AddCategorySpend(ctx context.Context, userID, month, category string, delta int64) (int64, error)
	MarkThresholdTriggered(ctx context.Context, userID, month, category, level string) (bool, error)
	MarkMerchantSeen(ctx context.Context, userID, merchant string) (bool, error)
	MarkAlertIssued(ctx context.Context, userID, alertID string) (bool, error)
	MarkTransactionProcessed(ctx context.Context, userID, transactionID string) (bool, error)
}

type Engine struct {
	cfg   Config
	store StateStore
}

func NewEngine(cfg Config, store StateStore) *Engine {
	if cfg.BudgetWarningRatio <= 0 {
		cfg.BudgetWarningRatio = 0.8
	}
	if cfg.BudgetCriticalRatio <= 0 {
		cfg.BudgetCriticalRatio = 1.0
	}
	if cfg.BudgetLimitsCents == nil {
		cfg.BudgetLimitsCents = map[string]int64{}
	}
	return &Engine{cfg: cfg, store: store}
}

func (e *Engine) Evaluate(ctx context.Context, event ledger.TransactionUpsertedEvent) ([]Alert, error) {
	alerts := make([]Alert, 0, 3)
	expenseAmount := normalizeExpenseAmount(event)
	if expenseAmount == 0 {
		return alerts, nil
	}
	if strings.TrimSpace(event.TransactionID) != "" {
		shouldProcess, err := e.store.MarkTransactionProcessed(ctx, event.UserID, event.TransactionID)
		if err != nil {
			return nil, err
		}
		if !shouldProcess {
			return alerts, nil
		}
	}

	createdAt := time.Now().UTC()
	if event.OccurredAt.IsZero() {
		event.OccurredAt = createdAt
	}
	category := normalizeCategory(event.Category)
	merchant := normalizeMerchant(event.Merchant)
	isImported := isImportedTransaction(event.SourceImportID)

	if e.cfg.LargeTransactionThresholdCents > 0 && expenseAmount >= e.cfg.LargeTransactionThresholdCents && (!isImported || e.cfg.NotifyImportedLargeTransactions) {
		alert, emitted, err := e.emitIfNew(ctx, AlertInput{
			UserID:         event.UserID,
			Type:           AlertTypeLargeTransaction,
			Severity:       "warning",
			Message:        fmt.Sprintf("Large transaction detected for %s: %s %s", merchant, formatCents(expenseAmount), event.Currency),
			Category:       category,
			Merchant:       merchant,
			AmountCents:    expenseAmount,
			TransactionID:  event.TransactionID,
			SourceImportID: event.SourceImportID,
			CreatedAt:      createdAt,
		})
		if err != nil {
			return nil, err
		}
		if emitted {
			alerts = append(alerts, alert)
		}
	}

	if merchant != "" {
		isNewMerchant, err := e.store.MarkMerchantSeen(ctx, event.UserID, merchant)
		if err != nil {
			return nil, err
		}
		if isNewMerchant && e.cfg.NewMerchantThresholdCents > 0 && expenseAmount >= e.cfg.NewMerchantThresholdCents && (!isImported || e.cfg.NotifyImportedNewMerchants) {
			alert, emitted, err := e.emitIfNew(ctx, AlertInput{
				UserID:         event.UserID,
				Type:           AlertTypeNewMerchant,
				Severity:       "warning",
				Message:        fmt.Sprintf("New merchant detected: %s for %s %s", merchant, formatCents(expenseAmount), event.Currency),
				Category:       category,
				Merchant:       merchant,
				AmountCents:    expenseAmount,
				TransactionID:  event.TransactionID,
				SourceImportID: event.SourceImportID,
				CreatedAt:      createdAt,
			})
			if err != nil {
				return nil, err
			}
			if emitted {
				alerts = append(alerts, alert)
			}
		}
	}

	limit := e.cfg.BudgetLimitsCents[category]
	if limit <= 0 {
		return alerts, nil
	}

	month := event.OccurredAt.UTC().Format("2006-01")
	total, err := e.store.AddCategorySpend(ctx, event.UserID, month, category, expenseAmount)
	if err != nil {
		return nil, err
	}

	criticalThreshold := int64(math.Ceil(float64(limit) * e.cfg.BudgetCriticalRatio))
	if criticalThreshold <= 0 {
		criticalThreshold = limit
	}
	if total >= criticalThreshold {
		shouldEmit, err := e.store.MarkThresholdTriggered(ctx, event.UserID, month, category, "critical")
		if err != nil {
			return nil, err
		}
		if shouldEmit {
			alert, emitted, err := e.emitIfNew(ctx, AlertInput{
				UserID:            event.UserID,
				Type:              AlertTypeBudgetCritical,
				Severity:          "critical",
				Message:           fmt.Sprintf("Budget exceeded for %s: %s / %s cents", category, strconv.FormatInt(total, 10), strconv.FormatInt(limit, 10)),
				Category:          category,
				Merchant:          merchant,
				AmountCents:       expenseAmount,
				CurrentSpendCents: total,
				LimitCents:        limit,
				TransactionID:     event.TransactionID,
				SourceImportID:    event.SourceImportID,
				CreatedAt:         createdAt,
			})
			if err != nil {
				return nil, err
			}
			if emitted {
				alerts = append(alerts, alert)
			}
		}
		return alerts, nil
	}

	warningThreshold := int64(math.Ceil(float64(limit) * e.cfg.BudgetWarningRatio))
	if warningThreshold <= 0 {
		warningThreshold = limit
	}
	if total >= warningThreshold {
		shouldEmit, err := e.store.MarkThresholdTriggered(ctx, event.UserID, month, category, "warning")
		if err != nil {
			return nil, err
		}
		if shouldEmit {
			alert, emitted, err := e.emitIfNew(ctx, AlertInput{
				UserID:            event.UserID,
				Type:              AlertTypeBudgetWarning,
				Severity:          "warning",
				Message:           fmt.Sprintf("Budget threshold reached for %s: %s / %s cents", category, strconv.FormatInt(total, 10), strconv.FormatInt(limit, 10)),
				Category:          category,
				Merchant:          merchant,
				AmountCents:       expenseAmount,
				CurrentSpendCents: total,
				LimitCents:        limit,
				TransactionID:     event.TransactionID,
				SourceImportID:    event.SourceImportID,
				CreatedAt:         createdAt,
			})
			if err != nil {
				return nil, err
			}
			if emitted {
				alerts = append(alerts, alert)
			}
		}
	}

	return alerts, nil
}

func NewNotificationJob(alert Alert, chatID string) NotificationJob {
	return NotificationJob{
		Alert:     alert,
		Channel:   "telegram",
		ChatID:    strings.TrimSpace(chatID),
		Attempt:   0,
		CreatedAt: time.Now().UTC(),
		IsDryRun:  strings.TrimSpace(chatID) == "",
	}
}

func ParseBudgetLimits(raw string, fallback map[string]int64) map[string]int64 {
	if strings.TrimSpace(raw) == "" {
		return cloneBudgetMap(fallback)
	}

	limits := make(map[string]int64)
	for _, item := range strings.Split(raw, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			continue
		}
		category := normalizeCategory(parts[0])
		value, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || value <= 0 {
			continue
		}
		limits[category] = value
	}
	if len(limits) == 0 {
		return cloneBudgetMap(fallback)
	}
	return limits
}

func normalizeExpenseAmount(event ledger.TransactionUpsertedEvent) int64 {
	switch normalizeCategory(event.Category) {
	case "income", "transfers", "pending":
		return 0
	}
	if event.AmountCents < 0 {
		return -event.AmountCents
	}
	return event.AmountCents
}

type AlertInput struct {
	UserID            string
	Type              string
	Severity          string
	Message           string
	Category          string
	Merchant          string
	AmountCents       int64
	CurrentSpendCents int64
	LimitCents        int64
	TransactionID     string
	SourceImportID    string
	CreatedAt         time.Time
}

func newAlert(input AlertInput) Alert {
	if input.CreatedAt.IsZero() {
		input.CreatedAt = time.Now().UTC()
	}
	return Alert{
		ID:                buildAlertID(input),
		UserID:            strings.TrimSpace(input.UserID),
		Type:              strings.TrimSpace(input.Type),
		Severity:          strings.TrimSpace(input.Severity),
		Message:           strings.TrimSpace(input.Message),
		Category:          normalizeCategory(input.Category),
		Merchant:          normalizeMerchant(input.Merchant),
		AmountCents:       input.AmountCents,
		CurrentSpendCents: input.CurrentSpendCents,
		LimitCents:        input.LimitCents,
		TransactionID:     strings.TrimSpace(input.TransactionID),
		SourceImportID:    strings.TrimSpace(input.SourceImportID),
		CreatedAt:         input.CreatedAt.UTC(),
	}
}

func buildAlertID(input AlertInput) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(input.UserID),
		strings.TrimSpace(input.Type),
		strings.TrimSpace(input.TransactionID),
		strconv.FormatInt(input.CurrentSpendCents, 10),
		strconv.FormatInt(input.LimitCents, 10),
		strconv.FormatInt(input.AmountCents, 10),
		strings.TrimSpace(input.SourceImportID),
	}, "|")))
	return "alert-" + hex.EncodeToString(sum[:8])
}

func (e *Engine) emitIfNew(ctx context.Context, input AlertInput) (Alert, bool, error) {
	alert := newAlert(input)
	created, err := e.store.MarkAlertIssued(ctx, alert.UserID, alert.ID)
	if err != nil {
		return Alert{}, false, err
	}
	return alert, created, nil
}

func normalizeCategory(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return "uncategorized"
	}
	return normalized
}

func isImportedTransaction(sourceImportID string) bool {
	normalized := strings.TrimSpace(strings.ToLower(sourceImportID))
	return normalized != "" && normalized != "manual"
}

func normalizeMerchant(value string) string {
	return strings.TrimSpace(strings.ToLower(value))
}

func cloneBudgetMap(source map[string]int64) map[string]int64 {
	cloned := make(map[string]int64, len(source))
	for key, value := range source {
		cloned[normalizeCategory(key)] = value
	}
	return cloned
}

func formatCents(value int64) string {
	return fmt.Sprintf("%.2f", float64(value)/100)
}
