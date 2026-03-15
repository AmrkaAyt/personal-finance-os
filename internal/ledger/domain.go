package ledger

import (
	"sort"
	"strconv"
	"sync"
	"time"
)

type Transaction struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	AccountID   string    `json:"account_id"`
	Merchant    string    `json:"merchant"`
	Category    string    `json:"category"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	OccurredAt  time.Time `json:"occurred_at"`
}

type RecurringPattern struct {
	Merchant     string `json:"merchant"`
	Category     string `json:"category"`
	AmountCents  int64  `json:"amount_cents"`
	IntervalDays int    `json:"interval_days"`
	Count        int    `json:"count"`
}

type Store struct {
	mu           sync.RWMutex
	transactions []Transaction
}

func NewStore() *Store {
	return &Store{transactions: make([]Transaction, 0, 32)}
}

func (s *Store) Add(transaction Transaction) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transactions = append(s.transactions, transaction)
}

func (s *Store) List() []Transaction {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copied := make([]Transaction, len(s.transactions))
	copy(copied, s.transactions)
	return copied
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

func formatAmount(amount int64) string {
	return strconv.FormatInt(amount, 10)
}
