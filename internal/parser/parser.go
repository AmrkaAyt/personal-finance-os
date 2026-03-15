package parser

import (
	"bytes"
	"encoding/csv"
	"strconv"
	"strings"
	"time"
)

type Transaction struct {
	Merchant    string     `bson:"merchant" json:"merchant"`
	Category    string     `bson:"category" json:"category"`
	Currency    string     `bson:"currency" json:"currency"`
	AmountCents int64      `bson:"amount_cents" json:"amount_cents"`
	OccurredAt  *time.Time `bson:"occurred_at,omitempty" json:"occurred_at,omitempty"`
	RawLine     string     `bson:"raw_line" json:"raw_line"`
}

type Summary struct {
	Format           string   `bson:"format" json:"format"`
	TransactionCount int      `bson:"transaction_count" json:"transaction_count"`
	Merchants        []string `bson:"merchants" json:"merchants"`
	TotalDebitCents  int64    `bson:"total_debit_cents" json:"total_debit_cents"`
	TotalCreditCents int64    `bson:"total_credit_cents" json:"total_credit_cents"`
}

type Result struct {
	Summary      Summary       `bson:"summary" json:"summary"`
	Transactions []Transaction `bson:"transactions" json:"transactions"`
}

func ParseStatement(filename string, payload []byte) Result {
	if looksLikeCSV(filename, payload) {
		return parseCSV(payload)
	}
	return parseLines(payload)
}

func NormalizeMerchant(value string) string {
	parts := strings.Fields(strings.ToLower(value))
	return strings.Join(parts, " ")
}

func looksLikeCSV(filename string, payload []byte) bool {
	name := strings.ToLower(filename)
	if strings.HasSuffix(name, ".csv") {
		return true
	}
	return bytes.Count(payload, []byte(",")) >= 2
}

func parseCSV(payload []byte) Result {
	reader := csv.NewReader(bytes.NewReader(payload))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil || len(records) == 0 {
		return parseLines(payload)
	}

	start := 0
	columns := []string{"merchant", "amount", "currency", "occurred_at", "category"}
	if hasHeader(records[0]) {
		columns = normalizeColumns(records[0])
		start = 1
	}

	transactions := make([]Transaction, 0, len(records)-start)
	for _, record := range records[start:] {
		if len(record) == 0 || isEmptyRecord(record) {
			continue
		}
		transactions = append(transactions, recordToTransaction(columns, record))
	}
	return buildResult("csv", transactions)
}

func parseLines(payload []byte) Result {
	lines := strings.Split(string(payload), "\n")
	transactions := make([]Transaction, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		transactions = append(transactions, Transaction{
			Merchant: NormalizeMerchant(trimmed),
			Category: "uncategorized",
			Currency: "UNKNOWN",
			RawLine:  trimmed,
		})
	}
	return buildResult("plain_text", transactions)
}

func buildResult(format string, transactions []Transaction) Result {
	merchantSet := make(map[string]struct{})
	merchants := make([]string, 0, len(transactions))
	var totalDebit int64
	var totalCredit int64
	for _, transaction := range transactions {
		if _, ok := merchantSet[transaction.Merchant]; !ok && transaction.Merchant != "" {
			merchantSet[transaction.Merchant] = struct{}{}
			merchants = append(merchants, transaction.Merchant)
		}
		if transaction.AmountCents < 0 {
			totalDebit += -transaction.AmountCents
		} else {
			totalCredit += transaction.AmountCents
		}
	}
	return Result{
		Summary: Summary{
			Format:           format,
			TransactionCount: len(transactions),
			Merchants:        merchants,
			TotalDebitCents:  totalDebit,
			TotalCreditCents: totalCredit,
		},
		Transactions: transactions,
	}
}

func recordToTransaction(columns []string, record []string) Transaction {
	transaction := Transaction{
		Category: "uncategorized",
		Currency: "USD",
		RawLine:  strings.Join(record, ","),
	}
	for index, value := range record {
		if index >= len(columns) {
			continue
		}
		normalized := strings.TrimSpace(value)
		switch columns[index] {
		case "merchant":
			transaction.Merchant = NormalizeMerchant(normalized)
		case "category":
			if normalized != "" {
				transaction.Category = strings.ToLower(normalized)
			}
		case "currency":
			if normalized != "" {
				transaction.Currency = strings.ToUpper(normalized)
			}
		case "amount":
			transaction.AmountCents = parseAmountCents(normalized)
		case "occurred_at":
			if parsed := parseDate(normalized); parsed != nil {
				transaction.OccurredAt = parsed
			}
		}
	}
	if transaction.Merchant == "" {
		transaction.Merchant = NormalizeMerchant(transaction.RawLine)
	}
	return transaction
}

func hasHeader(record []string) bool {
	for _, field := range record {
		normalized := strings.ToLower(strings.TrimSpace(field))
		switch normalized {
		case "merchant", "description", "amount", "currency", "date", "occurred_at", "category":
			return true
		}
	}
	return false
}

func normalizeColumns(record []string) []string {
	columns := make([]string, 0, len(record))
	for _, field := range record {
		normalized := strings.ToLower(strings.TrimSpace(field))
		switch normalized {
		case "merchant", "description":
			columns = append(columns, "merchant")
		case "amount", "sum":
			columns = append(columns, "amount")
		case "currency":
			columns = append(columns, "currency")
		case "date", "occurred_at":
			columns = append(columns, "occurred_at")
		case "category":
			columns = append(columns, "category")
		default:
			columns = append(columns, normalized)
		}
	}
	return columns
}

func isEmptyRecord(record []string) bool {
	for _, field := range record {
		if strings.TrimSpace(field) != "" {
			return false
		}
	}
	return true
}

func parseAmountCents(value string) int64 {
	normalized := strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(value), " ", ""), ",", ".")
	if normalized == "" {
		return 0
	}
	parsed, err := strconv.ParseFloat(normalized, 64)
	if err != nil {
		return 0
	}
	return int64(parsed * 100)
}

func parseDate(value string) *time.Time {
	layouts := []string{time.RFC3339, "2006-01-02", "02.01.2006", "02/01/2006"}
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, strings.TrimSpace(value))
		if err == nil {
			utc := parsed.UTC()
			return &utc
		}
	}
	return nil
}
