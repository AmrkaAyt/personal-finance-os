package parser

import "testing"

func TestParseStatementCSV(t *testing.T) {
	payload := []byte("merchant,amount,currency,date,category\nNetflix,15.99,usd,2026-03-01,subscriptions\nSalary,1000,usd,2026-03-02,income\n")
	result := ParseStatement("statement.csv", payload)
	if result.Summary.Format != "csv" {
		t.Fatalf("expected csv format, got %s", result.Summary.Format)
	}
	if result.Summary.TransactionCount != 2 {
		t.Fatalf("expected 2 transactions, got %d", result.Summary.TransactionCount)
	}
	if result.Transactions[0].Merchant != "netflix" {
		t.Fatalf("expected normalized merchant netflix, got %s", result.Transactions[0].Merchant)
	}
}

func TestNormalizeMerchant(t *testing.T) {
	if got := NormalizeMerchant("  COFFEE   SHOP "); got != "coffee shop" {
		t.Fatalf("unexpected normalized merchant: %s", got)
	}
}
