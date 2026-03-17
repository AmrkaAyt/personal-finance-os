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

func TestCategorizeTransactionHeuristics(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		merchant  string
		operation string
		detail    string
		amount    int64
		want      string
	}{
		{name: "transfer", operation: "Перевод", detail: "На карту Freedom Finance Bank*6514", amount: -7500000, want: "transfers"},
		{name: "subscription", merchant: "OPENAI *CHATGPT SUBSCR", amount: -1171136, want: "subscriptions"},
		{name: "transport", merchant: "YANDEX.GO ALMATY KZ", amount: -149000, want: "transport"},
		{name: "healthcare", merchant: "АПТЕКА ECOM", amount: -881200, want: "healthcare"},
		{name: "travel", merchant: "TOO \"Aviata\"", amount: -1846200, want: "travel"},
		{name: "fees", operation: "Разное", detail: "Комиссия за перевод на карту др. банка", amount: -14900, want: "fees"},
		{name: "pending fallback", operation: "Сумма в обработке", detail: "Unknown merchant", amount: -82000, want: "pending"},
	}

	for _, tc := range cases {
		if got := CategorizeTransaction("", tc.merchant, tc.operation, tc.detail, "", tc.amount); got != tc.want {
			t.Fatalf("%s: category = %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestParsePDFTextKaspiStatement(t *testing.T) {
	t.Parallel()

	text := `
Дата
Сумма
Операция
Детали
16.03.26
- 3 300,00 ₸
Перевод
Даниил Х.
15.03.26
- 10 500,00 ₸
Покупка
ИП КИНЬ-ДВИНЬ
07.03.26
- 799,00 ₸
Покупка
APPLE.COM/BILL
03.03.26
- 11 711,36 ₸
Покупка
OPENAI *CHATGPT SUBSCR
`

	transactions := parsePDFText(text)
	if len(transactions) != 4 {
		t.Fatalf("len(transactions) = %d, want 4", len(transactions))
	}
	if transactions[0].Currency != "KZT" {
		t.Fatalf("currency = %s, want KZT", transactions[0].Currency)
	}
	if transactions[1].Category != "food" {
		t.Fatalf("category = %s, want food", transactions[1].Category)
	}
	if transactions[2].Category != "subscriptions" {
		t.Fatalf("category = %s, want subscriptions", transactions[2].Category)
	}
	if transactions[3].Merchant != "openai *chatgpt subscr" {
		t.Fatalf("merchant = %s, want normalized OpenAI merchant", transactions[3].Merchant)
	}
}

func TestParsePDFTextFreedomStatement(t *testing.T) {
	t.Parallel()

	text := `
Дата
Сумма
Валюта
Операция
Детали
16.03.2026
-3,000.00 ₸
KZT
Сумма в
обработке
IP IMAN ASTANA KZ
14.03.2026
-1,490.00 ₸
KZT
Покупка
YANDEX.GO ALMATY KZ
13.03.2026
-18,462.00 ₸
KZT
Платеж
ТОО "Aviata" За оплату билета
`

	transactions := parsePDFText(text)
	if len(transactions) != 3 {
		t.Fatalf("len(transactions) = %d, want 3", len(transactions))
	}
	if transactions[0].Merchant != "ip iman astana kz" {
		t.Fatalf("merchant = %s, want ip iman astana kz", transactions[0].Merchant)
	}
	if transactions[1].Category != "transport" {
		t.Fatalf("category = %s, want transport", transactions[1].Category)
	}
	if transactions[2].Category != "travel" {
		t.Fatalf("category = %s, want travel", transactions[2].Category)
	}
	if transactions[2].AmountCents != -1846200 {
		t.Fatalf("amount_cents = %d, want -1846200", transactions[2].AmountCents)
	}
}
