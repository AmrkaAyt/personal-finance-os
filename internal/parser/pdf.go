package parser

import (
	"bytes"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	pdf "github.com/ledongthuc/pdf"
)

var (
	pdfDatePattern   = regexp.MustCompile(`^\d{2}\.\d{2}\.(?:\d{2}|\d{4})$`)
	pdfAmountPattern = regexp.MustCompile(`^[+-]?\d+(?:\.\d+)?$`)
)

func parsePDF(payload []byte) Result {
	text, err := extractPDFText(payload)
	if err != nil {
		return buildResult("pdf_unreadable", nil)
	}

	transactions := parsePDFText(text)
	return buildResult("pdf_text", transactions)
}

func extractPDFText(payload []byte) (string, error) {
	reader, err := pdf.NewReader(bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		return "", err
	}

	textReader, err := reader.GetPlainText()
	if err != nil {
		return "", err
	}

	data, err := io.ReadAll(textReader)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func parsePDFText(text string) []Transaction {
	lines := normalizePDFLines(text)
	transactions := make([]Transaction, 0, len(lines)/4)

	for index := 0; index < len(lines); {
		if !looksLikePDFDate(lines[index]) {
			index++
			continue
		}

		transaction, nextIndex, ok := consumePDFTransaction(lines, index)
		if !ok {
			index++
			continue
		}

		transactions = append(transactions, transaction)
		index = nextIndex
	}

	return transactions
}

func consumePDFTransaction(lines []string, start int) (Transaction, int, bool) {
	occurredAt := parsePDFDate(lines[start])
	if occurredAt == nil || start+1 >= len(lines) {
		return Transaction{}, start + 1, false
	}

	amountCents, currency, ok := parsePDFAmount(lines[start+1])
	if !ok {
		return Transaction{}, start + 1, false
	}

	rawParts := []string{lines[start], lines[start+1]}
	index := start + 2

	if index < len(lines) && looksLikePDFCurrency(lines[index]) {
		if currency == "UNKNOWN" {
			currency = normalizePDFCurrency(lines[index])
		}
		rawParts = append(rawParts, lines[index])
		index++
	}

	operationParts := make([]string, 0, 2)
	for index < len(lines) {
		line := lines[index]
		if looksLikePDFDate(line) {
			break
		}
		if isSkippablePDFLine(line) {
			index++
			continue
		}
		operationParts = append(operationParts, line)
		rawParts = append(rawParts, line)
		index++
		if !shouldContinuePDFOperation(operationParts) {
			break
		}
	}

	detailParts := make([]string, 0, 3)
	for index < len(lines) {
		line := lines[index]
		if looksLikePDFDate(line) {
			break
		}
		if isSkippablePDFLine(line) {
			index++
			continue
		}
		detailParts = append(detailParts, line)
		rawParts = append(rawParts, line)
		index++
	}

	operation := normalizePDFText(strings.Join(operationParts, " "))
	detail := normalizePDFText(strings.Join(detailParts, " "))
	merchant := selectPDFMerchant(operation, detail)
	if merchant == "" {
		merchant = NormalizeMerchant(operation)
	}

	return Transaction{
		Merchant:    merchant,
		Category:    CategorizeTransaction("", merchant, operation, detail, strings.Join(rawParts, " | "), amountCents),
		Currency:    firstNonEmptyString(currency, "UNKNOWN"),
		AmountCents: amountCents,
		OccurredAt:  occurredAt,
		RawLine:     strings.Join(rawParts, " | "),
	}, index, true
}

func normalizePDFLines(text string) []string {
	rawLines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	lines := make([]string, 0, len(rawLines))
	for _, line := range rawLines {
		normalized := normalizePDFText(line)
		if normalized == "" {
			continue
		}
		lines = append(lines, normalized)
	}
	return lines
}

func normalizePDFText(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func looksLikePDFDate(value string) bool {
	return pdfDatePattern.MatchString(strings.TrimSpace(value))
}

func parsePDFDate(value string) *time.Time {
	trimmed := strings.TrimSpace(value)
	for _, layout := range []string{"02.01.2006", "02.01.06"} {
		parsed, err := time.Parse(layout, trimmed)
		if err == nil {
			utc := parsed.UTC()
			return &utc
		}
	}
	return nil
}

func parsePDFAmount(value string) (int64, string, bool) {
	currency := detectPDFCurrency(value)
	normalized := strings.ToUpper(strings.TrimSpace(value))
	replacer := strings.NewReplacer(
		" ", "",
		"\u00A0", "",
		"₸", "",
		"$", "",
		"€", "",
		"¥", "",
		"₽", "",
		"₺", "",
		"KZT", "",
		"USD", "",
		"EUR", "",
		"CNY", "",
		"RUB", "",
		"AED", "",
		"TRY", "",
	)
	normalized = replacer.Replace(normalized)
	normalized = normalizeDecimalSeparators(normalized)
	if !pdfAmountPattern.MatchString(normalized) {
		return 0, currency, false
	}

	parsed, err := strconv.ParseFloat(normalized, 64)
	if err != nil {
		return 0, currency, false
	}
	return int64(math.Round(parsed * 100)), currency, true
}

func normalizeDecimalSeparators(value string) string {
	lastDot := strings.LastIndex(value, ".")
	lastComma := strings.LastIndex(value, ",")

	switch {
	case lastDot >= 0 && lastComma >= 0:
		if lastDot > lastComma {
			value = strings.ReplaceAll(value, ",", "")
		} else {
			value = strings.ReplaceAll(value, ".", "")
			value = strings.ReplaceAll(value, ",", ".")
		}
	case lastComma >= 0:
		if strings.Count(value, ",") == 1 && len(value)-lastComma-1 <= 2 {
			value = strings.ReplaceAll(value, ",", ".")
		} else {
			value = strings.ReplaceAll(value, ",", "")
		}
	case lastDot >= 0:
		if strings.Count(value, ".") > 1 {
			head := strings.ReplaceAll(value[:lastDot], ".", "")
			value = head + value[lastDot:]
		}
	}

	return value
}

func detectPDFCurrency(value string) string {
	upper := strings.ToUpper(strings.TrimSpace(value))
	switch {
	case strings.Contains(upper, "₸"), strings.Contains(upper, "KZT"):
		return "KZT"
	case strings.Contains(upper, "$"), strings.Contains(upper, "USD"):
		return "USD"
	case strings.Contains(upper, "€"), strings.Contains(upper, "EUR"):
		return "EUR"
	case strings.Contains(upper, "¥"), strings.Contains(upper, "CNY"):
		return "CNY"
	case strings.Contains(upper, "₽"), strings.Contains(upper, "RUB"):
		return "RUB"
	case strings.Contains(upper, "AED"):
		return "AED"
	case strings.Contains(upper, "TRY"), strings.Contains(upper, "₺"):
		return "TRY"
	default:
		return "UNKNOWN"
	}
}

func looksLikePDFCurrency(value string) bool {
	switch normalizePDFCurrency(value) {
	case "KZT", "USD", "EUR", "CNY", "RUB", "AED", "TRY":
		return true
	default:
		return false
	}
}

func normalizePDFCurrency(value string) string {
	return detectPDFCurrency(strings.TrimSpace(value))
}

func shouldContinuePDFOperation(parts []string) bool {
	if len(parts) == 0 {
		return false
	}
	joined := strings.ToLower(strings.TrimSpace(strings.Join(parts, " ")))
	return joined == "сумма в" || strings.HasSuffix(joined, " в")
}

func isSkippablePDFLine(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return true
	}

	for _, prefix := range []string{
		"подлинность справки",
		"просканировав qr-код",
		"https://",
		"ao \"kaspi bank\"",
		"ао «kaspi bank»",
		"www.kaspi.kz",
		"www.bankffin.kz",
		"по курсу, установленному банком",
		"сумма в обработке. банк ожидает",
	} {
		if strings.HasPrefix(normalized, prefix) {
			return true
		}
	}

	switch normalized {
	case "дата", "сумма", "валюта", "операция", "детали", "операция детали", "операция detали":
		return true
	}
	return false
}

func selectPDFMerchant(operation, detail string) string {
	source := strings.TrimSpace(detail)
	if source == "" {
		source = strings.TrimSpace(operation)
	}
	source = strings.TrimPrefix(source, "Возврат. ")
	return NormalizeMerchant(source)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
