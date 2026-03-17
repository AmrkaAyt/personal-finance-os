package parser

import "strings"

var categoryKeywordSets = []struct {
	category string
	keywords []string
}{
	{category: "fees", keywords: []string{
		"комиссия",
		"fee",
	}},
	{category: "transfers", keywords: []string{
		"между своими счетами",
		"на карту ",
		"по номеру счета",
		"перевод валюты freedom на счет",
		"с карты другого банка",
	}},
	{category: "income", keywords: []string{
		"salary",
		"зарплата",
	}},
	{category: "subscriptions", keywords: []string{
		"openai",
		"chatgpt",
		"apple.com/bill",
		"netflix",
		"spotify",
		"steam",
		"iqos",
	}},
	{category: "transport", keywords: []string{
		"yandex.go",
		"uber",
		"taxi",
		"jet sharing",
	}},
	{category: "groceries", keywords: []string{
		"magnum",
		"supermarket",
		"galmart",
		"vkus mart",
		"minimarket",
		"small supermarket",
	}},
	{category: "food", keywords: []string{
		"butcher",
		"buhen",
		"asau",
		"кинь-двинь",
		"yakovleva",
		"ariya hospitality",
		"coffee",
	}},
	{category: "travel", keywords: []string{
		"aviata",
		"booking",
		"air astana",
	}},
	{category: "utilities", keywords: []string{
		"телеком",
		"telecom",
		"ерц",
		"пенсионные взносы",
		"социальные отчисления",
	}},
	{category: "healthcare", keywords: []string{
		"аптека",
		"apteka",
	}},
}

func CategorizeTransaction(existingCategory, merchant, operation, detail, rawLine string, amountCents int64) string {
	category := strings.ToLower(strings.TrimSpace(existingCategory))
	if category != "" && category != "uncategorized" {
		return category
	}

	combined := strings.ToLower(strings.Join([]string{
		strings.TrimSpace(merchant),
		strings.TrimSpace(operation),
		strings.TrimSpace(detail),
		strings.TrimSpace(rawLine),
	}, " "))
	combined = strings.Join(strings.Fields(combined), " ")

	switch {
	case amountCents > 0 && strings.Contains(combined, "salary"):
		return "income"
	}

	for _, candidate := range categoryKeywordSets {
		for _, keyword := range candidate.keywords {
			if strings.Contains(combined, keyword) {
				return candidate.category
			}
		}
	}

	switch {
	case amountCents > 0 && (strings.Contains(combined, "пополнение") || strings.Contains(combined, "перевод")):
		return "transfers"
	case strings.Contains(combined, "перевод"):
		return "transfers"
	case strings.Contains(combined, "сумма в обработке"):
		return "pending"
	}

	return "uncategorized"
}
