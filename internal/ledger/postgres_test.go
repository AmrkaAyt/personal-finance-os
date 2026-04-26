package ledger

import "testing"

func TestCollectDerivedCategoriesSkipsSystemAndScopesByUser(t *testing.T) {
	t.Parallel()

	items := collectDerivedCategories([]Transaction{
		{UserID: "user-a", Category: "food"},
		{UserID: "user-a", Category: "gifts"},
		{UserID: "user-a", Category: "gifts"},
		{UserID: "user-b", Category: "gifts"},
		{UserID: "user-b", Category: "travel"},
		{UserID: "", Category: "custom"},
		{UserID: "user-c", Category: ""},
	})

	if len(items) != 2 {
		t.Fatalf("expected 2 derived categories, got %d", len(items))
	}
	if items[0].userID != "user-a" || items[0].name != "gifts" {
		t.Fatalf("unexpected first derived category: %+v", items[0])
	}
	if items[1].userID != "user-b" || items[1].name != "gifts" {
		t.Fatalf("unexpected second derived category: %+v", items[1])
	}
}
