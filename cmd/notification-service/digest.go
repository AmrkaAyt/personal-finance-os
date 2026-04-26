package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"personal-finance-os/internal/notificationdigest"
	"personal-finance-os/internal/platform/rabbitmq"
	"personal-finance-os/internal/rules"
)

func (s *service) flushDigests(ctx context.Context, _ *slog.Logger) error {
	if !s.digestEnabled {
		s.logger.Info("notification digest disabled")
		return nil
	}

	channel, err := rabbitmq.OpenChannel(s.rabbitConn)
	if err != nil {
		return err
	}
	defer func() {
		_ = channel.Close()
	}()
	if err := rabbitmq.DeclareWorkQueue(channel, s.queue); err != nil {
		return err
	}

	ticker := time.NewTicker(s.digestPollInterval)
	defer ticker.Stop()
	s.logger.Info("notification digest flusher ready", "window", s.digestWindow.String(), "poll_interval", s.digestPollInterval.String(), "max_items", s.digestMaxItems)

	for {
		if err := s.flushDueDigests(ctx, channel); err != nil {
			s.logger.Error("notification digest flush failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *service) flushDueDigests(ctx context.Context, channel *amqp.Channel) error {
	operationCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	keys, err := s.digestStore.DueKeys(operationCtx, time.Now().UTC(), 100)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := s.publishDigest(operationCtx, channel, key); err != nil {
			return err
		}
	}
	return nil
}

func (s *service) publishDigest(ctx context.Context, channel *amqp.Channel, key string) error {
	digest, ok, err := s.digestStore.Take(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	job := s.buildDigestNotificationJob(digest)
	if err := rabbitmq.PublishJSON(ctx, channel, s.queue, job); err != nil {
		if restoreErr := s.restoreDigest(ctx, digest); restoreErr != nil {
			return fmt.Errorf("publish digest: %w; restore digest: %v", err, restoreErr)
		}
		return err
	}

	s.logger.Info("notification digest published", "digest_key", digest.Key, "items", totalDigestItems(digest), "source_import_id", digest.SourceImportID)
	return nil
}

func (s *service) restoreDigest(ctx context.Context, digest notificationdigest.Digest) error {
	dueAt := time.Now().UTC().Add(s.digestWindow)
	typeCounts := make(map[string]int, len(digest.Items))
	for _, item := range digest.Items {
		typeCounts[item.Type]++
		job := rules.NotificationJob{
			Alert: rules.Alert{
				ID:             item.AlertID,
				UserID:         digest.UserID,
				Type:           item.Type,
				Severity:       item.Severity,
				Message:        item.Message,
				Category:       item.Category,
				Merchant:       item.Merchant,
				AmountCents:    item.AmountCents,
				TransactionID:  item.TransactionID,
				SourceImportID: item.SourceImportID,
				CreatedAt:      item.NotificationAt,
			},
			Channel:   firstNonEmpty(digest.Channel, "telegram"),
			ChatID:    digest.ChatID,
			CreatedAt: item.NotificationAt,
		}
		if _, err := s.digestStore.Append(ctx, digest.Key, job, dueAt, s.digestMaxItems); err != nil {
			return err
		}
	}
	for alertType, total := range digest.Counts {
		missing := total - typeCounts[alertType]
		for i := 0; i < missing; i++ {
			job := rules.NotificationJob{
				Alert: rules.Alert{
					ID:             fmt.Sprintf("%s-restore-%s-%d", digest.Key, alertType, i),
					UserID:         digest.UserID,
					Type:           alertType,
					Severity:       digestSeverity(digest),
					Message:        alertType,
					SourceImportID: digest.SourceImportID,
					CreatedAt:      time.Now().UTC(),
				},
				Channel:   firstNonEmpty(digest.Channel, "telegram"),
				ChatID:    digest.ChatID,
				CreatedAt: time.Now().UTC(),
			}
			if _, err := s.digestStore.Append(ctx, digest.Key, job, dueAt, s.digestMaxItems); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *service) enqueueDigest(ctx context.Context, job rules.NotificationJob) (notificationdigest.Digest, error) {
	return s.enqueueDigestAt(ctx, job, time.Time{})
}

func (s *service) enqueueDigestAt(ctx context.Context, job rules.NotificationJob, dueAt time.Time) (notificationdigest.Digest, error) {
	now := time.Now().UTC()
	if dueAt.IsZero() || !dueAt.After(now) {
		dueAt = now.Add(s.digestWindow)
	}
	key := s.digestKey(job, now)
	return s.digestStore.Append(ctx, key, s.normalizeDigestJob(job), dueAt, s.digestMaxItems)
}

func (s *service) pendingDigestCount(ctx context.Context) (int64, error) {
	if s.digestStore == nil {
		return 0, nil
	}
	return s.digestStore.PendingCount(ctx)
}

func (s *service) shouldBatch(job rules.NotificationJob) bool {
	if !s.digestEnabled {
		return false
	}
	if strings.TrimSpace(job.Channel) != "" && strings.TrimSpace(job.Channel) != "telegram" {
		return false
	}
	if job.Attempt > 0 || job.IsDryRun {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(job.Alert.Severity), "critical") {
		return false
	}
	switch strings.TrimSpace(job.Alert.Type) {
	case "", "digest", "demo", "telegram_command", "telegram_link_confirmed":
		return false
	default:
		return true
	}
}

func (s *service) digestKey(job rules.NotificationJob, now time.Time) string {
	userID := firstNonEmpty(strings.TrimSpace(job.Alert.UserID), "anonymous")
	chatID := firstNonEmpty(strings.TrimSpace(job.ChatID), strings.TrimSpace(s.defaultChatID), "default")
	if sourceImportID := strings.TrimSpace(job.Alert.SourceImportID); sourceImportID != "" {
		return fmt.Sprintf("telegram:%s:%s:import:%s", userID, chatID, sourceImportID)
	}
	bucket := now.UTC().Truncate(s.digestWindow).Format(time.RFC3339)
	return fmt.Sprintf("telegram:%s:%s:window:%s", userID, chatID, bucket)
}

func (s *service) normalizeDigestJob(job rules.NotificationJob) rules.NotificationJob {
	job.Channel = firstNonEmpty(strings.TrimSpace(job.Channel), "telegram")
	job.ChatID = firstNonEmpty(strings.TrimSpace(job.ChatID), strings.TrimSpace(s.defaultChatID))
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}
	if job.Alert.CreatedAt.IsZero() {
		job.Alert.CreatedAt = job.CreatedAt
	}
	return job
}

func (s *service) buildDigestNotificationJob(digest notificationdigest.Digest) rules.NotificationJob {
	now := time.Now().UTC()
	return rules.NotificationJob{
		Alert: rules.Alert{
			ID:             newDigestAlertID(digest, now),
			UserID:         digest.UserID,
			Type:           "digest",
			Severity:       digestSeverity(digest),
			Message:        buildTelegramDigestText(digest, s.digestWindow),
			SourceImportID: digest.SourceImportID,
			CreatedAt:      now,
		},
		Channel:   firstNonEmpty(digest.Channel, "telegram"),
		ChatID:    digest.ChatID,
		CreatedAt: now,
	}
}

func buildTelegramDigestText(digest notificationdigest.Digest, window time.Duration) string {
	title := fmt.Sprintf("Сводка алертов за %s.", window.String())
	if strings.TrimSpace(digest.SourceImportID) != "" {
		title = "Сводка алертов по импортированной выписке."
	}

	lines := []string{
		title,
		fmt.Sprintf("Всего алертов: %d", totalDigestItems(digest)),
	}
	if strings.TrimSpace(digest.SourceImportID) != "" {
		lines = append(lines, "Import ID: "+strings.TrimSpace(digest.SourceImportID))
	}

	typeKeys := make([]string, 0, len(digest.Counts))
	for alertType := range digest.Counts {
		typeKeys = append(typeKeys, alertType)
	}
	sort.Strings(typeKeys)
	if len(typeKeys) > 0 {
		lines = append(lines, "По типам:")
		for _, alertType := range typeKeys {
			lines = append(lines, fmt.Sprintf("- %s: %d", localizeAlertType(alertType), digest.Counts[alertType]))
		}
	}

	merchants := uniqueDigestMerchants(digest.Items, 5)
	if len(merchants) > 0 {
		lines = append(lines, "Мерчанты: "+strings.Join(merchants, ", "))
	}

	if amounts := summarizeDigestAmounts(digest.Items); amounts != "" {
		lines = append(lines, "Примеры сумм: "+amounts)
	}
	return strings.Join(lines, "\n")
}

func localizeAlertType(alertType string) string {
	switch strings.TrimSpace(alertType) {
	case rules.AlertTypeLargeTransaction:
		return "Крупная трата"
	case rules.AlertTypeNewMerchant:
		return "Новый мерчант"
	case rules.AlertTypeBudgetWarning:
		return "Предупреждение по бюджету"
	case rules.AlertTypeBudgetCritical:
		return "Критичный бюджет"
	case "digest":
		return "Сводка"
	default:
		return strings.TrimSpace(alertType)
	}
}

func digestSeverity(digest notificationdigest.Digest) string {
	severity := "info"
	for _, item := range digest.Items {
		switch strings.ToLower(strings.TrimSpace(item.Severity)) {
		case "critical":
			return "critical"
		case "warning":
			severity = "warning"
		}
	}
	return severity
}

func totalDigestItems(digest notificationdigest.Digest) int {
	total := 0
	for _, count := range digest.Counts {
		total += count
	}
	if total == 0 {
		total = len(digest.Items)
	}
	return total
}

func uniqueDigestMerchants(items []notificationdigest.Item, limit int) []string {
	if limit <= 0 {
		limit = 5
	}
	result := make([]string, 0, limit)
	seen := make(map[string]struct{}, limit)
	for _, item := range items {
		merchant := strings.TrimSpace(item.Merchant)
		if merchant == "" {
			continue
		}
		if _, ok := seen[merchant]; ok {
			continue
		}
		seen[merchant] = struct{}{}
		result = append(result, merchant)
		if len(result) >= limit {
			break
		}
	}
	return result
}

func summarizeDigestAmounts(items []notificationdigest.Item) string {
	if len(items) == 0 {
		return ""
	}
	values := make([]string, 0, 3)
	for _, item := range items {
		if item.AmountCents == 0 {
			continue
		}
		values = append(values, formatMinorAmount(item.AmountCents))
		if len(values) >= 3 {
			break
		}
	}
	return strings.Join(values, ", ")
}

func formatMinorAmount(value int64) string {
	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}
	return fmt.Sprintf("%s%d.%02d", sign, value/100, value%100)
}

func newDigestAlertID(digest notificationdigest.Digest, now time.Time) string {
	sum := sha256.Sum256([]byte(digest.Key + "|" + now.UTC().Format(time.RFC3339Nano)))
	return "digest-" + hex.EncodeToString(sum[:8])
}
