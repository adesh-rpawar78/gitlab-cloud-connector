package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// WebhookDestination sends batches via HTTP POST to an external webhook endpoint.
type WebhookDestination struct {
	url    string
	client *http.Client
}

func NewWebhookDestination(targetURL string) *WebhookDestination {
	return &WebhookDestination{
		url:    targetURL,
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

func (w *WebhookDestination) WriteBatch(ctx context.Context, records []Record) error {
	if w.url == "" {
		return fmt.Errorf("webhook destination URL is empty")
	}

	payload, err := json.Marshal(map[string]any{
		"event_count": len(records),
		"records":     records,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "GitLab-Cloud-Connector/1.0")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send webhook POST: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook endpoint responded with HTTP %d", resp.StatusCode)
	}

	fmt.Printf("--> [DESTINATION : WEBHOOK] Posted %d records to %s (Status: %d)\n", len(records), w.url, resp.StatusCode)
	return nil
}

func (w *WebhookDestination) Close() error {
	return nil
}
