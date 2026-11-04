package destination

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gitlab-cloud-connector/storage"
)

// RedisDestination pushes events into a Redis list (LPUSH) and trims it to maxItems.
type RedisDestination struct {
	client   storage.RedisClient
	listKey  string
	maxItems int
}

func NewRedisDestination(client storage.RedisClient, projectID string, maxItems int) *RedisDestination {
	if projectID == "" {
		projectID = "default"
	}
	if maxItems <= 0 {
		maxItems = 200
	}
	return &RedisDestination{
		client:   client,
		listKey:  fmt.Sprintf("gitlab:events:%s", projectID),
		maxItems: maxItems,
	}
}

func (r *RedisDestination) WriteBatch(ctx context.Context, records []Record) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for _, rec := range records {
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}

		if err := r.client.LPush(ctx, r.listKey, string(data)); err != nil {
			return fmt.Errorf("failed to LPUSH record to Redis: %w", err)
		}
	}

	_ = r.client.LTrim(ctx, r.listKey, 0, r.maxItems-1)
	return nil
}

func (r *RedisDestination) Close() error {
	return nil
}

// MultiDestination broadcasts records to multiple destinations (e.g. File + Redis).
type MultiDestination struct {
	sinks []Destination
}

func NewMultiDestination(sinks ...Destination) *MultiDestination {
	return &MultiDestination{sinks: sinks}
}

func (m *MultiDestination) WriteBatch(ctx context.Context, records []Record) error {
	for _, s := range m.sinks {
		if err := s.WriteBatch(ctx, records); err != nil {
			return err
		}
	}
	return nil
}

func (m *MultiDestination) Close() error {
	for _, s := range m.sinks {
		_ = s.Close()
	}
	return nil
}
