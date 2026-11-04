package connector

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"gitlab-cloud-connector/storage"
)

func TestParseCheckPoint(t *testing.T) {
	// Empty string returns default
	cp, err := ParseCheckPoint("")
	if err != nil {
		t.Fatalf("unexpected error parsing empty checkpoint: %v", err)
	}
	if cp.LastEventTimestamp == "" {
		t.Error("expected non-empty default timestamp")
	}

	// Valid JSON
	raw := `{"last_event_timestamp":"2026-10-24T12:00:00Z","last_event_id":555,"total_records_synced":42}`
	cp, err = ParseCheckPoint(raw)
	if err != nil {
		t.Fatalf("failed to parse valid JSON: %v", err)
	}
	if cp.LastEventTimestamp != "2026-10-24T12:00:00Z" {
		t.Errorf("unexpected timestamp: %s", cp.LastEventTimestamp)
	}
	if cp.LastEventID != 555 {
		t.Errorf("expected LastEventID 555, got %d", cp.LastEventID)
	}
	if cp.TotalRecordsSynced != 42 {
		t.Errorf("expected TotalRecordsSynced 42, got %d", cp.TotalRecordsSynced)
	}

	// Invalid JSON
	_, err = ParseCheckPoint("{invalid-json}")
	if err == nil {
		t.Error("expected error parsing invalid JSON, got nil")
	}
}

func TestRedisCheckPointStore(t *testing.T) {
	ctx := context.Background()
	// storage.NewClient connects to in-memory fallback if no real server
	redisClient := storage.NewClient("127.0.0.1:0")

	store := NewRedisCheckPointStore(redisClient, "test-project-123")

	// Initially empty
	cp, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to load initial checkpoint: %v", err)
	}
	if cp.TotalRecordsSynced != 0 {
		t.Errorf("expected 0 synced records initially, got %d", cp.TotalRecordsSynced)
	}

	// Save new checkpoint
	now := time.Now().UTC()
	toSave := CheckPoint{
		LastEventTimestamp: now.Format(time.RFC3339),
		LastEventID:        999,
		TotalRecordsSynced: 10,
	}
	if err := store.Save(ctx, toSave); err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	// Reload and verify
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to reload checkpoint: %v", err)
	}
	if loaded.LastEventID != 999 {
		t.Errorf("expected LastEventID 999, got %d", loaded.LastEventID)
	}
	if loaded.TotalRecordsSynced != 10 {
		t.Errorf("expected TotalRecordsSynced 10, got %d", loaded.TotalRecordsSynced)
	}

	// Reset
	if err := store.Reset(ctx); err != nil {
		t.Fatalf("failed to reset store: %v", err)
	}
	cleared, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to load after reset: %v", err)
	}
	if cleared.TotalRecordsSynced != 0 {
		t.Errorf("expected 0 after reset, got %d", cleared.TotalRecordsSynced)
	}
}

func TestFileCheckPointStore(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "checkpoint.json")

	store := NewFileCheckPointStore(filePath)

	// Initially empty file
	cp, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to load initial checkpoint: %v", err)
	}
	if cp.TotalRecordsSynced != 0 {
		t.Errorf("expected 0 synced records initially, got %d", cp.TotalRecordsSynced)
	}

	// Save
	toSave := CheckPoint{
		LastEventTimestamp: "2026-10-24T05:00:00Z",
		LastEventID:        1234,
		TotalRecordsSynced: 25,
	}
	if err := store.Save(ctx, toSave); err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	// Reload
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to reload checkpoint: %v", err)
	}
	if loaded.LastEventID != 1234 {
		t.Errorf("expected LastEventID 1234, got %d", loaded.LastEventID)
	}
	if loaded.TotalRecordsSynced != 25 {
		t.Errorf("expected TotalRecordsSynced 25, got %d", loaded.TotalRecordsSynced)
	}

	// Reset
	if err := store.Reset(ctx); err != nil {
		t.Fatalf("failed to reset store: %v", err)
	}
	afterReset, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("failed to load after reset: %v", err)
	}
	if afterReset.TotalRecordsSynced != 0 {
		t.Errorf("expected 0 after reset, got %d", afterReset.TotalRecordsSynced)
	}
}
