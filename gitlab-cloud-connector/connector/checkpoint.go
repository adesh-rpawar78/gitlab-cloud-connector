package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gitlab-cloud-connector/storage"
)

// CheckPoint tracks the resume cursor for the GitLab project.
//
// Pattern Note: In cloud-http-connector, checkpoint.go stores the watermark
// (timestamp and ID) of the last event processed. This ensures that across
// container restarts or recurring cron cycles, only new/unseen events are fetched.
type CheckPoint struct {
	LastEventTimestamp string    `json:"last_event_timestamp"` // RFC3339 / ISO 8601 (e.g. 2026-10-24T00:00:00Z)
	LastEventID        int64     `json:"last_event_id"`        // Highest event ID seen (tie-breaker)
	LastSyncedAt       time.Time `json:"last_synced_at"`       // Local clock time when sync completed
	TotalRecordsSynced int64     `json:"total_records_synced"` // Cumulative metrics counter
}

// String serializes the checkpoint into a JSON string.
func (cp CheckPoint) String() (string, error) {
	bytes, err := json.Marshal(cp)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ParseCheckPoint parses a raw JSON string into a structured CheckPoint.
func ParseCheckPoint(data string) (CheckPoint, error) {
	if data == "" {
		// Default starting checkpoint: 7 days ago
		return CheckPoint{
			LastEventTimestamp: time.Now().Add(-7 * 24 * time.Hour).UTC().Format(time.RFC3339),
			LastSyncedAt:       time.Now().UTC(),
			TotalRecordsSynced: 0,
		}, nil
	}
	var cp CheckPoint
	if err := json.Unmarshal([]byte(data), &cp); err != nil {
		return cp, fmt.Errorf("failed to parse checkpoint JSON: %w", err)
	}
	return cp, nil
}

// StateStore defines the abstraction for persisting and reading checkpoints.
type StateStore interface {
	Load(ctx context.Context) (CheckPoint, error)
	Save(ctx context.Context, cp CheckPoint) error
	Reset(ctx context.Context) error
}

// =========================================================================
// Redis-backed Checkpoint Store
// =========================================================================

type RedisCheckPointStore struct {
	client   storage.RedisClient
	redisKey string
}

func NewRedisCheckPointStore(client storage.RedisClient, projectID string) *RedisCheckPointStore {
	return &RedisCheckPointStore{
		client:   client,
		redisKey: fmt.Sprintf("gitlab:checkpoint:%s", projectID),
	}
}

func (r *RedisCheckPointStore) Load(ctx context.Context) (CheckPoint, error) {
	val, err := r.client.Get(ctx, r.redisKey)
	if err != nil {
		return CheckPoint{}, fmt.Errorf("failed to read checkpoint from redis: %w", err)
	}
	return ParseCheckPoint(val)
}

func (r *RedisCheckPointStore) Save(ctx context.Context, cp CheckPoint) error {
	cp.LastSyncedAt = time.Now().UTC()
	jsonStr, err := cp.String()
	if err != nil {
		return err
	}
	return r.client.Set(ctx, r.redisKey, jsonStr)
}

func (r *RedisCheckPointStore) Reset(ctx context.Context) error {
	return r.client.Del(ctx, r.redisKey)
}

// =========================================================================
// File-backed Checkpoint Store (Fallback)
// =========================================================================

type FileCheckPointStore struct {
	filePath string
	mu       sync.Mutex
}

func NewFileCheckPointStore(filePath string) *FileCheckPointStore {
	if filePath == "" {
		filePath = "checkpoint.json"
	}
	return &FileCheckPointStore{filePath: filePath}
}

func (f *FileCheckPointStore) Load(_ context.Context) (CheckPoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return ParseCheckPoint("")
		}
		return CheckPoint{}, err
	}
	return ParseCheckPoint(string(data))
}

func (f *FileCheckPointStore) Save(_ context.Context, cp CheckPoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	cp.LastSyncedAt = time.Now().UTC()
	jsonBytes, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(f.filePath)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}

	tmp := f.filePath + ".tmp"
	if err := os.WriteFile(tmp, jsonBytes, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, f.filePath)
}

func (f *FileCheckPointStore) Reset(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return os.Remove(f.filePath)
}
