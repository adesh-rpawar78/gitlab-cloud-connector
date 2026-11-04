package destination

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileDestination appends records as newline-delimited JSON (NDJSON) to disk.
type FileDestination struct {
	path string
	mu   sync.Mutex
}

func NewFileDestination(path string) *FileDestination {
	if path == "" {
		path = "output/gitlab_events.ndjson"
	}
	return &FileDestination{path: path}
}

func (f *FileDestination) WriteBatch(_ context.Context, records []Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	dir := filepath.Dir(f.path)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}

	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open output file: %w", err)
	}
	defer file.Close()

	for _, r := range records {
		b, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if _, err := file.Write(append(b, '\n')); err != nil {
			return err
		}
	}

	fmt.Printf("--> [DESTINATION : FILE] Appended %d records to %s\n", len(records), f.path)
	return nil
}

func (f *FileDestination) Close() error {
	return nil
}
