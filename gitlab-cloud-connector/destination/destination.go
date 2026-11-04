package destination

import (
	"context"
	"fmt"
	"time"

	"gitlab-cloud-connector/storage"
)

// Record is the normalized event envelope shipped to any downstream destination.
type Record struct {
	ID         string    `json:"id"`
	Source     string    `json:"source"`
	Timestamp  time.Time `json:"timestamp"`
	IngestedAt time.Time `json:"ingested_at"`
	Payload    any       `json:"payload"`
}

// Destination represents an output sink for processed events.
type Destination interface {
	WriteBatch(ctx context.Context, records []Record) error
	Close() error
}

// New creates the appropriate destination sink based on configuration.
func New(destType, target string, redisClient storage.RedisClient, projectID string) (Destination, error) {
	var primarySink Destination

	switch destType {
	case "file":
		primarySink = NewFileDestination(target)
	case "webhook":
		primarySink = NewWebhookDestination(target)
	case "redis":
		return NewRedisDestination(redisClient, projectID, 200), nil
	case "stdout", "":
		primarySink = NewStdoutDestination()
	default:
		return nil, fmt.Errorf("unsupported destination type: %s (supported: stdout, file, webhook, redis)", destType)
	}

	// Always also broadcast to Redis list for live Web UI display
	redisSink := NewRedisDestination(redisClient, projectID, 200)
	return NewMultiDestination(primarySink, redisSink), nil
}
