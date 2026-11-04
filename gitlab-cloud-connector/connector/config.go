package connector

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config encapsulates all settings required to connect to the GitLab API,
// manage state tracking, and route events to the destination sink.
//
// Pattern Note: In production connectors (like cloud-http-connector), config.go
// decouples raw configuration strings (from env vars or UI forms) into a validated,
// strongly-typed configuration model.
type Config struct {
	// GitLab Cloud API Settings
	GitLabBaseURL string `json:"gitlab_base_url"` // e.g. "https://gitlab.com"
	ProjectID     string `json:"project_id"`      // Numeric ID (e.g. "278964") or encoded path (e.g. "gitlab-org%2Fgitlab")
	PrivateToken  string `json:"private_token"`   // GitLab Personal Access Token (optional for public repos)
	PageSize      int    `json:"page_size"`       // Number of events per page (1 to 100)

	// Scheduling & Ingestion Loop
	PollInterval time.Duration `json:"poll_interval"` // Frequency of recurring pulls

	// Destination Routing
	DestinationType   string `json:"destination_type"`   // "stdout", "file", "webhook", "redis"
	DestinationTarget string `json:"destination_target"` // File path or webhook URL

	// Redis Storage Settings
	RedisAddr string `json:"redis_addr"` // e.g. "localhost:6379" or "redis.default:6379"

	// Web Dashboard
	UIPort string `json:"ui_port"` // e.g. "8080"
}

// DefaultConfig provides sensible defaults using GitLab's official public repository
// so the connector works immediately out-of-the-box with zero setup!
func DefaultConfig() Config {
	return Config{
		GitLabBaseURL:     "https://gitlab.com",
		ProjectID:         "278964", // GitLab FOSS (official public project)
		PrivateToken:      "",       // Empty for public access
		PageSize:          20,
		PollInterval:      30 * time.Second,
		DestinationType:   "stdout",
		DestinationTarget: "output/gitlab_events.ndjson",
		RedisAddr:         "localhost:6379",
		UIPort:            "8080",
	}
}

// LoadFromEnv loads settings from environment variables with fallback to defaults.
func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	if val := os.Getenv("GITLAB_BASE_URL"); val != "" {
		cfg.GitLabBaseURL = strings.TrimRight(val, "/")
	}
	if val := os.Getenv("GITLAB_PROJECT_ID"); val != "" {
		cfg.ProjectID = val
	}
	if val := os.Getenv("GITLAB_PRIVATE_TOKEN"); val != "" {
		cfg.PrivateToken = val
	}
	if val := os.Getenv("PAGE_SIZE"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			cfg.PageSize = n
		}
	}
	if val := os.Getenv("POLL_INTERVAL"); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			cfg.PollInterval = d
		}
	}
	if val := os.Getenv("DESTINATION_TYPE"); val != "" {
		cfg.DestinationType = val
	}
	if val := os.Getenv("DESTINATION_TARGET"); val != "" {
		cfg.DestinationTarget = val
	}
	if val := os.Getenv("REDIS_ADDR"); val != "" {
		cfg.RedisAddr = val
	}
	if val := os.Getenv("PORT"); val != "" {
		cfg.UIPort = val
	} else if val := os.Getenv("UI_PORT"); val != "" {
		cfg.UIPort = val
	}

	return &cfg
}

// Validate checks whether mandatory parameters are present and properly formatted.
func (c *Config) Validate() error {
	if c.GitLabBaseURL == "" {
		return fmt.Errorf("gitlab_base_url is required")
	}
	if _, err := url.ParseRequestURI(c.GitLabBaseURL); err != nil {
		return fmt.Errorf("invalid gitlab_base_url: %w", err)
	}
	if c.ProjectID == "" {
		return fmt.Errorf("project_id is required")
	}
	if c.PageSize <= 0 || c.PageSize > 100 {
		return fmt.Errorf("page_size must be between 1 and 100 (GitLab API constraint)")
	}

	validDestinations := map[string]bool{
		"stdout":  true,
		"file":    true,
		"webhook": true,
		"redis":   true,
	}
	if !validDestinations[c.DestinationType] {
		return fmt.Errorf("invalid destination_type '%s', allowed: stdout, file, webhook, redis", c.DestinationType)
	}

	if c.DestinationType == "webhook" && c.DestinationTarget == "" {
		return fmt.Errorf("destination_target (URL) is required when destination_type is webhook")
	}

	return nil
}
