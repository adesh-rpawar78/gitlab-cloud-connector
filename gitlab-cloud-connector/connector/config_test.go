package connector

import (
	"os"
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.GitLabBaseURL != "https://gitlab.com" {
		t.Errorf("expected default GitLabBaseURL https://gitlab.com, got %s", cfg.GitLabBaseURL)
	}
	if cfg.ProjectID != "278964" {
		t.Errorf("expected default ProjectID 278964, got %s", cfg.ProjectID)
	}
	if cfg.PageSize != 20 {
		t.Errorf("expected default PageSize 20, got %d", cfg.PageSize)
	}
	if cfg.PollInterval != 30*time.Second {
		t.Errorf("expected default PollInterval 30s, got %v", cfg.PollInterval)
	}
	if cfg.DestinationType != "stdout" {
		t.Errorf("expected default DestinationType stdout, got %s", cfg.DestinationType)
	}
	if cfg.UIPort != "8080" {
		t.Errorf("expected default UIPort 8080, got %s", cfg.UIPort)
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("expected default RedisAddr localhost:6379, got %s", cfg.RedisAddr)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig() should be valid, got: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := DefaultConfig()

	cfg.ProjectID = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error on empty ProjectID, got nil")
	}

	cfg.ProjectID = "278964"
	cfg.DestinationType = "invalid_sink"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error on invalid DestinationType, got nil")
	}

	cfg.DestinationType = "webhook"
	cfg.DestinationTarget = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error when webhook has empty DestinationTarget, got nil")
	}
}

func TestLoadFromEnv(t *testing.T) {
	os.Setenv("GITLAB_PROJECT_ID", "999888")
	os.Setenv("DESTINATION_TYPE", "redis")
	os.Setenv("PAGE_SIZE", "50")
	os.Setenv("POLL_INTERVAL", "15s")
	os.Setenv("PORT", "9090")
	defer func() {
		os.Unsetenv("GITLAB_PROJECT_ID")
		os.Unsetenv("DESTINATION_TYPE")
		os.Unsetenv("PAGE_SIZE")
		os.Unsetenv("POLL_INTERVAL")
		os.Unsetenv("PORT")
	}()

	cfg := LoadFromEnv()
	if cfg.ProjectID != "999888" {
		t.Errorf("expected ProjectID 999888, got %s", cfg.ProjectID)
	}
	if cfg.DestinationType != "redis" {
		t.Errorf("expected DestinationType redis, got %s", cfg.DestinationType)
	}
	if cfg.PageSize != 50 {
		t.Errorf("expected PageSize 50, got %d", cfg.PageSize)
	}
	if cfg.PollInterval != 15*time.Second {
		t.Errorf("expected PollInterval 15s, got %v", cfg.PollInterval)
	}
	if cfg.UIPort != "9090" {
		t.Errorf("expected UIPort 9090, got %s", cfg.UIPort)
	}
}
