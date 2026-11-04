package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// GitLabEvent models an audit / activity event from GitLab Cloud API.
type GitLabEvent struct {
	ID          int64  `json:"id"`
	ProjectID   int64  `json:"project_id"`
	ActionName  string `json:"action_name"` // "pushed to", "opened", "closed", "commented on", "joined"
	TargetType  string `json:"target_type"` // "Issue", "MergeRequest", "Milestone", "Note", etc.
	TargetTitle string `json:"target_title"`
	CreatedAt   string `json:"created_at"` // ISO 8601 timestamp
	Author      struct {
		ID       int64  `json:"id"`
		Username string `json:"username"`
		Name     string `json:"name"`
	} `json:"author"`
	PushData *struct {
		CommitCount int    `json:"commit_count"`
		Action      string `json:"action"`
		Ref         string `json:"ref"`
		CommitTitle string `json:"commit_title"`
	} `json:"push_data,omitempty"`
}

// TestConnectionResponse provides diagnostics when verifying credentials.
type TestConnectionResponse struct {
	Status        string        `json:"status"` // "SUCCESS" or "FAILED"
	ProjectName   string        `json:"project_name,omitempty"`
	ProjectWebURL string        `json:"project_web_url,omitempty"`
	LatencyMs     int64         `json:"latency_ms"`
	PreviewEvents []GitLabEvent `json:"preview_events,omitempty"`
	ErrorMessage  string        `json:"error_message,omitempty"`
}

// Fetcher is the core data extraction engine.
//
// Pattern Note: In cloud-http-connector, fetcher.go orchestrates:
// 1. Authenticated HTTP requests with automatic retries on rate limits (HTTP 429)
// 2. Cursor/Time pagination logic
// 3. TestConnection diagnostics endpoint for the Web UI
type Fetcher struct {
	httpClient         *http.Client
	eventFetchedCount  int64
	eventIngestedCount int64
}

// NewFetcher creates an initialized Fetcher instance with a resilient HTTP client.
func NewFetcher() *Fetcher {
	return NewFetcherWithClient(&http.Client{
		Timeout: 30 * time.Second,
	})
}

// NewFetcherWithClient creates a Fetcher with a custom HTTP client (useful for testing with mock transports).
func NewFetcherWithClient(client *http.Client) *Fetcher {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Fetcher{
		httpClient: client,
	}
}

// cleanProjectID ensures slash-separated project paths (e.g. 'gitlab-org/gitlab')
// are properly URL-encoded as 'gitlab-org%2Fgitlab' per GitLab API specification.
func cleanProjectID(id string) string {
	if !strings.Contains(id, "%2F") {
		return strings.ReplaceAll(id, "/", "%2F")
	}
	return id
}

// FetchEvents executes incremental event extraction starting from the checkpoint.
// It pages through the GitLab API and calls yield() for every batch.
func (f *Fetcher) FetchEvents(
	ctx context.Context,
	cfg *Config,
	cp *CheckPoint,
	yield func(events []GitLabEvent, updatedCP CheckPoint) error,
) error {
	page := 1
	var newestTimestamp = cp.LastEventTimestamp
	var newestID = cp.LastEventID
	var reachedCheckpoint bool

	projectID := cleanProjectID(cfg.ProjectID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 1. Build Query URL
		baseURL := fmt.Sprintf("%s/api/v4/projects/%s/events", cfg.GitLabBaseURL, projectID)
		u, err := url.Parse(baseURL)
		if err != nil {
			return fmt.Errorf("invalid endpoint URL: %w", err)
		}

		q := u.Query()
		q.Set("per_page", strconv.Itoa(cfg.PageSize))
		q.Set("page", strconv.Itoa(page))
		// Note: We intentionally avoid sort=asc because on large GitLab projects (like gitlab-org/gitlab),
		// ascending sort triggers unindexed full-table scans that timeout with HTTP 500.
		// Default sort=desc uses primary indexes and returns in milliseconds.

		if cp.LastEventTimestamp != "" {
			// Extract date part (YYYY-MM-DD) from RFC3339 timestamp
			datePart := strings.Split(cp.LastEventTimestamp, "T")[0]
			q.Set("after", datePart)
		}
		u.RawQuery = q.Encode()

		// 2. Execute Request with 429 Backoff
		resp, body, err := f.executeWithBackoff(ctx, u.String(), cfg.PrivateToken)
		if err != nil {
			return fmt.Errorf("failed fetching page %d: %w", page, err)
		}

		var events []GitLabEvent
		if err := json.Unmarshal(body, &events); err != nil {
			return fmt.Errorf("failed to decode GitLab response on page %d: %w", page, err)
		}

		atomic.AddInt64(&f.eventFetchedCount, int64(len(events)))

		if len(events) == 0 {
			break
		}

		// 3. Filter out records already seen (newest-first stream)
		newEvents := make([]GitLabEvent, 0, len(events))
		for _, ev := range events {
			// Stop if event is older or equal to checkpoint watermark
			if cp.LastEventTimestamp != "" {
				if ev.CreatedAt < cp.LastEventTimestamp || (ev.CreatedAt == cp.LastEventTimestamp && ev.ID <= cp.LastEventID) {
					reachedCheckpoint = true
					continue
				}
			}

			// Capture the newest event seen across all pages as the new watermark
			if ev.CreatedAt > newestTimestamp || (ev.CreatedAt == newestTimestamp && ev.ID > newestID) {
				newestTimestamp = ev.CreatedAt
				newestID = ev.ID
			}

			newEvents = append(newEvents, ev)
		}

		// 4. Yield events downstream to destination
		if len(newEvents) > 0 {
			nextCP := CheckPoint{
				LastEventTimestamp: newestTimestamp,
				LastEventID:        newestID,
				TotalRecordsSynced: cp.TotalRecordsSynced + int64(len(newEvents)),
			}

			if err := yield(newEvents, nextCP); err != nil {
				return fmt.Errorf("yield downstream error: %w", err)
			}
			atomic.AddInt64(&f.eventIngestedCount, int64(len(newEvents)))
			*cp = nextCP
		}

		// If we hit already-synced records or fewer events than pageSize, stop paging
		if reachedCheckpoint || len(events) < cfg.PageSize {
			break
		}

		// 5. Pagination Check (using GitLab X-Next-Page header)
		nextPageHeader := resp.Header.Get("X-Next-Page")
		if nextPageHeader == "" {
			break
		}

		nextPageNum, err := strconv.Atoi(nextPageHeader)
		if err != nil || nextPageNum <= page {
			break
		}
		page = nextPageNum
	}

	return nil
}

// TestConnection validates credentials and connectivity against the target GitLab project.
func (f *Fetcher) TestConnection(ctx context.Context, cfg *Config) (*TestConnectionResponse, error) {
	start := time.Now()

	// 1. Query Project Metadata (/api/v4/projects/:id)
	projectID := cleanProjectID(cfg.ProjectID)
	projectURL := fmt.Sprintf("%s/api/v4/projects/%s", cfg.GitLabBaseURL, projectID)
	resp, body, err := f.executeWithBackoff(ctx, projectURL, cfg.PrivateToken)
	latency := time.Since(start).Milliseconds()

	if resp != nil {
		if resp.StatusCode == http.StatusNotFound {
			return &TestConnectionResponse{
				Status:       "FAILED",
				LatencyMs:    latency,
				ErrorMessage: fmt.Sprintf("GitLab project '%s' not found. Verify the Project ID.", cfg.ProjectID),
			}, nil
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return &TestConnectionResponse{
				Status:       "FAILED",
				LatencyMs:    latency,
				ErrorMessage: "authentication failed. Please check your GitLab Private Token permissions.",
			}, nil
		}
	}

	if err != nil {
		return &TestConnectionResponse{
			Status:       "FAILED",
			LatencyMs:    latency,
			ErrorMessage: fmt.Sprintf("connection test failed: %v", err),
		}, err
	}

	var projectMeta struct {
		Name   string `json:"name_with_namespace"`
		WebURL string `json:"web_url"`
	}
	_ = json.Unmarshal(body, &projectMeta)

	// 2. Fetch up to 3 preview events to confirm read access to the events stream
	previewURL := fmt.Sprintf("%s/api/v4/projects/%s/events?per_page=3", cfg.GitLabBaseURL, projectID)
	_, eventsBody, err := f.executeWithBackoff(ctx, previewURL, cfg.PrivateToken)

	var previewEvents []GitLabEvent
	if err == nil {
		_ = json.Unmarshal(eventsBody, &previewEvents)
	}

	return &TestConnectionResponse{
		Status:        "SUCCESS",
		ProjectName:   projectMeta.Name,
		ProjectWebURL: projectMeta.WebURL,
		LatencyMs:     latency,
		PreviewEvents: previewEvents,
	}, nil
}

// executeWithBackoff handles HTTP requests with exponential backoff on HTTP 429 and 503.
func (f *Fetcher) executeWithBackoff(ctx context.Context, apiURL, token string) (*http.Response, []byte, error) {
	maxRetries := 3

	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
		if err != nil {
			return nil, nil, err
		}

		if token != "" {
			req.Header.Set("PRIVATE-TOKEN", token)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "GitLab-Cloud-Connector/1.0")

		resp, err := f.httpClient.Do(req)
		if err != nil {
			if attempt == maxRetries {
				return nil, nil, err
			}
			time.Sleep(time.Duration(1<<attempt) * 500 * time.Millisecond)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return resp, nil, err
		}

		// Handle Rate Limiting (HTTP 429)
		if resp.StatusCode == http.StatusTooManyRequests {
			backoff := time.Duration(1<<attempt) * time.Second
			if retryHeader := resp.Header.Get("Retry-After"); retryHeader != "" {
				if sec, err := strconv.Atoi(retryHeader); err == nil && sec > 0 {
					backoff = time.Duration(sec) * time.Second
				}
			}
			fmt.Printf("[RATE LIMIT] GitLab API throttled (HTTP 429). Backing off for %v (retry %d/%d)...\n", backoff, attempt+1, maxRetries)
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(backoff):
				continue
			}
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, body, nil
		}

		if resp.StatusCode >= 500 && attempt < maxRetries {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}

		return resp, body, fmt.Errorf("GitLab API returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil, nil, fmt.Errorf("exhausted retries connecting to GitLab API")
}
