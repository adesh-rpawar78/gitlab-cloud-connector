package connector

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// roundTripFunc implements http.RoundTripper for in-memory HTTP mocking
type roundTripFunc func(req *http.Request) *http.Response

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func TestFetchEventsPaginationAndDeduplication(t *testing.T) {
	ctx := context.Background()

	mockClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) *http.Response {
			query := req.URL.Query()
			page := query.Get("page")

			if page == "1" {
				body := `[
					{"id": 101, "project_id": 278964, "action_name": "pushed to", "created_at": "2026-10-24T10:00:00Z"},
					{"id": 102, "project_id": 278964, "action_name": "opened", "target_type": "Issue", "target_title": "Fix bug", "created_at": "2026-10-24T10:05:00Z"}
				]`
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(body)),
					Header:     make(http.Header),
				}
				resp.Header.Set("Content-Type", "application/json")
				resp.Header.Set("X-Next-Page", "2")
				return resp
			}

			// Page 2
			body := `[
				{"id": 103, "project_id": 278964, "action_name": "commented on", "created_at": "2026-10-24T10:10:00Z"}
			]`
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(body)),
				Header:     make(http.Header),
			}
			resp.Header.Set("Content-Type", "application/json")
			resp.Header.Set("X-Next-Page", "") // Last page
			return resp
		}),
	}

	fetcher := NewFetcherWithClient(mockClient)
	cfg := DefaultConfig()
	cfg.PageSize = 2

	cp := CheckPoint{
		LastEventTimestamp: "2026-10-24T09:00:00Z",
		LastEventID:        0,
		TotalRecordsSynced: 0,
	}

	var collected []GitLabEvent
	err := fetcher.FetchEvents(ctx, &cfg, &cp, func(events []GitLabEvent, updatedCP CheckPoint) error {
		collected = append(collected, events...)
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error fetching events: %v", err)
	}

	if len(collected) != 3 {
		t.Fatalf("expected 3 events across 2 pages, got %d", len(collected))
	}
	if cp.LastEventID != 103 {
		t.Errorf("expected final checkpoint LastEventID to be 103, got %d", cp.LastEventID)
	}
	if cp.TotalRecordsSynced != 3 {
		t.Errorf("expected TotalRecordsSynced to be 3, got %d", cp.TotalRecordsSynced)
	}
}

func TestTestConnectionSuccess(t *testing.T) {
	ctx := context.Background()

	mockClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) *http.Response {
			if strings.Contains(req.URL.Path, "/events") {
				body := `[{"id": 201, "action_name": "pushed to", "created_at": "2026-10-24T11:00:00Z"}]`
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(body)),
					Header:     make(http.Header),
				}
			}

			// Project metadata
			body := `{"name_with_namespace": "GitLab.org / GitLab FOSS", "web_url": "https://gitlab.com/gitlab-org/gitlab-foss"}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(body)),
				Header:     make(http.Header),
			}
		}),
	}

	fetcher := NewFetcherWithClient(mockClient)
	cfg := DefaultConfig()

	resp, err := fetcher.TestConnection(ctx, &cfg)
	if err != nil {
		t.Fatalf("unexpected error in TestConnection: %v", err)
	}

	if resp.Status != "SUCCESS" {
		t.Errorf("expected status SUCCESS, got %s", resp.Status)
	}
	if resp.ProjectName != "GitLab.org / GitLab FOSS" {
		t.Errorf("unexpected ProjectName: %s", resp.ProjectName)
	}
	if len(resp.PreviewEvents) != 1 {
		t.Errorf("expected 1 preview event, got %d", len(resp.PreviewEvents))
	}
}

func TestTestConnectionNotFound(t *testing.T) {
	ctx := context.Background()

	mockClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) *http.Response {
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(bytes.NewBufferString(`{"message":"404 Project Not Found"}`)),
				Header:     make(http.Header),
			}
		}),
	}

	fetcher := NewFetcherWithClient(mockClient)
	cfg := DefaultConfig()
	cfg.ProjectID = "non-existent-999999"

	resp, err := fetcher.TestConnection(ctx, &cfg)
	if err != nil {
		t.Fatalf("unexpected hard error: %v", err)
	}
	if resp.Status != "FAILED" {
		t.Errorf("expected status FAILED, got %s", resp.Status)
	}
	if !strings.Contains(resp.ErrorMessage, "not found") {
		t.Errorf("expected error message to mention not found, got: %s", resp.ErrorMessage)
	}
}
