package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"gitlab-cloud-connector/connector"
	"gitlab-cloud-connector/destination"
	"gitlab-cloud-connector/storage"
)

func setupTestServer() (*Server, storage.RedisClient, connector.StateStore) {
	cfg := connector.DefaultConfig()
	redisClient := storage.NewClient("127.0.0.1:0") // In-memory fallback
	cpStore := connector.NewRedisCheckPointStore(redisClient, cfg.ProjectID)
	dest, _ := destination.New("redis", "", redisClient, cfg.ProjectID)
	fetcher := connector.NewFetcher()

	server := NewServer(&cfg, fetcher, cpStore, dest, redisClient)
	return server, redisClient, cpStore
}

func TestHandleIndex(t *testing.T) {
	server, _, _ := setupTestServer()
	router := server.Routes()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
	contentType := rec.Header().Get("Content-Type")
	if contentType != "text/html; charset=utf-8" {
		t.Errorf("expected Content-Type text/html; charset=utf-8, got %s", contentType)
	}
	if rec.Body.Len() == 0 {
		t.Error("expected non-empty HTML body")
	}
}

func TestHandleStatus(t *testing.T) {
	server, _, _ := setupTestServer()
	router := server.Routes()

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}

	if resp["status"] != "online" {
		t.Errorf("expected status 'online', got %v", resp["status"])
	}
	if resp["project_id"] != "278964" {
		t.Errorf("expected project_id '278964', got %v", resp["project_id"])
	}
}

func TestHandleEventsAndReset(t *testing.T) {
	server, redisClient, cpStore := setupTestServer()
	router := server.Routes()

	// Push dummy event into redis
	ctx := reqCtx()
	listKey := "gitlab:events:278964"
	_ = redisClient.LPush(ctx, listKey, `{"id":1001,"action_name":"pushed to"}`)

	// Save dummy checkpoint
	_ = cpStore.Save(ctx, connector.CheckPoint{
		LastEventID:        1001,
		TotalRecordsSynced: 1,
	})

	// 1. Fetch events
	reqEvents := httptest.NewRequest(http.MethodGet, "/api/events", nil)
	recEvents := httptest.NewRecorder()
	router.ServeHTTP(recEvents, reqEvents)

	if recEvents.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", recEvents.Code)
	}
	var eventsResp struct {
		Count  int   `json:"count"`
		Events []any `json:"events"`
	}
	_ = json.Unmarshal(recEvents.Body.Bytes(), &eventsResp)
	if eventsResp.Count != 1 {
		t.Errorf("expected 1 event, got %d", eventsResp.Count)
	}

	// 2. Reset
	reqReset := httptest.NewRequest(http.MethodPost, "/api/reset", nil)
	recReset := httptest.NewRecorder()
	router.ServeHTTP(recReset, reqReset)

	if recReset.Code != http.StatusOK {
		t.Errorf("expected reset status 200, got %d", recReset.Code)
	}

	// 3. Verify events cleared
	recEventsAfter := httptest.NewRecorder()
	router.ServeHTTP(recEventsAfter, reqEvents)
	_ = json.Unmarshal(recEventsAfter.Body.Bytes(), &eventsResp)
	if eventsResp.Count != 0 {
		t.Errorf("expected 0 events after reset, got %d", eventsResp.Count)
	}
}

func reqCtx() context.Context {
	return httptest.NewRequest(http.MethodGet, "/", nil).Context()
}
