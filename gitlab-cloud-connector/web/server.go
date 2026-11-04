package web

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"gitlab-cloud-connector/connector"
	"gitlab-cloud-connector/destination"
	"gitlab-cloud-connector/storage"
)

//go:embed ui.html
var dashboardHTML []byte

// Server provides the HTTP dashboard and management REST API.
type Server struct {
	cfg         *connector.Config
	fetcher     *connector.Fetcher
	cpStore     connector.StateStore
	dest        destination.Destination
	redisClient storage.RedisClient
	server      *http.Server
	syncMu      sync.Mutex
	isSyncing   bool
}

func NewServer(
	cfg *connector.Config,
	fetcher *connector.Fetcher,
	cpStore connector.StateStore,
	dest destination.Destination,
	redisClient storage.RedisClient,
) *Server {
	return &Server{
		cfg:         cfg,
		fetcher:     fetcher,
		cpStore:     cpStore,
		dest:        dest,
		redisClient: redisClient,
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/config", s.handleSaveConfig)
	mux.HandleFunc("/api/events", s.handleEvents)
	mux.HandleFunc("/api/sync", s.handleSync)
	mux.HandleFunc("/api/test-connection", s.handleTestConnection)
	mux.HandleFunc("/api/reset", s.handleReset)
	return mux
}

func (s *Server) Start(addr string) error {
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.Routes(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 45 * time.Second,
	}

	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(dashboardHTML)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	cp, _ := s.cpStore.Load(r.Context())

	s.syncMu.Lock()
	syncing := s.isSyncing
	s.syncMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"status":            "online",
		"is_syncing":        syncing,
		"project_id":        s.cfg.ProjectID,
		"gitlab_url":        s.cfg.GitLabBaseURL,
		"destination_type":  s.cfg.DestinationType,
		"redis_is_external": s.redisClient.IsExternal(),
		"checkpoint":        cp,
	})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	listKey := fmt.Sprintf("gitlab:events:%s", s.cfg.ProjectID)
	rawEvents, err := s.redisClient.LRange(ctx, listKey, 0, 99)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	events := make([]any, 0, len(rawEvents))
	for _, raw := range rawEvents {
		var item any
		if err := json.Unmarshal([]byte(raw), &item); err == nil {
			events = append(events, item)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"count":  len(events),
		"events": events,
	})
}

func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.syncMu.Lock()
	if s.isSyncing {
		s.syncMu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]any{"message": "sync is already in progress"})
		return
	}
	s.isSyncing = true
	s.syncMu.Unlock()

	defer func() {
		s.syncMu.Lock()
		s.isSyncing = false
		s.syncMu.Unlock()
	}()

	cp, err := s.cpStore.Load(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	var syncedCount int
	err = s.fetcher.FetchEvents(r.Context(), s.cfg, &cp, func(events []connector.GitLabEvent, updatedCP connector.CheckPoint) error {
		records := make([]destination.Record, len(events))
		for i, ev := range events {
			t, _ := time.Parse(time.RFC3339, ev.CreatedAt)
			records[i] = destination.Record{
				ID:         fmt.Sprintf("%d", ev.ID),
				Source:     "gitlab",
				Timestamp:  t,
				IngestedAt: time.Now().UTC(),
				Payload:    ev,
			}
		}

		if err := s.dest.WriteBatch(r.Context(), records); err != nil {
			return err
		}

		syncedCount += len(records)
		return s.cpStore.Save(r.Context(), updatedCP)
	})

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "success",
		"events_synced": syncedCount,
		"checkpoint":    cp,
	})
}

func (s *Server) handleTestConnection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	testCfg := *s.cfg
	var bodyCfg struct {
		GitLabBaseURL string `json:"gitlab_base_url"`
		ProjectID     string `json:"project_id"`
		PrivateToken  string `json:"private_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&bodyCfg); err == nil {
		if bodyCfg.GitLabBaseURL != "" {
			testCfg.GitLabBaseURL = bodyCfg.GitLabBaseURL
		}
		if bodyCfg.ProjectID != "" {
			testCfg.ProjectID = bodyCfg.ProjectID
		}
		if bodyCfg.PrivateToken != "" {
			testCfg.PrivateToken = bodyCfg.PrivateToken
		}
	}

	res, err := s.fetcher.TestConnection(r.Context(), &testCfg)
	if err != nil && res == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleSaveConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var newCfg struct {
		GitLabBaseURL   string `json:"gitlab_base_url"`
		ProjectID       string `json:"project_id"`
		PrivateToken    string `json:"private_token"`
		PageSize        int    `json:"page_size"`
		DestinationType string `json:"destination_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&newCfg); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json payload"})
		return
	}

	if newCfg.GitLabBaseURL != "" {
		s.cfg.GitLabBaseURL = newCfg.GitLabBaseURL
	}
	if newCfg.ProjectID != "" {
		s.cfg.ProjectID = newCfg.ProjectID
	}
	if newCfg.PrivateToken != "" {
		s.cfg.PrivateToken = newCfg.PrivateToken
	}
	if newCfg.PageSize > 0 {
		s.cfg.PageSize = newCfg.PageSize
	}
	if newCfg.DestinationType != "" {
		s.cfg.DestinationType = newCfg.DestinationType
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "config_saved",
		"config":  s.cfg,
		"message": "Configuration updated successfully.",
	})
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	_ = s.cpStore.Reset(r.Context())
	listKey := fmt.Sprintf("gitlab:events:%s", s.cfg.ProjectID)
	_ = s.redisClient.Del(r.Context(), listKey)

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "reset_successful",
		"message": "Checkpoint watermark and event cache reset in Redis.",
	})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
