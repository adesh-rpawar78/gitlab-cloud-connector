package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitlab-cloud-connector/connector"
	"gitlab-cloud-connector/destination"
	"gitlab-cloud-connector/storage"
	"gitlab-cloud-connector/web"
)

func main() {
	onceFlag := flag.Bool("once", false, "Execute single extraction run and exit (CronJob / batch mode)")
	destFlag := flag.String("dest", "", "Destination sink: 'stdout', 'file', 'webhook', 'redis'")
	projectFlag := flag.String("project", "", "GitLab Project ID or URL-encoded path")
	portFlag := flag.String("port", "", "Web UI Port (default: 8080)")
	redisFlag := flag.String("redis", "", "Redis address (default: localhost:6379)")
	flag.Parse()

	log.Println("==================================================")
	log.Println("         GitLab Cloud Ingestion Connector         ")
	log.Println("==================================================")

	// 1. Load Configuration
	cfg := connector.LoadFromEnv()
	if *destFlag != "" {
		cfg.DestinationType = *destFlag
	}
	if *projectFlag != "" {
		cfg.ProjectID = *projectFlag
	}
	if *portFlag != "" {
		cfg.UIPort = *portFlag
	}
	if *redisFlag != "" {
		cfg.RedisAddr = *redisFlag
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("[FATAL] Invalid configuration: %v", err)
	}

	// 2. Initialize Redis Storage (with automatic in-memory fallback)
	redisClient := storage.NewClient(cfg.RedisAddr)

	// 3. Initialize Checkpoint Store (Stored in Redis key gitlab:checkpoint:<project_id>)
	cpStore := connector.NewRedisCheckPointStore(redisClient, cfg.ProjectID)

	// 4. Initialize Destination Sinks
	dest, err := destination.New(cfg.DestinationType, cfg.DestinationTarget, redisClient, cfg.ProjectID)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize destination sink: %v", err)
	}
	defer dest.Close()

	// 5. Initialize GitLab Fetcher
	fetcher := connector.NewFetcher()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 6. Handle Batch / CronJob Execution (-once)
	if *onceFlag {
		log.Printf("[RUN] Starting one-shot sync for GitLab Project ID: %s", cfg.ProjectID)
		cp, err := cpStore.Load(ctx)
		if err != nil {
			log.Fatalf("[FATAL] Failed to load checkpoint: %v", err)
		}

		var count int
		err = fetcher.FetchEvents(ctx, cfg, &cp, func(events []connector.GitLabEvent, nextCP connector.CheckPoint) error {
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

			if err := dest.WriteBatch(ctx, records); err != nil {
				return err
			}
			count += len(records)
			return cpStore.Save(ctx, nextCP)
		})

		if err != nil {
			log.Fatalf("[ERROR] One-shot sync failed: %v", err)
		}

		log.Printf("[RUN] Ingestion complete. Synced %d events. Watermark updated in Redis.", count)
		return
	}

	// 7. Start Web UI Server
	webServer := web.NewServer(cfg, fetcher, cpStore, dest, redisClient)
	addr := fmt.Sprintf(":%s", cfg.UIPort)
	go func() {
		log.Printf("[INIT] Web UI Dashboard listening at: http://localhost:%s\n", cfg.UIPort)
		if err := webServer.Start(addr); err != nil && err != http.ErrServerClosed {
			log.Printf("[WARN] Web server failed to start: %v (Hint: pass -port=8081 if port %s is already in use)", err, cfg.UIPort)
		}
	}()

	// 8. Continuous Poller Daemon
	log.Printf("[RUN] Poller daemon active. Polling GitLab every %v. Press Ctrl+C to stop.\n", cfg.PollInterval)
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	// Initial immediate poll on startup
	runSyncCycle(ctx, cfg, fetcher, cpStore, dest)

	for {
		select {
		case <-ctx.Done():
			log.Println("\n[INFO] Graceful shutdown initiated...")
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			_ = webServer.Shutdown(shutdownCtx)
			log.Println("[INFO] GitLab Cloud Connector stopped cleanly.")
			return
		case <-ticker.C:
			runSyncCycle(ctx, cfg, fetcher, cpStore, dest)
		}
	}
}

func runSyncCycle(
	ctx context.Context,
	cfg *connector.Config,
	fetcher *connector.Fetcher,
	cpStore connector.StateStore,
	dest destination.Destination,
) {
	cp, err := cpStore.Load(ctx)
	if err != nil {
		log.Printf("[ERROR] Failed to load checkpoint: %v", err)
		return
	}

	var cycleCount int
	err = fetcher.FetchEvents(ctx, cfg, &cp, func(events []connector.GitLabEvent, nextCP connector.CheckPoint) error {
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

		if err := dest.WriteBatch(ctx, records); err != nil {
			return err
		}
		cycleCount += len(records)
		return cpStore.Save(ctx, nextCP)
	})

	if err != nil {
		log.Printf("[ERROR] Ingestion cycle error: %v", err)
		return
	}

	if cycleCount > 0 {
		log.Printf("[POLLER] Successfully synced %d new events from GitLab project %s (Total: %d)",
			cycleCount, cfg.ProjectID, cp.TotalRecordsSynced)
	}
}
