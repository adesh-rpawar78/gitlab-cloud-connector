![Uploading image.png…]()

# GitLab Cloud Ingestion Connector

A high-performance, production-grade cloud ingestion connector built in Go. It incrementally extracts event streams from the **GitLab Cloud REST API** (free tier compatible), manages state using **Redis** watermarking checkpoints, routes data to pluggable destinations (Stdout, NDJSON File, HTTP Webhook, Redis), serves an embedded **Web UI Dashboard**, and deploys declaratively via **Kubernetes** and **ArgoCD GitOps**.

---

## 🌟 Resume & Interview Highlights

- **Production Cloud Connector Pattern**: Designed following the industry-standard `config.go`, `checkpoint.go`, and `fetcher.go` architecture for resilient data integration pipelines.
- **Incremental Extraction & Monotonic Watermarking**: Implemented timestamp + ID cursor state tracking stored in Redis (`gitlab:checkpoint:<project_id>`) to eliminate duplicate event ingestion and guarantee at-least-once delivery across restarts.
- **API Resilience & Rate-Limit Handling**: Handled HTTP 429 (Too Many Requests) with dynamic `Retry-After` header parsing and exponential backoff with jitter.
- **Multi-Sink Destination Architecture**: Clean Go interface allowing simultaneous broadcasting to Redis streams/lists, append-only NDJSON log files, external webhooks, or stdout.
- **Zero-Dependency Redis Layer**: Engineered native Go RESP (REdis Serialization Protocol) client with an automatic thread-safe in-memory fallback engine—allowing instant zero-setup local execution even without a running Redis server.
- **Embedded Web Dashboard**: Embedded a responsive HTML5/CSS3/Vanilla JS management dashboard into the compiled Go binary using `//go:embed`, providing live metrics, manual sync triggers, connection diagnostics, and event payload inspection.
- **Cloud-Native & GitOps Ready**: Containerized with a hardened multi-stage Docker build (Alpine, non-root user UID 10001, read-only root filesystem), packaged with Kubernetes manifests (Deployment, Service, ConfigMap, Secret, CronJob), and configured for ArgoCD continuous delivery.

---

## 🏛️ Architecture Overview

```
                        ┌──────────────────────────────────────────┐
                        │          GitLab Cloud REST API           │
                        │   (GET /api/v4/projects/:id/events)      │
                        └────────────────────┬─────────────────────┘
                                             │ HTTP/1.1 (JSON)
                                             ▼
                                  ┌─────────────────────┐
                                  │     fetcher.go      │
                                  │  - Cursor Pagination│
                                  │  - 429 Rate Backoff │
                                  │  - Deduplication    │
                                  └──────────┬──────────┘
                                             │
                       ┌─────────────────────┴─────────────────────┐
                       ▼                                           ▼
            ┌──────────────────────┐                   ┌───────────────────────┐
            │    checkpoint.go     │                   │     destination/      │
            │  - Monotonic Cursor  │                   │  - Redis list         │
            │  - Redis State Store │                   │  - Append NDJSON File │
            │  - File Fallback     │                   │  - Webhook POST       │
            └──────────┬───────────┘                   │  - Stdout Logger      │
                       │                               └───────────┬───────────┘
                       ▼                                           │
         ┌───────────────────────────┐                             │
         │  Redis Database / Cache   │◄────────────────────────────┘
         │  - gitlab:checkpoint:<id> │
         │  - gitlab:events:<id>     │
         └─────────────┬─────────────┘
                       │
                       ▼
         ┌───────────────────────────┐
         │     Web UI Dashboard      │
         │  - Embedded (//go:embed)  │
         │  - Live Sync & Telemetry  │
         │  - Diagnostic Inspector   │
         │  - Port 8080 REST API     │
         └───────────────────────────┘
```

---

## 📂 Project Directory Structure

```
gitlab-cloud-connector/
├── connector/
│   ├── config.go            # Configuration model, env loader, and validation logic
│   ├── config_test.go       # Config unit tests
│   ├── checkpoint.go        # StateStore interface, CheckPoint model, Redis & File stores
│   ├── checkpoint_test.go   # Checkpoint serialization and store unit tests
│   ├── fetcher.go           # Core HTTP ingestion engine, pagination, backoff, TestConnection
│   └── fetcher_test.go       # In-memory mock transport unit tests
├── destination/
│   ├── destination.go       # Record envelope and Destination interface definition
│   ├── stdout.go            # Terminal console sink
│   ├── file.go              # Append-only NDJSON file sink
│   ├── webhook.go           # HTTP POST batch delivery sink
│   └── redis.go             # Redis list sink & MultiDestination broadcaster
├── storage/
│   └── redis.go             # Native RESP Redis client with in-memory fallback engine
├── web/
│   ├── ui.html              # Responsive single-page dashboard (HTML5, CSS3, JS)
│   ├── server.go            # REST API handler embedding ui.html (//go:embed)
│   └── server_test.go       # HTTP endpoint unit tests using httptest
├── deploy/
│   ├── k8s/
│   │   ├── configmap.yaml   # Environment configuration for Kubernetes
│   │   ├── secret.yaml      # Private Token secret template
│   │   ├── redis.yaml       # Redis Deployment and ClusterIP Service
│   │   ├── deployment.yaml  # Continuous connector daemon Deployment (singleton)
│   │   ├── service.yaml     # ClusterIP Service exposing Web UI (port 8080)
│   │   └── cronjob.yaml     # Alternative batch ingestion CronJob (-once mode)
│   └── argocd/
│       └── application.yaml # ArgoCD Application CRD for GitOps deployment
├── Dockerfile               # Production multi-stage Alpine container build
├── go.mod                   # Go 1.22 module definition
├── main.go                  # CLI entrypoint, poller daemon, batch runner, and HTTP server
└── README.md                # Comprehensive documentation
```

---

## 🔍 Core Component Breakdown

### 1. `connector/config.go`
- Implements the **Configuration Pattern** used across modern connectors.
- Encapsulates `GitLabBaseURL`, `ProjectID`, `PrivateToken`, `PageSize`, `PollInterval`, `DestinationType`, `DestinationTarget`, `RedisAddr`, and `UIPort`.
- Provides `Validate()` for fail-fast startup validation and `LoadFromEnv()` with sensible defaults (defaults to GitLab Free Public Project ID `278964` / GitLab FOSS).

### 2. `connector/checkpoint.go`
- Implements the **Resilient Watermarking Pattern**.
- Tracks `last_event_timestamp` (RFC3339), `last_event_id` (tie-breaker for simultaneous events), and `total_records_synced`.
- The `StateStore` interface abstracts the storage backend, supported by:
  - `RedisCheckPointStore`: High-performance Redis key-value storage (`gitlab:checkpoint:<project_id>`).
  - `FileCheckPointStore`: Local atomic JSON file storage using temp files and atomic rename.

### 3. `connector/fetcher.go`
- Implements the **Ingestion & Extraction Engine**.
- Executes authenticated HTTP queries against `GET /api/v4/projects/:id/events`.
- Iterates over GitLab's response headers (`X-Next-Page`, `X-Total-Pages`) in monotonic ascending order (`sort=asc`).
- Employs **Exponential Backoff**: automatically detects HTTP 429 rate limits, parses the `Retry-After` header, and pauses execution safely.
- Implements `TestConnection()`: verifies credentials and network reachability before initiating pipelines, returning project metadata and preview events.

---

## 🚀 Getting Started Locally

### Prerequisites
- Go 1.22+ installed
- *(Optional)* Redis server running on `localhost:6379`. If Redis is not installed, the connector automatically activates its built-in in-memory Redis engine so you can test immediately!

### 1. Run the Web Dashboard & Continuous Daemon
```bash
go run main.go
```
Open your browser and navigate to:
```
http://localhost:8080
```

### 2. Run in One-Shot Batch Mode (CronJob Style)
```bash
go run main.go -once
```

### 3. Customize via CLI Flags or Environment Variables
```bash
# Ingest from a custom GitLab project and route to Redis:
go run main.go -project="gitlab-org/gitlab" -dest="redis" -redis="localhost:6379"

# Or configure via environment variables:
export GITLAB_PROJECT_ID="278964"
export DESTINATION_TYPE="redis"
export POLL_INTERVAL="15s"
export PORT="8080"
go run main.go
```

### 4. Run Unit Tests
```bash
go test -v ./...
```

---

## 🐳 Docker Containerization

The project uses a secure, multi-stage `Dockerfile`:
```bash
# Build the Docker image
docker build -t gitlab-cloud-connector:latest .

# Run container locally
docker run -d -p 8080:8080 \
  -e DESTINATION_TYPE="redis" \
  -e REDIS_ADDR="host.docker.internal:6379" \
  gitlab-cloud-connector:latest
```

---

## ☸️ Kubernetes Deployment

Deploy the entire stack (Redis + Connector + Service) with `kubectl`:

```bash
# 1. Apply Redis caching layer
kubectl apply -f deploy/k8s/redis.yaml

# 2. Apply ConfigMap and Secret
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/secret.yaml

# 3. Deploy Connector Daemon & Service
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml

# 4. (Optional) Deploy Batch CronJob
kubectl apply -f deploy/k8s/cronjob.yaml

# 5. Access the Web Dashboard via port-forward:
kubectl port-forward svc/gitlab-cloud-connector 8080:8080
```

---

## 🔄 ArgoCD GitOps Deployment

The project includes an ArgoCD Custom Resource Definition in `deploy/argocd/application.yaml`.

To deploy via ArgoCD:
1. Push this repository to your GitHub account:
   ```bash
   git init
   git add .
   git commit -m "Initial commit of gitlab-cloud-connector"
   git remote add origin https://github.com/<your-username>/gitlab-cloud-connector.git
   git push -u origin main
   ```
2. Update the `repoURL` in `deploy/argocd/application.yaml` to your GitHub repo URL.
3. Apply the application manifest to your ArgoCD cluster:
   ```bash
   kubectl apply -f deploy/argocd/application.yaml
   ```
4. ArgoCD will automatically detect changes, synchronize manifests, self-heal drift, and manage lifecycle rollouts.

---

## 🔒 License & Copyright Cleanliness
This codebase is 100% original, written from scratch, and does not contain any proprietary or copyrighted code. You are completely free to push this project to your public GitHub profile and feature it on your resume.
