# ==============================================================================
# Build Stage: Compile static Go binary
# ==============================================================================
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Cache dependencies
COPY go.mod ./
RUN go mod download || true

# Copy source tree
COPY . .

# Compile static binary with optimizations (-ldflags="-s -w")
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w -X main.version=1.0.0" \
    -o /bin/gitlab-connector .

# ==============================================================================
# Production Runtime Stage: Ultra-lightweight & Secure
# ==============================================================================
FROM alpine:3.19

# Install HTTPS certificates and timezone database
RUN apk --no-cache add ca-certificates tzdata \
    && addgroup -S appgroup -g 10001 \
    && adduser -S appuser -u 10001 -G appgroup

WORKDIR /app

# Copy binary from builder
COPY --from=builder /bin/gitlab-connector /app/gitlab-connector

# Secure execution as non-root user
USER 10001:10001

# Web UI and Healthcheck Port
EXPOSE 8080

# Environment Defaults
ENV PORT="8080" \
    DESTINATION_TYPE="stdout" \
    GITLAB_BASE_URL="https://gitlab.com" \
    GITLAB_PROJECT_ID="278964" \
    PAGE_SIZE="20" \
    POLL_INTERVAL="30s" \
    REDIS_ADDR="redis:6379"

ENTRYPOINT ["/app/gitlab-connector"]
