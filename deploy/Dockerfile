# ── Stage 1: Build Go binaries ────────────────────────────────────────────────
FROM golang:1.24-bookworm AS go-builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" \
        -o /bin/pulsewatch-server ./server/cmd/pulsewatch-server/

# ── Stage 2: Build frontend ───────────────────────────────────────────────────
FROM node:20-slim AS frontend-builder

WORKDIR /frontend
COPY frontend/package*.json ./
RUN npm ci --ignore-scripts

COPY frontend/ ./
RUN npm run build          # output goes to /frontend/dist

# ── Stage 3: Production image ─────────────────────────────────────────────────
FROM python:3.12-slim-bookworm

WORKDIR /app

# System deps: supervisor (process manager) + postgresql-client (migrations)
RUN apt-get update \
 && apt-get install -y --no-install-recommends supervisor postgresql-client \
 && rm -rf /var/lib/apt/lists/*

# Copy Go server binary
COPY --from=go-builder /bin/pulsewatch-server /usr/local/bin/pulsewatch-server

# Copy built frontend into the location the server serves static files from
COPY --from=frontend-builder /frontend/dist /app/frontend/dist

# Python sidecar
COPY sidecar/requirements.txt /app/sidecar/requirements.txt
RUN pip install --no-cache-dir -r /app/sidecar/requirements.txt

COPY sidecar/ /app/sidecar/

# DB migrations
COPY server/migrations/ /app/migrations/

# Supervisord config
COPY deploy/supervisord.conf /etc/supervisor/conf.d/pulsewatch.conf

# Health-check script (run migrations + start services)
COPY deploy/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080

CMD ["/entrypoint.sh"]
