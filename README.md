# PulseWatch 🫀

A real-time distributed system monitoring platform built in Go with a Python ML predictive sidecar.

## Architecture

```
┌─────────────────┐      gRPC / TLS      ┌──────────────────────┐
│   Agent (Go)    │ ──────────────────► │   Server (Go)        │
│                 │                      │                      │
│  • CPU          │   MetricBatch        │  • gRPC ingest       │
│  • Memory       │   + WAL durability   │  • Deferred acks     │
│  • Disk         │   + backoff/jitter   │  • DB-level dedup    │
│  • Network      │                      │  • TimescaleDB       │
│  • Processes    │◄─────────────────── │  • Rollup aggregates │
└─────────────────┘      BatchAck        └──────────┬───────────┘
                                                     │
                         ┌───────────────────────────┤
                         │                           │
              ┌──────────▼──────────┐   ┌───────────▼──────────┐
              │  Python ML Sidecar  │   │  React Dashboard     │
              │                     │   │                      │
              │  • Runway forecaster│   │  • Host inventory    │
              │  • Isolation Forest │   │  • uPlot live charts │
              │  • Anomaly scoring  │   │  • Alert feed        │
              └─────────────────────┘   └──────────────────────┘
```

## Key Reliability Properties

| Property | Implementation |
|----------|---------------|
| Zero data loss across server outages | Agent-side WAL + deferred acks (ack only after DB commit) |
| No duplicate writes after replay | `agent_sequences` table — `ON CONFLICT DO NOTHING` |
| Bounded memory under load | `buffer.Queue` with 5,000-cap channel + `ErrBufferFull` shedding |
| No cardinality explosions | Process collector groups by exe name; `MaxPointsPerBatch=2000`, `MaxLabelsPerPoint=32` |
| Rollup correctness | Stores `sum_val`/`count_val`/`min_val`/`max_val` — never avg-of-avgs |

## Project Structure

```
.
├── agent/                      # Metric collection agent
│   ├── cmd/agent/main.go       # Entry point
│   └── internal/
│       ├── collector/          # CPU, memory, disk, network, process
│       ├── sender/             # gRPC client + backoff
│       └── wal/                # Write-ahead log (segment-rolling, CRC32)
│
├── server/                     # Ingest & query server
│   ├── cmd/pulsewatch-server/  # Entry point (gRPC :50051, HTTP :8080)
│   └── internal/
│       ├── buffer/             # Bounded queue, 8 workers, deferred ack
│       ├── ingest/             # gRPC service implementation
│       ├── reaper/             # Offline host detection
│       ├── storage/            # pgx/v5 — writes, dedup, rollup queries
│       └── telemetry/          # Prometheus /metrics endpoint
│
├── proto/                      # Protobuf definitions + generated Go code
│   ├── metrics.proto
│   └── v1/
│
├── deploy/                     # Docker Compose (TimescaleDB)
│   ├── docker-compose.yml
│   └── .env.example
│
├── server/migrations/          # SQL migration files
│   ├── 001_init.sql            # Schema, hypertable, indexes
│   └── 002_rollups.sql         # Continuous aggregates + retention
│
├── bench/                      # Load + chaos scripts
│   ├── chaos_server_outage.ps1
│   └── verify_phase4.ps1
│
└── docs/                       # Design notes & implementation plan
```

## Getting Started

### Prerequisites

- Go 1.21+
- Docker Desktop
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (for proto changes)

### 1. Start TimescaleDB

```bash
cd deploy
docker compose up -d
```

### 2. Apply Migrations

```bash
docker exec -i pulsewatch-timescaledb psql -U pulsewatch -d pulsewatch < server/migrations/001_init.sql
docker exec -i pulsewatch-timescaledb psql -U pulsewatch -d pulsewatch < server/migrations/002_rollups.sql
```

### 3. Build

```bash
go build -o bin/pulsewatch-server.exe ./server/cmd/pulsewatch-server/
go build -o bin/agent.exe ./agent/cmd/agent/
```

### 4. Run

```bash
# Terminal 1
bin/pulsewatch-server.exe

# Terminal 2
bin/agent.exe
```

### 5. Verify

```bash
# Host list
curl http://localhost:8080/api/v1/hosts

# Latest metric
curl "http://localhost:8080/api/v1/metrics/latest?host=<hostname>&metric=system.cpu.total"

# Prometheus metrics
curl http://localhost:8080/metrics
```

## Roadmap

- [x] Phase 1 — Foundation (repo, CI, proto, Docker Compose)
- [x] Phase 2 — Walking Skeleton (CPU → gRPC → TimescaleDB end-to-end)
- [x] Phase 3 — Reliability Core (WAL, backoff, deferred acks, chaos test)
- [x] Phase 4 — Widen (all collectors, heartbeats, rollups, /metrics)
- [x] Phase 5 — Query API & Live Dashboard
- [ ] Phase 6 — Alerting & Streaming Detection
- [ ] Phase 7 — Load Generator & Evaluation
- [ ] Phase 8 — Python ML Sidecar

## Chaos Test Results

```
Server SIGKILL → Agent spooled 3,068 bytes to WAL
Server restart  → All batches replayed, WAL purged
DB result       → last_acked_seq=11, total_points=187
                  Zero data loss ✅  Zero duplicates ✅
```

## License

MIT
