# PulseWatch 🫀

**Real-time system monitoring for any PC — stream CPU, Memory, Disk, Network and Process metrics through a corporate firewall to a free cloud dashboard with AI anomaly detection.**

[![CI](https://github.com/sirius073/PulseWatch/actions/workflows/ci.yml/badge.svg)](https://github.com/sirius073/PulseWatch/actions/workflows/ci.yml)
[![Go 1.24](https://img.shields.io/badge/Go-1.24-blue)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## 🚀 Monitor Your Own PC in 3 Steps (No Config, No Port Forwarding)

### Step 1 — Deploy PulseWatch to Render.com (free, one time)

1. Fork this repo → go to [render.com](https://render.com) → **New → Blueprint**
2. Connect your forked repo — Render reads `render.yaml` automatically
3. Click **Apply** → Render provisions a free PostgreSQL DB + deploys the container
4. Your dashboard URL will be: `https://pulsewatch-xxxx.onrender.com`

> **First build takes ~5 minutes.** Subsequent deploys are ~60 seconds.

---

### Step 2 — Download the Agent & Run It

Go to [**GitHub Releases**](https://github.com/sirius073/PulseWatch/releases) → download for your OS:

| OS | File |
|----|------|
| Windows 10/11 | `agent-windows-amd64.exe` |
| Linux x86_64 | `agent-linux-amd64` |
| Linux ARM64 | `agent-linux-arm64` |
| macOS Apple Silicon | `agent-darwin-arm64` |

```powershell
# Windows — point at your deployed Render URL
.\agent-windows-amd64.exe -server https://pulsewatch-xxxx.onrender.com

# Linux / macOS
chmod +x ./agent-linux-amd64
./agent-linux-amd64 -server https://pulsewatch-xxxx.onrender.com

# Multiple PCs with custom names
./agent-linux-amd64 -server https://pulsewatch-xxxx.onrender.com -host-id gaming-rig
```

**Agent flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `-server` | `localhost:50051` | Server address — `host:50051` for gRPC OR `https://...` for HTTP |
| `-host-id` | hostname | Custom name shown in dashboard |
| `-wal-dir` | `wal_data` | Local write-ahead log directory |

---

### Step 3 — Open Your Dashboard

Visit **`https://pulsewatch-xxxx.onrender.com`**

Your PC appears in the host grid within seconds. Click it to see:

| Panel | What you see |
|-------|-------------|
| **CPU** | Live % per core + overall, 1-minute trend |
| **Memory** | Used / available GB, real-time |
| **Disk** | Read/write MB/s per mount |
| **Network** | Sent/received bytes/s per interface |
| **Processes** | Top-10 by CPU/memory, grouped by executable |
| **Anomaly Feed** | Z-Score / EWMA / MAD alerts when a metric spikes |
| **AI Forecast** | ML runway predictor — "disk full in ~14 days" |

> **Cold start note:** Render's free tier spins down after 15 min of no traffic. The agent buffers data to its local WAL and replays automatically when the server wakes (~30 seconds).

---

## How It Works

```
Your PC                              Render.com (free)
─────────────────────────────────    ────────────────────────────────────
agent.exe                            pulsewatch-server :8080
  │                                       │
  ├─ Collects every 3 seconds:            │
  │   CPU, Mem, Disk, Net, Procs         │
  │                                       │
  ├─ POST /api/v1/ingest/batch ─────────►│ ← works through any firewall
  │   (standard HTTPS, port 443)         ├─ 8-worker pool → PostgreSQL
  │                                       ├─ Alert engine (Z-Score/EWMA/MAD)
  │◄─ { status:"ok", ack_seq:N } ────────┤
  │                                       │
  │  Server offline / cold starting?      │  Python ML Sidecar :8000
  ├─ WAL.Append → wal_data/ (disk)       │  ├─ Runway Forecaster
  ├─ Exponential backoff (0.5s→15s)     │  └─ Isolation Forest
  └─ Auto-replay when server responds   │
                                         │
Browser ─────────────────────────────►  │  React Dashboard (SSE live stream)
```

**Transport is auto-detected:**
- `-server host:50051` → gRPC (for local / Docker setups)
- `-server https://...` → standard HTTPS (for Render / any internet deploy)

No code change needed. The same agent binary works both ways.

---

## Reliability Guarantees

| Property | How |
|----------|-----|
| **Zero data loss** across server restarts | Agent-side WAL + deferred acks (ack only after DB commit) |
| **No duplicate writes** after WAL replay | `agent_sequences` table — `ON CONFLICT DO NOTHING` |
| **No memory blowup** under load | Bounded `buffer.Queue` (5,000-cap channel, back-pressure shedding) |
| **No metric cardinality explosion** | Process collector groups by exe name, never PID |
| **Correct rollup math** | Stores `sum_val/count_val/min_val/max_val` — never avg-of-avgs |

---

## What the Dashboard Shows

### Host Grid
All connected PCs with live status (online / offline), OS, arch, and agent version.

### Live Charts (uPlot — fast canvas rendering)
CPU total %, per-core breakdown, memory used GB, disk read/write MB/s, network in/out — all streaming via Server-Sent Events at 3-second cadence.

### Anomaly Detection (3 algorithms running in parallel)
- **Z-Score** — detects sudden single-point spikes (e.g. CPU jumps from 20% → 95%)
- **EWMA** — detects sustained trend shifts (e.g. memory slowly climbing over hours)
- **MAD** (Median Absolute Deviation) — robust to outliers, catches intermittent spikes

### AI Runway Forecaster (Python ML sidecar)
- Fits an ARIMA-style model on the last 24h of data
- Predicts: *"At current growth rate, disk will be full in ~14 days"*
- Also runs Multivariate Isolation Forest for cross-metric anomaly scoring

### Alert Feed
Real-time list of triggered alerts with metric name, host, severity, and timestamp.

---

## Running Locally (Docker)

If you want to run the full stack on your own machine:

```bash
# 1. Start TimescaleDB
cd deploy && docker compose up -d

# 2. Apply migrations
docker exec -i pulsewatch-timescaledb psql -U pulsewatch -d pulsewatch < server/migrations/001_init.sql
docker exec -i pulsewatch-timescaledb psql -U pulsewatch -d pulsewatch < server/migrations/002_rollups.sql

# 3. Build
go build -o bin/pulsewatch-server ./server/cmd/pulsewatch-server/
go build -o bin/agent ./agent/cmd/agent/

# 4. Run server
.\bin\pulsewatch-server.exe   # Windows
./bin/pulsewatch-server       # Linux

# 5. Run agent (gRPC mode, local)
.\bin\agent.exe               # Windows
./bin/agent                   # Linux

# 6. Open dashboard
start http://localhost:8080   # Windows
xdg-open http://localhost:8080 # Linux

# 7. Run Python ML sidecar (optional)
pip install -r sidecar/requirements.txt
python -m uvicorn sidecar.main:app --host 0.0.0.0 --port 8000
```

---

## Project Structure

```
.
├── agent/                        # Lightweight Go metric agent
│   ├── cmd/agent/main.go         # Entry point (CLI flags: -server, -host-id)
│   └── internal/
│       ├── collector/            # CPU, memory, disk, network, process
│       ├── sender/               # gRPC + HTTP dual-mode transport
│       └── wal/                  # Write-ahead log (segment-rolling, CRC32)
│
├── server/                       # Ingest & query server (Go)
│   ├── cmd/pulsewatch-server/    # Entry point (gRPC :50051, HTTP :8080)
│   └── internal/
│       ├── alert/                # Alert engine + state machine
│       ├── buffer/               # Bounded queue, 8 workers, deferred ack
│       ├── detect/               # Z-Score, EWMA, MAD anomaly detectors
│       ├── ingest/               # gRPC service implementation
│       ├── reaper/               # Offline host detection
│       ├── storage/              # pgx/v5 — writes, dedup, rollup queries
│       └── telemetry/            # Prometheus /metrics endpoint
│
├── sidecar/                      # Python ML sidecar (FastAPI)
│   ├── main.py                   # /predict/runway, /predict/anomaly
│   └── tests/                    # pytest suite (10 tests)
│
├── frontend/                     # React + uPlot dashboard (Vite)
│   └── src/
│       ├── App.tsx               # Host grid + routing
│       └── components/           # LiveChart, AlertFeed, HostCard
│
├── proto/v1/                     # Protobuf definitions + generated Go
│
├── deploy/                       # Production deployment
│   ├── Dockerfile                # 3-stage: Go + Node + Python
│   ├── supervisord.conf          # Process manager config
│   ├── entrypoint.sh             # Migration runner + start
│   ├── docker-compose.yml        # Local dev (TimescaleDB)
│   └── .env.example
│
├── render.yaml                   # Render.com Blueprint (1-click deploy)
├── .github/workflows/
│   ├── ci.yml                    # Build + lint + test on every push
│   └── release.yml               # Cross-compile agent on git tag v*
└── server/migrations/
    ├── 001_init.sql              # Schema, indexes
    └── 002_rollups.sql           # Continuous aggregates + retention
```

---

## Chaos Test Results

```
Scenario: Server SIGKILL mid-stream
─────────────────────────────────
Agent WAL spooled: 3,068 bytes (11 batches)
Server restart:    all batches replayed
DB result:         last_acked_seq=11, total_points=187

Zero data loss ✅   Zero duplicates ✅
```

---

## Render Free Tier — What to Expect

| Aspect | Detail |
|--------|--------|
| Cost | \$0 — free web service + free PostgreSQL |
| RAM | 512 MB (server + sidecar fits in ~200 MB) |
| Cold start | ~30 sec after 15 min of no HTTP traffic |
| DB storage | 1 GB free (~6 months of 3s cadence metrics) |
| Egress | 100 GB/month (more than enough for personal use) |
| TimescaleDB | Not available — plain Postgres 16 used instead |

---

## License

MIT
