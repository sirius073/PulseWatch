# PulseWatch 🫀

**Real-time system telemetry ingestion, SSE live streaming, and predictive monitoring built with Go, TimescaleDB, React/uPlot, and a Python (FastAPI) ML sidecar.**

[![CI](https://github.com/sirius073/PulseWatch/actions/workflows/ci.yml/badge.svg)](https://github.com/sirius073/PulseWatch/actions/workflows/ci.yml)
[![Go 1.24](https://img.shields.io/badge/Go-1.24-blue)](https://golang.org)

---

## System Problems Solved & Measurable Aspects

- **Server restarts & dropped connections mid-stream:**
  - *Problem:* In-memory or fire-and-forget metric shipping loses batches when the server restarts, crashes, or becomes unreachable, and retries can create duplicate rows.
  - *Solution & measurement:* Segment-rolling local WAL (`wal_data/`), exponential backoff (`0.5s` to `15s`), deferred acknowledgments until DB commit, and sequence idempotency (`agent_sequences` with `ON CONFLICT DO NOTHING`). In a `SIGKILL` mid-stream test, `3,068 bytes` (`11` batches) spooled to WAL and replayed on restart to `last_acked_seq=11` and `187` total points with `0` lost or duplicate batches.
- **Unbounded memory growth under bursty ingest:**
  - *Problem:* Unbuffered or unbounded ingestion channels can cause memory spikes when metric arrival exceeds DB write throughput.
  - *Solution & measurement:* Bounded `buffer.Queue` (`5,000`-item capacity with back-pressure shedding) feeding an `8`-worker write pool, collecting host metrics on a `3s` interval.
- **Metric cardinality explosion & roll-up drift:**
  - *Problem:* Tracking per-PID process series inflates time-series cardinality, and averaging pre-computed averages distorts rollups.
  - *Solution & measurement:* Process metrics aggregate by executable name rather than PID, and rollups store `sum`, `count`, `min`, and `max` for `1m` continuous aggregates (`metrics_rollup_1m`).
- **Capacity exhaustion visibility & multivariate outliers:**
  - *Problem:* Static thresholds miss slow disk growth or joint multi-metric shifts.
  - *Solution & measurement:* FastAPI sidecar (`:8000`) computes runway estimates over a default `6h` window (`system.disk.used_percent`, `100%` limit) for the Host Detail **AI Runway Forecast** card and runs Isolation Forest anomaly scoring over a default `2h` window across `cpu`, `memory`, `disk`, and `net_sent`.
- **Frontend API response shape & hook ordering bugs:**
  - *Problem:* Endpoints returning raw JSON arrays broke `.value` lookups in `Hosts.tsx` and `HostDetail.tsx`, and post-early-return hooks in `HostDetail.tsx` risked hook-order mismatches.
  - *Solution & measurement:* Normalized array responses in both pages and hoisted `runway` state/effects above early returns (`notFound` and `!host`) across `4` live SSE uPlot charts (`CPU`, `Memory`, `Disk Usage`, `Net Sent`).

---

## How It Works

```text
Go Agent                        Go Server (:50051 / :8080)          UI & Python ML Sidecar (:8000)
────────                        ──────────────────────────          ──────────────────────────────
Collects every 3s:              ├─ gRPC (:50051) & HTTP (:8080)     ├─ React + Vite + uPlot (SSE)
CPU, Mem, Disk, Net, Procs      ├─ Bounded queue (5,000)            └─ FastAPI sidecar (:8000)
  │                             ├─ 8 workers → TimescaleDB             ├─ /predict/runway (6h default)
  ├─── gRPC or HTTP batch ─────►├─ Z-Score / EWMA / MAD / Alerts       └─ /predict/anomalies (2h default)
  │                             └─ Offline host reaper
  └─ On failure: WAL (0.5s→15s)
```

- `-server host:50051` uses gRPC; `-server https://...` uses HTTP/HTTPS.

---

## Run Locally

### 1. Start TimescaleDB
```powershell
cd .\deploy
docker compose up -d
cd ..
```

### 2. Apply Migrations
```powershell
Get-Content .\server\migrations\001_init.sql | docker exec -i pulsewatch-timescaledb psql -U pulsewatch -d pulsewatch
Get-Content .\server\migrations\002_rollups.sql | docker exec -i pulsewatch-timescaledb psql -U pulsewatch -d pulsewatch
```

### 3. Build Frontend and Go Binaries
```powershell
cd .\frontend
npm install
npm run build
cd ..

go build -o .\bin\pulsewatch-server.exe .\server\cmd\pulsewatch-server\
go build -o .\bin\agent.exe .\agent\cmd\agent\
```

### 4. Start Server and Agent
```powershell
# Terminal 1: Server (HTTP on :8080, gRPC on :50051)
.\bin\pulsewatch-server.exe
```

```powershell
# Terminal 2: Agent
.\bin\agent.exe -server localhost:50051
```

Flags:
- `-server` (default `localhost:50051`): `host:50051` for gRPC or `https://...` for HTTP
- `-host-id` (default hostname): host identifier shown in the dashboard
- `-wal-dir` (default `wal_data`): local WAL directory

### 5. Open Dashboard
Open `http://localhost:8080`:
- **Hosts (`/`)**: Connected hosts, status, OS/arch, and agent version.
- **Host Detail (`/hosts/:hostId`)**: Live SSE uPlot charts (`CPU`, `Memory`, `Disk Usage`, `Net Sent`) and the **AI Runway Forecast** card when forecast data is returned.

### 6. Run Python ML Sidecar (Optional)
```powershell
pip install -r .\sidecar\requirements.txt
python -m sidecar.main
```

Endpoints on the sidecar:
- `GET /predict/runway` — Capacity runway estimate (defaults to `system.disk.used_percent`, `hours=6`, `capacity_limit=100.0`)
- `GET /predict/anomalies/host/{host_id}` and `POST /predict/anomalies` — Isolation Forest scoring (`hours=2` default on host lookup) over `cpu`, `memory`, `disk`, and `net_sent`

---

## Future Target: Cloud Deployment (Render)

Not deployed yet; `render.yaml` and `deploy/` are kept for a potential future Render deployment using `.\agent.exe -server https://<your-app>.onrender.com`.