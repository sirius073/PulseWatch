# PulseWatch 🫀

**Real-time system telemetry ingestion, live SSE streaming, and predictive monitoring built with Go, TimescaleDB, React/uPlot, and a Python (FastAPI) ML sidecar.**

[![CI](https://github.com/sirius073/PulseWatch/actions/workflows/ci.yml/badge.svg)](https://github.com/sirius073/PulseWatch/actions/workflows/ci.yml)
[![Go 1.24](https://img.shields.io/badge/Go-1.24-blue)](https://golang.org)

---

## Why This Project Exists

PulseWatch monitors host metrics in real time using a local agent, a Go ingest/query backend backed by TimescaleDB, a React + uPlot frontend over Server-Sent Events (SSE), and an optional Python ML sidecar.

Core design choices:
- **Durable ingestion:** Local write-ahead log (`wal_data/`) with sequence tracking (`agent_sequences`, `ON CONFLICT DO NOTHING`) so restarts or connection drops do not lose or duplicate data.
- **Bounded back-pressure:** Bounded queue, worker pool, and deferred acknowledgments after persistence.
- **Safe rollups & cardinality:** Process metrics group by executable name rather than PID, and TimescaleDB stores aggregate components (`sum`, `count`, `min`, `max`) for continuous rollups.
- **Separation of live streaming & ML:** Live charts stream via SSE, while the FastAPI sidecar computes capacity runway forecasts and Isolation Forest anomaly scores.

---

## How It Works

```text
Go Agent                        Go Server (:50051 / :8080)          UI & Python ML Sidecar (:8000)
────────                        ──────────────────────────          ──────────────────────────────
Collects every 3s:              ├─ gRPC (:50051) & HTTP (:8080)     ├─ React + Vite + uPlot (SSE)
CPU, Mem, Disk, Net, Procs      ├─ Bounded queue + workers          └─ FastAPI sidecar (:8000)
  │                             ├─ TimescaleDB + rollups               ├─ /predict/runway
  ├─── gRPC or HTTP batch ─────►├─ Z-Score / EWMA / MAD / Alerts       └─ /predict/anomalies
  │                             └─ Offline host reaper
  └─ On failure: WAL + retry
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
- **Host Detail (`/hosts/:hostId`)**: Live SSE uPlot charts (CPU, Memory, Disk Usage, Net Sent) and the **AI Runway Forecast** card when runway data is returned.

### 6. Run Python ML Sidecar (Optional)
```powershell
pip install -r .\sidecar\requirements.txt
python -m sidecar.main
```

Endpoints on the sidecar:
- `GET /predict/runway` — Capacity runway estimate (defaults to `system.disk.used_percent`)
- `GET /predict/anomalies/host/{host_id}` and `POST /predict/anomalies` — Isolation Forest scoring over `cpu`, `memory`, `disk`, and `net_sent`

---

## Future Target: Cloud Deployment (Render)

Not deployed yet; `render.yaml` and `deploy/` are kept for a potential future Render deployment using `.\agent.exe -server https://<your-app>.onrender.com`.