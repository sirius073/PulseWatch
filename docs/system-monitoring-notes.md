# PulseWatch: Real-Time System Monitoring Platform (Go / C++)

> Working name. A lightweight monitoring system: agents collect host metrics, a server ingests and stores them at scale, an engine detects anomalies and fires alerts, and a dashboard shows everything live.

---

## 1. What it is

A small, Prometheus/Datadog-style platform built from scratch:

1. A **lightweight agent** runs on each machine and collects CPU, memory, disk, network and process metrics.
2. Agents ship batched metrics to an **ingest server** that authenticates, validates and buffers them.
3. A **time-series store** keeps raw data and downsampled rollups with retention.
4. A **detection and alert engine** flags anomalies and threshold breaches.
5. A **dashboard** shows live charts, host health and alerts.

**Why this project:** it is your systems project. It shows concurrency, networking, storage design, backpressure, fault tolerance and performance measurement, the things Google-style SWE interviews and system design rounds probe. It pairs naturally with the code reviewer: PulseWatch can monitor that deployed service.

> Language note: **Go is the primary choice** for the agent and server (goroutines, simple static binaries, strong ecosystem for networking and cloud infra). C++ is a valid alternative for the agent only, if you want to read `/proc` directly and show low-level performance work. Avoid building the whole thing in C++, as it slows development without adding resume value here.

### Goals
- Agent uses minimal resources (target under 1% CPU and under 30 MB RAM).
- Server ingests tens of thousands of data points per second on a laptop-class machine.
- No data loss when the network or server is briefly down.
- Anomaly detection with measured precision, recall and time-to-detect.

### Non-goals for the MVP
- Multi-tenant SaaS and billing.
- Windows/macOS agents (Linux first).
- Distributed, replicated storage (mention it as future work).
- Log or trace ingestion (metrics only).

---

## 2. What users see

- **Hosts page:** list of agents with online/offline status, last seen, version.
- **Host detail:** live charts for CPU, memory, disk, network, load and top processes.
- **Query/explore view:** pick a metric, host and time range; see raw or rolled-up data.
- **Alerts page:** firing and resolved alerts, history, silencing.
- **Rules page:** create threshold and anomaly rules with a "for N minutes" condition.
- **Settings:** API keys per host group, retention, notification channels (webhook/Slack/email).

---

## 3. Tech stack

| Layer | Choice | Reason |
|---|---|---|
| Agent | Go (or C++ alternative) | Static binary, low overhead, easy concurrency |
| Wire protocol | Protobuf over gRPC (HTTP/2) or HTTP + gzip | Compact, typed, streaming-capable |
| Ingest server | Go | Concurrency, backpressure, batching |
| Buffer | In-process queue + write-ahead log (or NATS/Kafka as an upgrade) | Durability under bursts |
| Storage | TimescaleDB (Postgres) first; optional custom embedded store later | Fast to ship; custom store is a stretch project |
| Detection | Go (EWMA, z-score, MAD, seasonal baseline) + optional Python sidecar (Isolation Forest) for comparison | Honest baseline vs ML comparison |
| Query API | Go (REST/JSON) | Simple for the dashboard |
| Dashboard | React, TypeScript, Vite, uPlot or Recharts, WebSocket/SSE for live data | Fast charting |
| Infra | Docker, docker-compose, GitHub Actions, Terraform (optional) | Reproducible, deployable |
| Observability | The system monitors itself (expose its own metrics) | Dogfooding |
| Testing | `go test`, race detector, fuzzing, k6/custom load generator | Correctness and performance |

---

## 4. Architecture

```
 +---------+      batched, compressed       +----------------+
 | Agent   |  ---------------------------->  | Ingest server  |
 | (host)  |   gRPC / HTTP, API key auth     | validate, auth |
 | local   |                                 | rate limit     |
 | WAL     |  <-- config, acks ------------  +-------+--------+
 +---------+                                         |
                                                     v
                                          +----------------------+
                                          | Buffer / WAL queue   |
                                          +----------+-----------+
                                                     |
                           +-------------------------+-----------------------+
                           v                                                 v
                  +------------------+                             +-------------------+
                  | Writer           |                             | Detection / Alert |
                  | batch inserts    |                             | engine (streaming)|
                  +--------+---------+                             +---------+---------+
                           v                                                 |
                  +------------------+      +----------------+               v
                  | Time-series DB   | <--- | Rollup/Retention|     +------------------+
                  | raw + 1m/5m/1h   |      | jobs           |      | Notifier         |
                  +--------+---------+      +----------------+      | webhook/Slack    |
                           v                                         +------------------+
                  +------------------+
                  | Query API        | <---- Dashboard (REST + WebSocket/SSE)
                  +------------------+
```

---

## 5. Component deep dive

### 5.1 Agent
**Collects** (every N seconds, default 10): CPU (per core and total), memory and swap, load average, disk usage and IO, network bytes/packets/errors, top processes, uptime.
**Sources:** read `/proc` and `/sys` directly (educational, and the natural C++ path) or use a library like gopsutil in Go first, then replace hot paths.

**Behaviors that matter:**
- **Collection loop** with a ticker; each collector runs in its own goroutine with timeouts so one slow source cannot stall others.
- **Batching:** group points, compress (gzip/zstd), send every few seconds.
- **Local write-ahead log (WAL):** if the server is unreachable, append batches to disk (bounded size, oldest dropped first) and replay when it returns. This is the "no data loss" guarantee.
- **Retry with exponential backoff and jitter** so a recovering server is not stampeded.
- **Auth:** per-agent API key (upgrade: mTLS).
- **Identity:** stable host ID (generated once, persisted), plus labels (env, role, region).
- **Remote config:** server can change interval or enabled collectors; agent applies without restart.
- **Heartbeat:** lets the server mark hosts offline.
- **Graceful shutdown:** flush buffers on SIGTERM.
- **Self-limiting:** cap memory, cap log size.

### 5.2 Wire protocol
Define messages in Protobuf: `MetricPoint {name, labels, timestamp, value}`, `Batch {host_id, points[], seq}`, `Ack {seq}`. A **sequence number per agent** lets the server detect gaps and duplicates, so retries are **idempotent** (same idea as webhook idempotency in the reviewer project).

### 5.3 Ingest server
- Authenticate and validate (timestamp sanity, label cardinality limits, payload size limits).
- **Rate limiting** per agent.
- Hand batches to the buffer and ack only after durable enqueue (or choose at-most-once vs at-least-once deliberately and document it).
- **Backpressure:** if the buffer is full, return a retryable error instead of crashing.
- Track host liveness from batches and heartbeats.

### 5.4 Buffer and writer
- Bounded channel or on-disk queue between ingest and storage.
- **Writer** batches inserts (for example 5,000 rows or 1 second, whichever comes first) using COPY/bulk insert for throughput.
- Worker pool sized to DB capacity; retries on transient errors.
- Upgrade path: swap the buffer for NATS or Kafka and explain the tradeoff.

### 5.5 Storage
Schema idea (Timescale hypertable):

```
metrics(
  ts          timestamptz,
  host_id     bigint,
  metric_id   int,      -- interned metric name
  labels_hash bigint,   -- interned label set
  value       double precision
)  -- partitioned by time, indexed on (host_id, metric_id, ts)
```

- **Retention:** raw data 7 days; 1-minute rollups 30 days; 1-hour rollups 1 year.
- **Rollups:** continuous aggregates or scheduled jobs (min, max, avg, count, p95).
- **Cardinality control:** reject or cap unbounded label values; this is the classic way monitoring systems die.
- **Stretch:** write your own embedded storage (append-only segments, Gorilla-style delta-of-delta timestamp and XOR float compression, in-memory index) and benchmark it against Timescale. This single stretch goal makes the project stand out.

### 5.6 Detection and alerting
**Rule alerts:** `metric > threshold for 5m`, evaluated on a schedule against recent data.
**Anomaly detection (streaming, per host and metric):**
1. Baseline: rolling mean and standard deviation (z-score), or EWMA.
2. Robust version: median and MAD to resist outliers.
3. Seasonal baseline: compare to the same hour yesterday or last week.
4. Optional Python sidecar: Isolation Forest or a small forecasting model.

**Alert lifecycle:** `pending` (condition true, waiting for the "for" duration) then `firing` then `resolved`, with **deduplication** (one alert per fingerprint), **grouping**, **silences** and **flap protection** (hysteresis).
**Notifier:** webhook, Slack and email with retry. Make delivery idempotent.

### 5.7 Query API
- `GET /api/hosts`, `GET /api/hosts/{id}`
- `GET /api/query?metric=cpu_total&host=...&from=...&to=...&step=1m&agg=avg`
- `GET /api/alerts`, `POST /api/rules`, `POST /api/silences`
- Live stream: WebSocket or SSE for dashboard charts.
- Choose rollup tables automatically based on the requested time range.
- Timeouts, pagination and query cost limits to protect the DB.

### 5.8 Dashboard
React + TypeScript with a fast time-series chart library (uPlot handles large datasets well). Pages: Hosts, Host detail, Explore, Alerts, Rules, Settings. Include loading, error and empty states, and keyboard-accessible controls.

---

## 6. Suggested repo structure

```
pulsewatch/
├── docs/                  spec.md, architecture.md, protocol.md, schema.md
├── proto/                 metrics.proto
├── agent/                 main.go, collectors/, wal/, sender/, config/
├── server/
│   ├── cmd/pulsewatch-server/
│   ├── internal/ingest/   # auth, validation, rate limit
│   ├── internal/buffer/
│   ├── internal/storage/  # writer, queries, rollups
│   ├── internal/detect/   # zscore, ewma, mad, seasonal
│   ├── internal/alert/    # rules, lifecycle, notifier
│   └── internal/api/
├── sidecar/               # optional Python ML comparison
├── loadgen/               # simulates N agents
├── frontend/
├── deploy/                # docker-compose, Terraform
├── bench/                 # benchmark scripts and results
└── .github/workflows/ci.yml
```

---

## 7. Reliability and security

- No data loss across short outages (agent WAL plus ack protocol).
- Idempotent ingestion via per-agent sequence numbers.
- Bounded everything: queues, WAL size, payload size, label cardinality.
- Graceful degradation: shed load with retryable errors instead of falling over.
- Timeouts and context cancellation on every network call.
- TLS for transport; per-agent API keys (stretch: mTLS and key rotation).
- Agents run unprivileged and read-only; no remote command execution.
- Race detector in CI; fuzz the protocol parser.

---

## 8. Evaluation and benchmarks (your measurable results)

**Performance**
- Ingest throughput (points/sec) and p50/p95/p99 ingest latency with a load generator simulating 100, 1,000 and 5,000 agents.
- Agent overhead (CPU %, RSS memory) measured over 24 hours.
- Query latency by time range (raw vs rollup).
- Storage footprint per million points, before and after compression/rollups.

**Resilience (chaos tests)**
- Kill the server for 5 minutes; verify zero data loss after replay.
- Kill the DB; verify backpressure and recovery.
- Duplicate and reorder batches; verify idempotency.

**Detection quality**
- Inject faults with `stress-ng` (CPU spikes, memory leaks, disk fill, network loss) and labeled normal periods.
- Report **precision, recall and time-to-detect** for z-score vs EWMA vs MAD vs seasonal vs Isolation Forest, and say honestly which wins and why.

---

## 9. Testing

- Unit tests for collectors (parse sample `/proc` files), WAL, protocol encode/decode, detectors and alert state machine.
- Integration tests with docker-compose (agent, server and DB).
- Property and fuzz tests on the protocol and rule parser.
- Load tests with the generator in CI (smoke-level) and locally (full).
- Dashboard: Playwright smoke tests and accessibility checks.

---

## 10. Deployment

- Docker images for server and sidecar; agent as a static binary plus a systemd unit.
- docker-compose for local; deploy server and DB to a small cloud VM or managed container service.
- GitHub Actions: lint, vet, race tests, build, publish images and agent binaries as release artifacts.
- Run the agent on your own machines and on the deployed code reviewer, so the demo uses **real data**.
- Optional: Terraform for the cloud pieces.

---

## 11. Build milestones

| Week | Goal |
|---|---|
| 1 | Agent collects CPU/memory and prints locally; Protobuf schema; minimal server accepts batches and writes to Postgres; docker-compose; CI. |
| 2 | Agent batching, retries with backoff, WAL replay, API key auth; server validation, rate limiting, bulk writer, sequence-number idempotency. |
| 3 | Query API, rollups and retention, dashboard host list and live charts. |
| 4 | Rule alerts, alert lifecycle, notifier; z-score/EWMA/MAD detectors. |
| 5 | Load generator, benchmarks, chaos tests, detection evaluation with injected faults. |
| 6 | Polish: README with architecture diagram, demo video, optional custom storage engine or Python sidecar comparison. |

Build vertically: get one metric flowing agent to dashboard first, then widen.

---

## 12. Extra features (after the MVP)

- Custom storage engine with delta-of-delta/XOR compression, benchmarked against Timescale.
- Replace the in-process buffer with NATS or Kafka and compare.
- Distributed ingest with consistent hashing by host.
- Service discovery and auto-registration of agents.
- Prometheus-compatible `/metrics` scraping and remote-write support.
- Per-process and container (cgroup/Docker) metrics.
- Anomaly explanations ("CPU is 4.2 sigma above the weekly baseline").
- Forecasting (disk full in N days).
- Alert routing by labels and on-call schedules.
- Agent auto-update with signed binaries.
- C++ agent variant with a head-to-head resource benchmark against the Go agent.

---

## 13. Resume bullets (fill in real numbers once measured)

- Built a distributed metrics monitoring platform in **Go**: lightweight Linux agent, gRPC ingest server, time-series storage with rollups and retention, and a React/TypeScript live dashboard.
- Engineered at-least-once delivery with agent-side write-ahead logging, sequence-number idempotency, backoff with jitter and backpressure; **zero data loss** across **[N]**-minute outage tests.
- Sustained **[X]** points/sec at p99 ingest latency **[Y]** ms with **[N]** simulated agents; agent overhead **[<1% CPU, Z MB RSS]**.
- Implemented streaming anomaly detection (z-score, EWMA, MAD, seasonal) with alert lifecycle, dedupe and silencing; evaluated against stress-ng fault injection: **[P]%** precision, **[R]%** recall, **[T]** s mean time-to-detect.
- CI/CD with race detection and fuzzing; deployed and monitoring **[N]** real hosts.

Only list what is actually built and measured.

---

## 14. Interview topics this project prepares you for

- Design a metrics monitoring system (a classic system design question).
- Push vs pull collection and tradeoffs.
- At-most-once vs at-least-once vs exactly-once delivery; how idempotency fixes duplicates.
- Backpressure and load shedding.
- Why time-series data needs special storage (append-heavy, compression, rollups, retention).
- High cardinality and why it breaks monitoring systems.
- Goroutines, channels, worker pools, contexts and cancellation; avoiding goroutine leaks and data races.
- Reducing false positives in alerting (hysteresis, "for" durations, dedupe).
- Scaling to 100,000 hosts (sharding, partitioned queues, replication).

---

## 15. What you should design yourself (not delegate)

The wire protocol and delivery guarantees, the WAL and replay logic, the schema and rollup strategy, the alert state machine, the detectors, the benchmark and fault-injection harness. Use an AI assistant for boilerplate, CRUD endpoints, Docker/CI files and UI components, and read every diff it produces.

---

## 16. How it connects to the code reviewer project

Deploy ReviewPilot first, then point PulseWatch agents at its hosts. You get a real workload to monitor, a shared deployment story, and a single narrative for interviews: "I built a developer tool and the infrastructure to observe and operate it."
