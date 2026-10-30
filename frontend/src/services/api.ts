// api.ts — typed REST client for the PulseWatch server HTTP API.
//
// All functions return plain data objects so callers (hooks, components)
// never touch fetch directly. Error handling throws so React boundaries
// or try/catch in hooks can catch them uniformly.

const BASE = '' // empty = same origin; Vite proxy maps /api → :8080

// ─── Types ────────────────────────────────────────────────────────────────────

export interface Host {
  host_id: string
  hostname: string
  os: string
  arch: string
  agent_version: string
  first_seen: string   // ISO 8601
  last_heartbeat: string
  status: 'online' | 'offline'
}

export interface DataPoint {
  ts: string    // ISO 8601
  value: number
}

export interface RangeResult {
  host_id: string
  metric_name: string
  from: string
  to: string
  step: string
  agg: string
  table: string   // "raw" | "rollup_1m"
  points: DataPoint[]
}

export interface LatestMetric {
  timestamp: string
  host_id: string
  metric_name: string
  value: number
  labels: Record<string, string>
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function get<T>(path: string): Promise<T> {
  const res = await fetch(BASE + path)
  if (!res.ok) {
    const body = await res.text()
    throw new Error(`API ${path} → ${res.status}: ${body}`)
  }
  return res.json() as Promise<T>
}

// ─── Endpoints ────────────────────────────────────────────────────────────────

/** List all hosts. */
export async function fetchHosts(): Promise<{ value: Host[]; Count: number }> {
  return get('/api/v1/hosts')
}

/** Get the latest reading of a single metric on a host. */
export async function fetchLatest(host: string, metric: string): Promise<LatestMetric> {
  return get(`/api/v1/metrics/latest?host=${encodeURIComponent(host)}&metric=${encodeURIComponent(metric)}`)
}

/** Get a time-range of metric data.
 *
 * @param from  – RFC3339 start (optional, defaults to now-1h on server)
 * @param to    – RFC3339 end   (optional, defaults to now on server)
 * @param step  – bucket size: "1m" | "5m" | "1h"
 * @param agg   – aggregation:  "avg" | "min" | "max" | "sum"
 */
export async function fetchRange(
  host: string,
  metric: string,
  from?: Date,
  to?: Date,
  step?: string,
  agg?: string,
): Promise<RangeResult> {
  const params = new URLSearchParams({
    host,
    metric,
    ...(from ? { from: from.toISOString() } : {}),
    ...(to ? { to: to.toISOString() } : {}),
    ...(step ? { step } : {}),
    ...(agg ? { agg } : {}),
  })
  return get(`/api/v1/metrics/range?${params}`)
}
