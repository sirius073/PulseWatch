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

export interface Rule {
  id: string
  name: string
  host_id?: string
  metric_name: string
  condition: string
  threshold: number
  detector?: string
  duration: number
  severity: 'critical' | 'warning' | 'info'
  webhook_url?: string
  created_at: string
}

export interface AlertInstance {
  id: string
  rule_id: string
  rule_name: string
  host_id: string
  metric_name: string
  state: 'normal' | 'pending' | 'firing' | 'resolved'
  current_value: number
  threshold: number
  severity: string
  message: string
  first_trigger: string
  fired_at?: string
  resolved_at?: string
  last_updated: string
}

export interface Notification {
  alert_id: string
  rule_name: string
  host_id: string
  state: string
  severity: string
  message: string
  timestamp: string
}

export interface AlertsResponse {
  active: AlertInstance[]
  history: AlertInstance[]
  notifications: Notification[]
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

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(BASE + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`API ${path} → ${res.status}: ${text}`)
  }
  return res.json() as Promise<T>
}

async function del<T>(path: string): Promise<T> {
  const res = await fetch(BASE + path, { method: 'DELETE' })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`API ${path} → ${res.status}: ${text}`)
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

/** Fetch all alert rules. */
export async function fetchRules(): Promise<Rule[]> {
  return get('/api/v1/rules')
}

/** Create or update an alert rule. */
export async function createRule(rule: Partial<Rule> & { duration_sec?: number }): Promise<{ status: string; rule: Rule }> {
  return post('/api/v1/rules', rule)
}

/** Delete an alert rule by ID. */
export async function deleteRule(id: string): Promise<{ status: string; deleted: boolean }> {
  return del(`/api/v1/rules?id=${encodeURIComponent(id)}`)
}

/** Fetch active alerts, history, and notifications. */
export async function fetchAlerts(): Promise<AlertsResponse> {
  return get('/api/v1/alerts')
}
