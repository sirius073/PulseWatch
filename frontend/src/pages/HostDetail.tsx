// HostDetail.tsx — live metrics dashboard for a single host.
//
// Shows CPU, memory, disk, and network charts with real-time SSE updates.
// Each chart is powered by useMetricStream (SSE hook) → MetricChart (uPlot).
//
// Layout:
//   ← Back   [hostname]   [online badge]
//   ┌─────────────┐  ┌─────────────┐
//   │ CPU %        │  │ Memory %    │
//   │ <uPlot>      │  │ <uPlot>     │
//   └─────────────┘  └─────────────┘
//   ┌─────────────┐  ┌─────────────┐
//   │ Disk I/O     │  │ Network     │
//   │ <uPlot>      │  │ <uPlot>     │
//   └─────────────┘  └─────────────┘

import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { fetchHosts, type Host } from '../services/api'
import { useMetricStream } from '../services/stream'
import { MetricChart } from '../components/MetricChart'

// ─── Per-chart config ─────────────────────────────────────────────────────────

interface ChartConfig {
  metric: string
  label: string
  unit: string
  color: string
  formatValue: (v: number) => string
}

const CHARTS: ChartConfig[] = [
  {
    metric: 'system.cpu.total',
    label: 'CPU',
    unit: '%',
    color: '#58a6ff',
    formatValue: v => `${v.toFixed(1)}%`,
  },
  {
    metric: 'system.memory.used_percent',
    label: 'Memory',
    unit: '%',
    color: '#3fb950',
    formatValue: v => `${v.toFixed(1)}%`,
  },
  {
    metric: 'system.disk.used_percent',
    label: 'Disk Usage',
    unit: '%',
    color: '#d29922',
    formatValue: v => `${v.toFixed(1)}%`,
  },
  {
    metric: 'system.network.bytes_sent',
    label: 'Net Sent',
    unit: 'B/s',
    color: '#bc8cff',
    formatValue: v => formatBytes(v),
  },
]

function formatBytes(b: number): string {
  if (b < 1024) return `${b.toFixed(0)} B`
  if (b < 1024 ** 2) return `${(b / 1024).toFixed(1)} KB`
  if (b < 1024 ** 3) return `${(b / 1024 ** 2).toFixed(1)} MB`
  return `${(b / 1024 ** 3).toFixed(2)} GB`
}

// ─── Single chart panel (own SSE stream) ─────────────────────────────────────

function LiveChartPanel({ hostId, cfg }: { hostId: string; cfg: ChartConfig }) {
  const { points, latest, connected } = useMetricStream({
    host: hostId,
    metric: cfg.metric,
    maxPoints: 120,
    intervalSec: 5,
  })

  return (
    <div className="card chart-card">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 8 }}>
        <h4>{cfg.label}</h4>
        <div>
          {connected && <span className="live-dot" />}
          {latest != null ? (
            <span className="chart-value">
              {cfg.formatValue(latest.value)}
            </span>
          ) : (
            <span style={{ color: 'var(--muted)', fontSize: 13 }}>waiting…</span>
          )}
        </div>
      </div>
      {points.length > 1 ? (
        <MetricChart
          points={points}
          label={cfg.label}
          unit={cfg.unit}
          color={cfg.color}
          height={150}
        />
      ) : (
        <div style={{ height: 150, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--muted)' }}>
          Collecting data…
        </div>
      )}
    </div>
  )
}

// ─── Page component ───────────────────────────────────────────────────────────

export function HostDetail() {
  const { hostId } = useParams<{ hostId: string }>()
  const navigate = useNavigate()
  const [host, setHost] = useState<Host | null>(null)
  const [notFound, setNotFound] = useState(false)

  useEffect(() => {
    if (!hostId) return
    fetchHosts().then(data => {
      const found = (data.value ?? []).find(h => h.host_id === decodeURIComponent(hostId))
      if (found) setHost(found)
      else setNotFound(true)
    }).catch(() => setNotFound(true))
  }, [hostId])

  if (notFound) {
    return (
      <div className="page">
        <button className="back-btn" onClick={() => navigate('/')}>← Back</button>
        <p className="error-msg">Host not found.</p>
      </div>
    )
  }

  if (!host) {
    return <div className="loading">Loading host…</div>
  }

  const decodedId = decodeURIComponent(hostId!)

  return (
    <div className="page">
      <button className="back-btn" onClick={() => navigate('/')}>← Back</button>

      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20 }}>
        <h2 style={{ fontSize: 20 }}>{host.hostname}</h2>
        <span className={`badge badge-${host.status}`}>{host.status}</span>
        <span style={{ color: 'var(--muted)', fontSize: 12 }}>
          {host.os} / {host.arch} · agent {host.agent_version}
        </span>
      </div>

      <div className="charts-grid">
        {CHARTS.map(cfg => (
          <LiveChartPanel key={cfg.metric} hostId={decodedId} cfg={cfg} />
        ))}
      </div>
    </div>
  )
}
