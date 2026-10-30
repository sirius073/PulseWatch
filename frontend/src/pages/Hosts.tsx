// Hosts.tsx — host inventory page.
//
// Polls /api/v1/hosts every 15 seconds and renders a card grid.
// Clicking a card navigates to HostDetail.

import { useEffect, useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { fetchHosts, type Host } from '../services/api'

function timeSince(iso: string): string {
  const secs = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (secs < 60) return `${secs}s ago`
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`
  return `${Math.floor(secs / 86400)}d ago`
}

export function Hosts() {
  const [hosts, setHosts] = useState<Host[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const navigate = useNavigate()

  const load = useCallback(async () => {
    try {
      const data = await fetchHosts()
      setHosts(data.value ?? [])
      setError(null)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  // Initial load + 15-second refresh
  useEffect(() => {
    load()
    const id = setInterval(load, 15_000)
    return () => clearInterval(id)
  }, [load])

  if (loading) return <div className="loading">Loading hosts…</div>
  if (error) return <div className="page"><p className="error-msg">⚠ {error}</p></div>

  const online = hosts.filter(h => h.status === 'online').length

  return (
    <div className="page">
      <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
        <h2 style={{ fontSize: 18 }}>Hosts</h2>
        <span style={{ color: 'var(--muted)', fontSize: 13 }}>
          {online} online · {hosts.length - online} offline
        </span>
        <span
          style={{ marginLeft: 'auto', color: 'var(--muted)', fontSize: 12 }}
          title="Refreshes every 15 seconds"
        >
          <span className="live-dot" />
          auto-refresh 15s
        </span>
      </div>

      {hosts.length === 0 ? (
        <p className="loading" style={{ marginTop: 40 }}>
          No hosts registered yet. Start the agent on a machine.
        </p>
      ) : (
        <div className="host-grid">
          {hosts.map(h => (
            <div
              key={h.host_id}
              className="card host-card"
              onClick={() => navigate(`/hosts/${encodeURIComponent(h.host_id)}`)}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <h3>{h.hostname}</h3>
                <span className={`badge badge-${h.status}`}>{h.status}</span>
              </div>
              <p className="meta">{h.os} / {h.arch} · agent {h.agent_version}</p>
              <p className="meta" style={{ marginTop: 8 }}>
                Last heartbeat: {timeSince(h.last_heartbeat)}
              </p>
              <p className="meta">
                First seen: {new Date(h.first_seen).toLocaleDateString()}
              </p>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
