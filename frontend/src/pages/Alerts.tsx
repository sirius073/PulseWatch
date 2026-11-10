import React, { useEffect, useState, useCallback } from 'react'
import {
  fetchAlerts,
  fetchRules,
  createRule,
  deleteRule,
  type AlertInstance,
  type Rule,
  type Notification,
} from '../services/api'

export function Alerts() {
  const [activeAlerts, setActiveAlerts] = useState<AlertInstance[]>([])
  const [notifications, setNotifications] = useState<Notification[]>([])
  const [rules, setRules] = useState<Rule[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // New Rule Form State
  const [showForm, setShowForm] = useState(false)
  const [ruleName, setRuleName] = useState('')
  const [metricName, setMetricName] = useState('system.cpu.total')
  const [condition, setCondition] = useState('>')
  const [threshold, setThreshold] = useState<number>(85)
  const [detector, setDetector] = useState('zscore')
  const [durationSec, setDurationSec] = useState<number>(30)
  const [severity, setSeverity] = useState<'critical' | 'warning' | 'info'>('warning')
  const [webhookUrl, setWebhookUrl] = useState('')

  const refresh = useCallback(async () => {
    try {
      const [alertsData, rulesData] = await Promise.all([
        fetchAlerts(),
        fetchRules(),
      ])
      setActiveAlerts(alertsData.active ?? [])
      setNotifications(alertsData.notifications ?? [])
      setRules(rulesData ?? [])
      setError(null)
    } catch (err) {
      setError(String(err))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    refresh()
    const interval = setInterval(refresh, 5000)
    return () => clearInterval(interval)
  }, [refresh])

  const handleCreateRule = async (e: React.FormEvent) => {
    e.preventDefault()
    try {
      await createRule({
        name: ruleName,
        metric_name: metricName,
        condition,
        threshold: Number(threshold),
        detector: condition === 'anomaly' ? detector : undefined,
        duration_sec: Number(durationSec),
        severity,
        webhook_url: webhookUrl || undefined,
      })
      setShowForm(false)
      setRuleName('')
      refresh()
    } catch (err) {
      alert('Failed to create rule: ' + err)
    }
  }

  const handleDeleteRule = async (id: string) => {
    if (!confirm('Delete alert rule?')) return
    try {
      await deleteRule(id)
      refresh()
    } catch (err) {
      alert('Failed to delete rule: ' + err)
    }
  }

  if (loading) return <div className="loading">Loading alerts & rules…</div>

  return (
    <div className="page">
      <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 24 }}>
        <h2 style={{ fontSize: 20 }}>Alert Management & Live Anomaly Feed</h2>
        <span style={{ marginLeft: 'auto', color: 'var(--muted)', fontSize: 12 }}>
          <span className="live-dot" /> auto-refresh 5s
        </span>
      </div>

      {error && <p className="error-msg">⚠ {error}</p>}

      {/* Active Alerts Section */}
      <div style={{ marginBottom: 32 }}>
        <h3 style={{ fontSize: 16, marginBottom: 12 }}>
          Active Alerts ({activeAlerts.length})
        </h3>
        {activeAlerts.length === 0 ? (
          <div className="card" style={{ color: 'var(--green)', padding: '16px' }}>
            ✓ All systems operating within normal parameters. No active alerts.
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {activeAlerts.map((a) => (
              <div
                key={a.id}
                className="card"
                style={{
                  borderLeft: `4px solid ${
                    a.severity === 'critical'
                      ? 'var(--red)'
                      : a.severity === 'warning'
                      ? 'var(--yellow)'
                      : 'var(--accent)'
                  }`,
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <span
                      className={`badge ${
                        a.state === 'firing' ? 'badge-offline' : 'badge-online'
                      }`}
                    >
                      {a.state.toUpperCase()}
                    </span>
                    <strong>{a.rule_name}</strong>
                    <span style={{ color: 'var(--muted)', fontSize: 13 }}>
                      on host <code>{a.host_id}</code>
                    </span>
                  </div>
                  <span style={{ fontSize: 12, color: 'var(--muted)' }}>
                    {new Date(a.first_trigger).toLocaleTimeString()}
                  </span>
                </div>
                <p style={{ marginTop: 8, fontSize: 13 }}>{a.message}</p>
                <div style={{ marginTop: 8, fontSize: 12, color: 'var(--muted)' }}>
                  Metric: <code>{a.metric_name}</code> · Current:{' '}
                  <strong>{a.current_value.toFixed(2)}</strong> · Threshold:{' '}
                  {a.threshold}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Configured Rules Section */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <h3 style={{ fontSize: 16 }}>Configured Rules ({rules.length})</h3>
          <button
            className="back-btn"
            style={{ margin: 0, padding: '4px 12px' }}
            onClick={() => setShowForm(!showForm)}
          >
            {showForm ? 'Cancel' : '+ New Rule'}
          </button>
        </div>

        {showForm && (
          <form
            onSubmit={handleCreateRule}
            className="card"
            style={{ marginBottom: 16, display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 12 }}
          >
            <div>
              <label style={{ fontSize: 12, color: 'var(--muted)' }}>Rule Name</label>
              <input
                required
                style={{ width: '100%', padding: '6px', background: 'var(--bg)', border: '1px solid var(--border)', color: 'var(--text)', borderRadius: 4 }}
                value={ruleName}
                onChange={(e) => setRuleName(e.target.value)}
                placeholder="e.g. CPU Spike Alert"
              />
            </div>
            <div>
              <label style={{ fontSize: 12, color: 'var(--muted)' }}>Metric</label>
              <select
                style={{ width: '100%', padding: '6px', background: 'var(--bg)', border: '1px solid var(--border)', color: 'var(--text)', borderRadius: 4 }}
                value={metricName}
                onChange={(e) => setMetricName(e.target.value)}
              >
                <option value="system.cpu.total">system.cpu.total</option>
                <option value="system.memory.used_percent">system.memory.used_percent</option>
                <option value="system.disk.used_percent">system.disk.used_percent</option>
                <option value="system.network.bytes_sent">system.network.bytes_sent</option>
              </select>
            </div>
            <div>
              <label style={{ fontSize: 12, color: 'var(--muted)' }}>Condition</label>
              <select
                style={{ width: '100%', padding: '6px', background: 'var(--bg)', border: '1px solid var(--border)', color: 'var(--text)', borderRadius: 4 }}
                value={condition}
                onChange={(e) => setCondition(e.target.value)}
              >
                <option value=">">&gt; (Greater than)</option>
                <option value=">=">&gt;= (Greater or equal)</option>
                <option value="<">&lt; (Less than)</option>
                <option value="<=">&lt;= (Less or equal)</option>
                <option value="anomaly">Streaming Anomaly (AI/Stats)</option>
              </select>
            </div>
            {condition === 'anomaly' && (
              <div>
                <label style={{ fontSize: 12, color: 'var(--muted)' }}>Detector Algorithm</label>
                <select
                  style={{ width: '100%', padding: '6px', background: 'var(--bg)', border: '1px solid var(--border)', color: 'var(--text)', borderRadius: 4 }}
                  value={detector}
                  onChange={(e) => setDetector(e.target.value)}
                >
                  <option value="zscore">Z-Score (Rolling Std Dev)</option>
                  <option value="ewma">EWMA (Exponential Moving Avg)</option>
                  <option value="mad">MAD (Median Absolute Deviation)</option>
                </select>
              </div>
            )}
            <div>
              <label style={{ fontSize: 12, color: 'var(--muted)' }}>
                {condition === 'anomaly' ? 'Score Threshold' : 'Threshold'}
              </label>
              <input
                type="number"
                step="0.1"
                required
                style={{ width: '100%', padding: '6px', background: 'var(--bg)', border: '1px solid var(--border)', color: 'var(--text)', borderRadius: 4 }}
                value={threshold}
                onChange={(e) => setThreshold(Number(e.target.value))}
              />
            </div>
            <div>
              <label style={{ fontSize: 12, color: 'var(--muted)' }}>Duration (seconds)</label>
              <input
                type="number"
                min="0"
                style={{ width: '100%', padding: '6px', background: 'var(--bg)', border: '1px solid var(--border)', color: 'var(--text)', borderRadius: 4 }}
                value={durationSec}
                onChange={(e) => setDurationSec(Number(e.target.value))}
              />
            </div>
            <div>
              <label style={{ fontSize: 12, color: 'var(--muted)' }}>Severity</label>
              <select
                style={{ width: '100%', padding: '6px', background: 'var(--bg)', border: '1px solid var(--border)', color: 'var(--text)', borderRadius: 4 }}
                value={severity}
                onChange={(e) => setSeverity(e.target.value as any)}
              >
                <option value="critical">Critical</option>
                <option value="warning">Warning</option>
                <option value="info">Info</option>
              </select>
            </div>
            <div style={{ gridColumn: '1 / -1' }}>
              <label style={{ fontSize: 12, color: 'var(--muted)' }}>Webhook URL (optional)</label>
              <input
                type="url"
                style={{ width: '100%', padding: '6px', background: 'var(--bg)', border: '1px solid var(--border)', color: 'var(--text)', borderRadius: 4 }}
                value={webhookUrl}
                onChange={(e) => setWebhookUrl(e.target.value)}
                placeholder="https://hooks.slack.com/... or http://..."
              />
            </div>
            <div style={{ gridColumn: '1 / -1', marginTop: 8 }}>
              <button
                type="submit"
                className="back-btn"
                style={{ background: 'var(--accent)', color: '#0d1117', fontWeight: 600, border: 'none' }}
              >
                Save Alert Rule
              </button>
            </div>
          </form>
        )}

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: 12 }}>
          {rules.map((r) => (
            <div key={r.id} className="card" style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <strong>{r.name}</strong>
                <span
                  className="badge"
                  style={{
                    background:
                      r.severity === 'critical'
                        ? 'rgba(248,81,73,0.2)'
                        : 'rgba(210,153,34,0.2)',
                    color:
                      r.severity === 'critical' ? 'var(--red)' : 'var(--yellow)',
                  }}
                >
                  {r.severity}
                </span>
              </div>
              <div style={{ fontSize: 12, color: 'var(--muted)' }}>
                Metric: <code>{r.metric_name}</code>
              </div>
              <div style={{ fontSize: 12 }}>
                Condition:{' '}
                <code>
                  {r.condition} {r.threshold}
                  {r.detector ? ` (${r.detector})` : ''}
                </code>{' '}
                · for {r.duration ? `${Math.round(r.duration / 1e9)}s` : '0s'}
              </div>
              {r.webhook_url && (
                <div style={{ fontSize: 11, color: 'var(--muted)', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                  Webhook: {r.webhook_url}
                </div>
              )}
              <div style={{ marginTop: 'auto', paddingTop: 8 }}>
                <button
                  onClick={() => handleDeleteRule(r.id)}
                  style={{
                    background: 'none',
                    border: 'none',
                    color: 'var(--red)',
                    fontSize: 12,
                    cursor: 'pointer',
                    padding: 0,
                  }}
                >
                  Delete Rule
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Recent Notifications / History */}
      <div>
        <h3 style={{ fontSize: 16, marginBottom: 12 }}>
          Recent Notifications ({notifications.length})
        </h3>
        {notifications.length === 0 ? (
          <div className="card" style={{ color: 'var(--muted)', fontSize: 13 }}>
            No notifications generated yet.
          </div>
        ) : (
          <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ background: 'var(--bg)', borderBottom: '1px solid var(--border)', textAlign: 'left' }}>
                  <th style={{ padding: '8px 12px' }}>Timestamp</th>
                  <th style={{ padding: '8px 12px' }}>State</th>
                  <th style={{ padding: '8px 12px' }}>Rule</th>
                  <th style={{ padding: '8px 12px' }}>Host</th>
                  <th style={{ padding: '8px 12px' }}>Message</th>
                </tr>
              </thead>
              <tbody>
                {notifications.slice(-15).reverse().map((n, idx) => (
                  <tr key={idx} style={{ borderBottom: '1px solid var(--border)' }}>
                    <td style={{ padding: '8px 12px', color: 'var(--muted)', whiteSpace: 'nowrap' }}>
                      {new Date(n.timestamp).toLocaleTimeString()}
                    </td>
                    <td style={{ padding: '8px 12px' }}>
                      <span className={`badge ${n.state === 'firing' ? 'badge-offline' : 'badge-online'}`}>
                        {n.state}
                      </span>
                    </td>
                    <td style={{ padding: '8px 12px' }}>{n.rule_name}</td>
                    <td style={{ padding: '8px 12px' }}><code>{n.host_id}</code></td>
                    <td style={{ padding: '8px 12px' }}>{n.message}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
