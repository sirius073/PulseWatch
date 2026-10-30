// stream.ts — React hook for Server-Sent Events (SSE) live metric streaming.
//
// Why SSE over WebSocket?
//   • One-directional (server → client) — exactly what live charts need.
//   • Browser EventSource API handles reconnection automatically.
//   • Works over plain HTTP/1.1; no upgrade handshake.
//   • Simpler server implementation (just an HTTP handler with Flusher).

import { useEffect, useRef, useState } from 'react'

export interface LivePoint {
  ts: string    // ISO 8601
  value: number
}

interface UseMetricStreamOptions {
  host: string
  metric: string
  /** How many points to keep in the rolling buffer. Default: 120 (2 min at 1s). */
  maxPoints?: number
  /** Poll interval in seconds (passed to the server). Default: 5. */
  intervalSec?: number
  enabled?: boolean
}

interface UseMetricStreamResult {
  points: LivePoint[]
  latest: LivePoint | null
  connected: boolean
  error: string | null
}

/**
 * useMetricStream — subscribes to the SSE endpoint and maintains a rolling
 * buffer of `maxPoints` live readings. Each new data point is appended and
 * the oldest is dropped when the buffer is full.
 *
 * Usage:
 *   const { points, latest, connected } = useMetricStream({
 *     host: 'sirius-Victus',
 *     metric: 'system.cpu.total',
 *   })
 */
export function useMetricStream({
  host,
  metric,
  maxPoints = 120,
  intervalSec = 5,
  enabled = true,
}: UseMetricStreamOptions): UseMetricStreamResult {
  const [points, setPoints] = useState<LivePoint[]>([])
  const [latest, setLatest] = useState<LivePoint | null>(null)
  const [connected, setConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Keep a stable ref to maxPoints so the closure in addEventListener doesn't
  // capture a stale value if maxPoints changes at runtime.
  const maxRef = useRef(maxPoints)
  maxRef.current = maxPoints

  useEffect(() => {
    if (!enabled || !host || !metric) return

    const url =
      `/api/v1/stream/metrics?host=${encodeURIComponent(host)}` +
      `&metric=${encodeURIComponent(metric)}` +
      `&interval=${intervalSec}`

    const es = new EventSource(url)

    es.onopen = () => {
      setConnected(true)
      setError(null)
    }

    es.onmessage = (evt) => {
      try {
        const pt = JSON.parse(evt.data) as LivePoint
        setLatest(pt)
        setPoints((prev) => {
          const next = [...prev, pt]
          // Trim to the rolling window size.
          return next.length > maxRef.current
            ? next.slice(next.length - maxRef.current)
            : next
        })
      } catch {
        // Ignore parse errors for SSE keepalive comments (lines starting with ':').
      }
    }

    es.onerror = () => {
      setConnected(false)
      setError('SSE connection lost — reconnecting…')
      // EventSource reconnects automatically; we just surface the error state.
    }

    return () => {
      es.close()
      setConnected(false)
    }
  }, [host, metric, intervalSec, enabled])

  return { points, latest, connected, error }
}
