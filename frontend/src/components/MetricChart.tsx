// MetricChart.tsx — a reusable uPlot time-series chart component.
//
// Why uPlot?
//   • Renders 100k+ points in <50ms — far faster than Chart.js or Recharts.
//   • Tiny bundle (~40 KB). No canvas abstraction overhead.
//   • Used by Grafana for high-frequency time-series.
//
// This component manages the uPlot instance lifecycle:
//   mount → create instance → on data change, setData() (not re-create)
//   unmount → destroy instance
//
// Re-creating a uPlot instance on every data update is expensive and causes
// flicker. We use a ref to hold the instance and call setData() instead.

import { useEffect, useRef } from 'react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import type { LivePoint } from '../services/stream'

interface MetricChartProps {
  points: LivePoint[]
  label: string
  unit?: string
  color?: string
  height?: number
}

export function MetricChart({
  points,
  label,
  unit = '',
  color = '#58a6ff',
  height = 160,
}: MetricChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)

  // Build uPlot data format: [timestamps_array, values_array]
  // uPlot uses Unix seconds (not ms) for the time axis.
  function toUPlotData(pts: LivePoint[]): uPlot.AlignedData {
    const xs = pts.map(p => new Date(p.ts).getTime() / 1000)
    const ys = pts.map(p => p.value)
    return [xs, ys]
  }

  // Create or update the plot.
  useEffect(() => {
    if (!containerRef.current) return
    const data = toUPlotData(points)

    if (plotRef.current) {
      // Instance already exists — just update data (no flicker).
      plotRef.current.setData(data)
      return
    }

    // First mount: create the uPlot instance.
    const opts: uPlot.Options = {
      title: '',
      width: containerRef.current.clientWidth,
      height,
      scales: {
        x: { time: true },
        y: { auto: true },
      },
      axes: [
        {
          // x-axis: minimal tick labels
          stroke: '#8b949e',
          grid: { stroke: '#21262d', width: 1 },
          ticks: { stroke: '#21262d' },
        },
        {
          // y-axis
          stroke: '#8b949e',
          grid: { stroke: '#21262d', width: 1 },
          ticks: { stroke: '#21262d' },
          size: 60,
          values: (_self, ticks) =>
            ticks.map(v => (v == null ? '' : `${v.toFixed(1)}${unit ? ' ' + unit : ''}`)),
        },
      ],
      series: [
        {}, // x series (time) — no visual
        {
          label,
          stroke: color,
          width: 2,
          fill: color + '18', // 10% alpha fill under line
          points: { show: false }, // hide individual dots for dense data
        },
      ],
      cursor: {
        // Show crosshair + tooltip values on hover
        drag: { x: true, y: false },
      },
    }

    plotRef.current = new uPlot(opts, data, containerRef.current)
  }, [points, label, unit, color, height])

  // Resize observer: keeps the chart width in sync with container.
  useEffect(() => {
    if (!containerRef.current) return
    const ro = new ResizeObserver(entries => {
      const w = entries[0].contentRect.width
      if (plotRef.current && w > 0) {
        plotRef.current.setSize({ width: w, height })
      }
    })
    ro.observe(containerRef.current)
    return () => ro.disconnect()
  }, [height])

  // Cleanup on unmount.
  useEffect(() => {
    return () => {
      plotRef.current?.destroy()
      plotRef.current = null
    }
  }, [])

  return <div ref={containerRef} style={{ width: '100%' }} />
}
