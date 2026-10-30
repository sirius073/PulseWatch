// Package api implements the HTTP query layer for PulseWatch.
// It sits between main.go and the storage layer, handling time-range
// queries, rollup selection, SSE streaming, and response marshalling.
package api

import (
	"context"
	"fmt"
	"time"
)

// ─── Types ────────────────────────────────────────────────────────────────────

// DataPoint is one value in a time-series response.
// Using float64 for value covers both integer counters and fractional gauges.
type DataPoint struct {
	Ts    time.Time `json:"ts"`
	Value float64   `json:"value"`
}

// RangeResult is the JSON envelope returned by /api/v1/metrics/range.
type RangeResult struct {
	HostID     string      `json:"host_id"`
	MetricName string      `json:"metric_name"`
	From       time.Time   `json:"from"`
	To         time.Time   `json:"to"`
	Step       string      `json:"step"`
	Agg        string      `json:"agg"`
	Points     []DataPoint `json:"points"`
}

// QueryRangeParams holds the validated inputs for a range query.
type QueryRangeParams struct {
	HostID     string
	MetricName string
	From       time.Time
	To         time.Time
	// Step is the bucket size. Supported: "1m", "5m", "1h".
	// Raw queries (window < 6 h) ignore Step and return every raw row.
	Step string
	// Agg is the aggregation function: "avg", "min", "max", "sum".
	Agg string
}

// ─── Querier interface ────────────────────────────────────────────────────────

// Querier is the subset of storage.Storage that the API layer needs.
// Keeping it as an interface makes the query handler testable without a real DB.
type Querier interface {
	QueryRange(ctx context.Context, p QueryRangeParams) ([]DataPoint, error)
	QueryLatestValue(ctx context.Context, hostID, metric string) (float64, time.Time, error)
}

// ─── Rollup selection ─────────────────────────────────────────────────────────

// windowThresholds defines when to switch from the raw hypertable to a rollup.
// Keeping this in one place makes it easy to tune without touching SQL.
const (
	rawMaxWindow     = 6 * time.Hour  // ≤6 h → query raw `metrics` table
	rollup1mMaxWindow = 7 * 24 * time.Hour // ≤7 d → query `metrics_rollup_1m`
)

// TableFor returns the SQL table/view name appropriate for the requested window.
// The caller uses this to build the right SELECT without duplicating the logic.
func TableFor(from, to time.Time) string {
	window := to.Sub(from)
	switch {
	case window <= rawMaxWindow:
		return "raw"
	case window <= rollup1mMaxWindow:
		return "rollup_1m"
	default:
		// Beyond 7 days we still use the 1-minute rollup (it covers 30 days).
		// A future Phase could add a 1-hour rollup here.
		return "rollup_1m"
	}
}

// ─── Parameter validation ─────────────────────────────────────────────────────

// ValidateRangeParams sanity-checks user-supplied query parameters and sets
// defaults so the storage layer never receives zero-values.
func ValidateRangeParams(p *QueryRangeParams) error {
	if p.HostID == "" {
		return fmt.Errorf("host is required")
	}
	if p.MetricName == "" {
		return fmt.Errorf("metric is required")
	}
	if p.From.IsZero() {
		// Default: last 1 hour
		p.From = time.Now().Add(-1 * time.Hour)
	}
	if p.To.IsZero() || p.To.Before(p.From) {
		p.To = time.Now()
	}
	if p.To.Sub(p.From) > 30*24*time.Hour {
		return fmt.Errorf("range cannot exceed 30 days")
	}
	if p.Step == "" {
		p.Step = defaultStep(p.From, p.To)
	}
	if p.Agg == "" {
		p.Agg = "avg"
	}
	switch p.Agg {
	case "avg", "min", "max", "sum":
		// ok
	default:
		return fmt.Errorf("agg must be one of: avg, min, max, sum")
	}
	return nil
}

// defaultStep picks a sensible bucket size based on the requested window.
// The goal is ~300 data points in the response — enough for a chart without
// overwhelming the browser or the DB.
func defaultStep(from, to time.Time) string {
	window := to.Sub(from)
	switch {
	case window <= time.Hour:
		return "1m" // 60 points
	case window <= 6*time.Hour:
		return "5m" // 72 points
	default:
		return "1h" // up to 168 points for 7d
	}
}
