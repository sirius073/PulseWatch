package api_test

import (
	"testing"
	"time"

	"pulsewatch/server/internal/api"
)

// TestTableFor verifies that the correct rollup table is selected based on
// window size. This is pure logic — no DB needed.
func TestTableFor(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		from time.Time
		to   time.Time
		want string
	}{
		{
			name: "1h window → raw",
			from: now.Add(-1 * time.Hour),
			to:   now,
			want: "raw",
		},
		{
			name: "6h window → raw (boundary)",
			from: now.Add(-6 * time.Hour),
			to:   now,
			want: "raw",
		},
		{
			name: "6h1s window → rollup_1m",
			from: now.Add(-6*time.Hour - time.Second),
			to:   now,
			want: "rollup_1m",
		},
		{
			name: "7d window → rollup_1m",
			from: now.Add(-7 * 24 * time.Hour),
			to:   now,
			want: "rollup_1m",
		},
		{
			name: "30d window → rollup_1m (capped)",
			from: now.Add(-30 * 24 * time.Hour),
			to:   now,
			want: "rollup_1m",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := api.TableFor(tc.from, tc.to)
			if got != tc.want {
				t.Errorf("TableFor(%v, %v) = %q; want %q", tc.from, tc.to, got, tc.want)
			}
		})
	}
}

// TestValidateRangeParams checks defaults and error cases.
func TestValidateRangeParams(t *testing.T) {
	t.Run("missing host → error", func(t *testing.T) {
		p := api.QueryRangeParams{MetricName: "system.cpu.total"}
		if err := api.ValidateRangeParams(&p); err == nil {
			t.Fatal("expected error for empty host")
		}
	})

	t.Run("missing metric → error", func(t *testing.T) {
		p := api.QueryRangeParams{HostID: "myhost"}
		if err := api.ValidateRangeParams(&p); err == nil {
			t.Fatal("expected error for empty metric")
		}
	})

	t.Run("window > 30d → error", func(t *testing.T) {
		now := time.Now()
		p := api.QueryRangeParams{
			HostID:     "myhost",
			MetricName: "system.cpu.total",
			From:       now.Add(-31 * 24 * time.Hour),
			To:         now,
		}
		if err := api.ValidateRangeParams(&p); err == nil {
			t.Fatal("expected error for >30d window")
		}
	})

	t.Run("bad agg → error", func(t *testing.T) {
		p := api.QueryRangeParams{
			HostID:     "myhost",
			MetricName: "system.cpu.total",
			Agg:        "median", // unsupported
		}
		if err := api.ValidateRangeParams(&p); err == nil {
			t.Fatal("expected error for bad agg")
		}
	})

	t.Run("defaults filled in", func(t *testing.T) {
		p := api.QueryRangeParams{
			HostID:     "myhost",
			MetricName: "system.cpu.total",
			// From, To, Step, Agg all zero/empty
		}
		if err := api.ValidateRangeParams(&p); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Agg != "avg" {
			t.Errorf("default Agg = %q; want avg", p.Agg)
		}
		if p.Step == "" {
			t.Error("Step should be defaulted")
		}
		if p.From.IsZero() {
			t.Error("From should be defaulted to ~now-1h")
		}
		if p.To.IsZero() {
			t.Error("To should be defaulted to ~now")
		}
	})
}
