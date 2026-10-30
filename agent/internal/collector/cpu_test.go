package collector_test

import (
	"context"
	"testing"
	"time"

	"pulsewatch/agent/internal/collector"
)

func TestCPUCollector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cpuCol := collector.NewCPUCollector(100 * time.Millisecond)
	points, err := cpuCol.Collect(ctx)
	if err != nil {
		t.Fatalf("unexpected error collecting CPU metrics: %v", err)
	}

	if len(points) == 0 {
		t.Fatal("expected at least 1 CPU metric point, got 0")
	}

	// Verify system.cpu.total exists and has reasonable value
	var foundTotal bool
	for _, pt := range points {
		if pt.Name == "system.cpu.total" {
			foundTotal = true
			if pt.Value < 0.0 || pt.Value > 100.0 {
				t.Errorf("expected CPU percentage between 0 and 100, got %f", pt.Value)
			}
			if pt.TimestampUnixMs <= 0 {
				t.Errorf("invalid timestamp %d", pt.TimestampUnixMs)
			}
		}
	}

	if !foundTotal {
		t.Error("system.cpu.total metric was not found in collected points")
	}
}
