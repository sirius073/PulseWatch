package collector_test

import (
	"context"
	"testing"
	"time"

	"pulsewatch/agent/internal/collector"
)

func TestAllCollectors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collectors := []collector.Collector{
		collector.NewCPUCollector(50 * time.Millisecond),
		collector.NewMemoryCollector(),
		collector.NewDiskCollector(),
		collector.NewNetworkCollector(),
		collector.NewProcessCollector(5),
	}

	for _, col := range collectors {
		t.Run(col.Name(), func(t *testing.T) {
			points, err := col.Collect(ctx)
			if err != nil {
				t.Fatalf("collector %s failed: %v", col.Name(), err)
			}
			if len(points) == 0 {
				t.Logf("collector %s returned 0 points (may be expected in virtual env)", col.Name())
				return
			}

			// Validate points have non-empty name, valid timestamp, and no PID labels
			for _, pt := range points {
				if pt.Name == "" {
					t.Errorf("collector %s returned point with empty name", col.Name())
				}
				if pt.TimestampUnixMs <= 0 {
					t.Errorf("collector %s returned invalid timestamp", col.Name())
				}
				if _, hasPID := pt.Labels["pid"]; hasPID {
					t.Errorf("CRITICAL: collector %s violated cardinality rule by including 'pid' label!", col.Name())
				}
			}
		})
	}
}
