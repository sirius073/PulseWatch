package collector

import (
	"context"
	"fmt"
	"strconv"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"

	"github.com/shirou/gopsutil/v4/cpu"
)

// CPUCollector collects overall and per-core CPU utilization percentages.
type CPUCollector struct {
	sampleDuration time.Duration
}

// NewCPUCollector constructs a new CPU collector.
// sampleDuration controls how long gopsutil samples CPU times (e.g. 100ms).
func NewCPUCollector(sampleDuration time.Duration) *CPUCollector {
	if sampleDuration <= 0 {
		sampleDuration = 100 * time.Millisecond
	}
	return &CPUCollector{
		sampleDuration: sampleDuration,
	}
}

func (c *CPUCollector) Name() string {
	return "cpu"
}

func (c *CPUCollector) Collect(ctx context.Context) ([]*pulsewatchv1.MetricPoint, error) {
	nowMs := time.Now().UnixMilli()
	var points []*pulsewatchv1.MetricPoint

	// 1. Total CPU Utilization Percentage
	totalPercents, err := cpu.PercentWithContext(ctx, c.sampleDuration, false)
	if err != nil {
		return nil, fmt.Errorf("failed to collect total cpu: %w", err)
	}

	if len(totalPercents) > 0 {
		points = append(points, &pulsewatchv1.MetricPoint{
			Name:            "system.cpu.total",
			Value:           totalPercents[0],
			TimestampUnixMs: nowMs,
			Labels: map[string]string{
				"type": "total",
			},
		})
	}

	// 2. Per-Core CPU Utilization Percentage
	perCorePercents, err := cpu.PercentWithContext(ctx, 0, true)
	if err == nil {
		for coreIdx, corePct := range perCorePercents {
			points = append(points, &pulsewatchv1.MetricPoint{
				Name:            "system.cpu.core",
				Value:           corePct,
				TimestampUnixMs: nowMs,
				Labels: map[string]string{
					"core": strconv.Itoa(coreIdx),
				},
			})
		}
	}

	return points, nil
}
