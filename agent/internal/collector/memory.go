package collector

import (
	"context"
	"fmt"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"

	"github.com/shirou/gopsutil/v4/mem"
)

// MemoryCollector gathers virtual memory and swap utilization metrics.
type MemoryCollector struct{}

// NewMemoryCollector constructs a new memory collector.
func NewMemoryCollector() *MemoryCollector {
	return &MemoryCollector{}
}

func (m *MemoryCollector) Name() string {
	return "memory"
}

func (m *MemoryCollector) Collect(ctx context.Context) ([]*pulsewatchv1.MetricPoint, error) {
	nowMs := time.Now().UnixMilli()
	var points []*pulsewatchv1.MetricPoint

	// 1. Virtual Memory (RAM)
	vMem, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed collecting virtual memory: %w", err)
	}

	points = append(points,
		&pulsewatchv1.MetricPoint{
			Name:            "system.memory.total_bytes",
			Value:           float64(vMem.Total),
			TimestampUnixMs: nowMs,
			Labels:          map[string]string{"type": "ram"},
		},
		&pulsewatchv1.MetricPoint{
			Name:            "system.memory.used_bytes",
			Value:           float64(vMem.Used),
			TimestampUnixMs: nowMs,
			Labels:          map[string]string{"type": "ram"},
		},
		&pulsewatchv1.MetricPoint{
			Name:            "system.memory.used_percent",
			Value:           vMem.UsedPercent,
			TimestampUnixMs: nowMs,
			Labels:          map[string]string{"type": "ram"},
		},
		&pulsewatchv1.MetricPoint{
			Name:            "system.memory.available_bytes",
			Value:           float64(vMem.Available),
			TimestampUnixMs: nowMs,
			Labels:          map[string]string{"type": "ram"},
		},
	)

	// 2. Swap Space
	swap, err := mem.SwapMemoryWithContext(ctx)
	if err == nil && swap.Total > 0 {
		points = append(points,
			&pulsewatchv1.MetricPoint{
				Name:            "system.swap.total_bytes",
				Value:           float64(swap.Total),
				TimestampUnixMs: nowMs,
				Labels:          map[string]string{"type": "swap"},
			},
			&pulsewatchv1.MetricPoint{
				Name:            "system.swap.used_bytes",
				Value:           float64(swap.Used),
				TimestampUnixMs: nowMs,
				Labels:          map[string]string{"type": "swap"},
			},
			&pulsewatchv1.MetricPoint{
				Name:            "system.swap.used_percent",
				Value:           swap.UsedPercent,
				TimestampUnixMs: nowMs,
				Labels:          map[string]string{"type": "swap"},
			},
		)
	}

	return points, nil
}
