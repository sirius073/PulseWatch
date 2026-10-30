package collector

import (
	"context"
	"fmt"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"

	"github.com/shirou/gopsutil/v4/disk"
)

// DiskCollector gathers filesystem usage and block I/O statistics.
type DiskCollector struct{}

// NewDiskCollector constructs a new disk collector.
func NewDiskCollector() *DiskCollector {
	return &DiskCollector{}
}

func (d *DiskCollector) Name() string {
	return "disk"
}

func (d *DiskCollector) Collect(ctx context.Context) ([]*pulsewatchv1.MetricPoint, error) {
	nowMs := time.Now().UnixMilli()
	var points []*pulsewatchv1.MetricPoint

	// 1. Filesystem partition capacity
	partitions, err := disk.PartitionsWithContext(ctx, false)
	if err == nil {
		seenMounts := make(map[string]bool)
		for _, part := range partitions {
			if seenMounts[part.Mountpoint] || part.Fstype == "squashfs" {
				continue
			}
			seenMounts[part.Mountpoint] = true

			usage, err := disk.UsageWithContext(ctx, part.Mountpoint)
			if err != nil || usage.Total == 0 {
				continue
			}

			labels := map[string]string{
				"mount":  part.Mountpoint,
				"device": part.Device,
			}

			points = append(points,
				&pulsewatchv1.MetricPoint{
					Name:            "system.disk.total_bytes",
					Value:           float64(usage.Total),
					TimestampUnixMs: nowMs,
					Labels:          labels,
				},
				&pulsewatchv1.MetricPoint{
					Name:            "system.disk.used_bytes",
					Value:           float64(usage.Used),
					TimestampUnixMs: nowMs,
					Labels:          labels,
				},
				&pulsewatchv1.MetricPoint{
					Name:            "system.disk.used_percent",
					Value:           usage.UsedPercent,
					TimestampUnixMs: nowMs,
					Labels:          labels,
				},
				&pulsewatchv1.MetricPoint{
					Name:            "system.disk.free_bytes",
					Value:           float64(usage.Free),
					TimestampUnixMs: nowMs,
					Labels:          labels,
				},
			)
		}
	}

	// 2. Block I/O counters
	ioCounters, err := disk.IOCountersWithContext(ctx)
	if err == nil {
		for devName, io := range ioCounters {
			labels := map[string]string{"device": devName}
			points = append(points,
				&pulsewatchv1.MetricPoint{
					Name:            "system.disk.read_bytes",
					Value:           float64(io.ReadBytes),
					TimestampUnixMs: nowMs,
					Labels:          labels,
				},
				&pulsewatchv1.MetricPoint{
					Name:            "system.disk.write_bytes",
					Value:           float64(io.WriteBytes),
					TimestampUnixMs: nowMs,
					Labels:          labels,
				},
			)
		}
	}

	if len(points) == 0 && err != nil {
		return nil, fmt.Errorf("failed collecting disk metrics: %w", err)
	}

	return points, nil
}
