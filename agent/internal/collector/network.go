package collector

import (
	"context"
	"fmt"
	"strings"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"

	"github.com/shirou/gopsutil/v4/net"
)

// NetworkCollector gathers network interface traffic, packet rates, and drop statistics.
type NetworkCollector struct{}

// NewNetworkCollector constructs a new network collector.
func NewNetworkCollector() *NetworkCollector {
	return &NetworkCollector{}
}

func (n *NetworkCollector) Name() string {
	return "network"
}

func (n *NetworkCollector) Collect(ctx context.Context) ([]*pulsewatchv1.MetricPoint, error) {
	nowMs := time.Now().UnixMilli()
	var points []*pulsewatchv1.MetricPoint

	counters, err := net.IOCountersWithContext(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("failed collecting network io: %w", err)
	}

	for _, io := range counters {
		if strings.HasPrefix(io.Name, "lo") {
			continue // Skip loopback interface to focus on real network traffic
		}

		labels := map[string]string{"interface": io.Name}

		points = append(points,
			&pulsewatchv1.MetricPoint{
				Name:            "system.network.bytes_sent",
				Value:           float64(io.BytesSent),
				TimestampUnixMs: nowMs,
				Labels:          labels,
			},
			&pulsewatchv1.MetricPoint{
				Name:            "system.network.bytes_recv",
				Value:           float64(io.BytesRecv),
				TimestampUnixMs: nowMs,
				Labels:          labels,
			},
			&pulsewatchv1.MetricPoint{
				Name:            "system.network.packets_sent",
				Value:           float64(io.PacketsSent),
				TimestampUnixMs: nowMs,
				Labels:          labels,
			},
			&pulsewatchv1.MetricPoint{
				Name:            "system.network.packets_recv",
				Value:           float64(io.PacketsRecv),
				TimestampUnixMs: nowMs,
				Labels:          labels,
			},
			&pulsewatchv1.MetricPoint{
				Name:            "system.network.errors_in",
				Value:           float64(io.Errin),
				TimestampUnixMs: nowMs,
				Labels:          labels,
			},
			&pulsewatchv1.MetricPoint{
				Name:            "system.network.errors_out",
				Value:           float64(io.Errout),
				TimestampUnixMs: nowMs,
				Labels:          labels,
			},
			&pulsewatchv1.MetricPoint{
				Name:            "system.network.drop_in",
				Value:           float64(io.Dropin),
				TimestampUnixMs: nowMs,
				Labels:          labels,
			},
			&pulsewatchv1.MetricPoint{
				Name:            "system.network.drop_out",
				Value:           float64(io.Dropout),
				TimestampUnixMs: nowMs,
				Labels:          labels,
			},
		)
	}

	return points, nil
}
