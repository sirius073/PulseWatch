package collector

import (
	"context"
	pulsewatchv1 "pulsewatch/proto/v1"
)

// Collector is the interface that all metric collectors (CPU, Memory, Disk, etc.) implement.
// Each collector can be executed concurrently with context timeouts to prevent any single
// slow system call from blocking the main collection loop.
type Collector interface {
	// Name returns the identifier of this collector for logging and metrics.
	Name() string

	// Collect gathers metric readings and converts them to protocol buffer MetricPoints.
	Collect(ctx context.Context) ([]*pulsewatchv1.MetricPoint, error)
}
