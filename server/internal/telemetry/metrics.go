package telemetry

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// ServerMetrics holds atomic counters and gauges tracking server health and ingestion throughput.
type ServerMetrics struct {
	BatchesReceived  uint64
	PointsReceived   uint64
	BatchesCommitted uint64
	BatchesDuplicate uint64
	CommitErrors     uint64
	DroppedPoints    uint64
}

// Global server metrics instance
var Metrics = &ServerMetrics{}

// IncBatchesReceived increments received batches count.
func (m *ServerMetrics) IncBatchesReceived() {
	atomic.AddUint64(&m.BatchesReceived, 1)
}

// AddPointsReceived adds points to the received counter.
func (m *ServerMetrics) AddPointsReceived(n int) {
	atomic.AddUint64(&m.PointsReceived, uint64(n))
}

// IncBatchesCommitted increments committed batches count.
func (m *ServerMetrics) IncBatchesCommitted() {
	atomic.AddUint64(&m.BatchesCommitted, 1)
}

// IncBatchesDuplicate increments duplicate batches count.
func (m *ServerMetrics) IncBatchesDuplicate() {
	atomic.AddUint64(&m.BatchesDuplicate, 1)
}

// IncCommitErrors increments database commit errors count.
func (m *ServerMetrics) IncCommitErrors() {
	atomic.AddUint64(&m.CommitErrors, 1)
}

// AddDroppedPoints adds to dropped points count under backpressure.
func (m *ServerMetrics) AddDroppedPoints(n int) {
	atomic.AddUint64(&m.DroppedPoints, uint64(n))
}

// Handler returns a standard Prometheus text exposition format HTTP handler.
func (m *ServerMetrics) Handler(queueDepthFunc func() int, activeHostsFunc func() int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		queueDepth := 0
		if queueDepthFunc != nil {
			queueDepth = queueDepthFunc()
		}

		activeHosts := 0
		if activeHostsFunc != nil {
			activeHosts = activeHostsFunc()
		}

		_, _ = fmt.Fprintf(w, "# HELP pulsewatch_server_ingest_batches_total Total batches received.\n")
		_, _ = fmt.Fprintf(w, "# TYPE pulsewatch_server_ingest_batches_total counter\n")
		_, _ = fmt.Fprintf(w, "pulsewatch_server_ingest_batches_total %d\n\n", atomic.LoadUint64(&m.BatchesReceived))

		_, _ = fmt.Fprintf(w, "# HELP pulsewatch_server_ingest_points_total Total individual metric points received.\n")
		_, _ = fmt.Fprintf(w, "# TYPE pulsewatch_server_ingest_points_total counter\n")
		_, _ = fmt.Fprintf(w, "pulsewatch_server_ingest_points_total %d\n\n", atomic.LoadUint64(&m.PointsReceived))

		_, _ = fmt.Fprintf(w, "# HELP pulsewatch_server_batches_committed_total Total batches successfully committed to database.\n")
		_, _ = fmt.Fprintf(w, "# TYPE pulsewatch_server_batches_committed_total counter\n")
		_, _ = fmt.Fprintf(w, "pulsewatch_server_batches_committed_total %d\n\n", atomic.LoadUint64(&m.BatchesCommitted))

		_, _ = fmt.Fprintf(w, "# HELP pulsewatch_server_batches_duplicate_total Total batches recognized as duplicate.\n")
		_, _ = fmt.Fprintf(w, "# TYPE pulsewatch_server_batches_duplicate_total counter\n")
		_, _ = fmt.Fprintf(w, "pulsewatch_server_batches_duplicate_total %d\n\n", atomic.LoadUint64(&m.BatchesDuplicate))

		_, _ = fmt.Fprintf(w, "# HELP pulsewatch_server_commit_errors_total Total database commit failures.\n")
		_, _ = fmt.Fprintf(w, "# TYPE pulsewatch_server_commit_errors_total counter\n")
		_, _ = fmt.Fprintf(w, "pulsewatch_server_commit_errors_total %d\n\n", atomic.LoadUint64(&m.CommitErrors))

		_, _ = fmt.Fprintf(w, "# HELP pulsewatch_server_queue_depth Current depth of the in-memory ingest queue.\n")
		_, _ = fmt.Fprintf(w, "# TYPE pulsewatch_server_queue_depth gauge\n")
		_, _ = fmt.Fprintf(w, "pulsewatch_server_queue_depth %d\n\n", queueDepth)

		_, _ = fmt.Fprintf(w, "# HELP pulsewatch_server_active_hosts Currently online agents reporting heartbeats.\n")
		_, _ = fmt.Fprintf(w, "# TYPE pulsewatch_server_active_hosts gauge\n")
		_, _ = fmt.Fprintf(w, "pulsewatch_server_active_hosts %d\n", activeHosts)
	}
}
