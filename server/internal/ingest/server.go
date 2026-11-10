package ingest

import (
	"context"
	"fmt"
	"log"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"
	"pulsewatch/server/internal/alert"
	"pulsewatch/server/internal/buffer"
	"pulsewatch/server/internal/storage"
	"pulsewatch/server/internal/telemetry"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the gRPC MetricsIngestService.
type Server struct {
	pulsewatchv1.UnimplementedMetricsIngestServiceServer
	store       *storage.Storage
	queue       *buffer.Queue
	alertEngine *alert.Engine
}

// NewServer constructs an ingestion server backed by the storage engine, worker queue, and alert engine.
func NewServer(store *storage.Storage, queue *buffer.Queue, alertEngine *alert.Engine) *Server {
	return &Server{
		store:       store,
		queue:       queue,
		alertEngine: alertEngine,
	}
}

// IngestBatch receives a batch of metrics from an agent.
// Under our deferred ack architecture, this method returns an Ack ONLY after
// the batch has successfully committed to the database.
func (s *Server) IngestBatch(ctx context.Context, req *pulsewatchv1.MetricBatch) (*pulsewatchv1.BatchAck, error) {
	if req.HostId == "" {
		return nil, status.Error(codes.InvalidArgument, "host_id is required")
	}
	if req.Seq == 0 {
		return nil, status.Error(codes.InvalidArgument, "seq must be greater than 0")
	}

	telemetry.Metrics.IncBatchesReceived()
	telemetry.Metrics.AddPointsReceived(len(req.Points))

	if len(req.Points) == 0 {
		return &pulsewatchv1.BatchAck{
			HostId: req.HostId,
			Seq:    req.Seq,
			Status: pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED,
		}, nil
	}

	// Route through bounded queue with deferred DB commit confirmation
	ackStatus, err := s.queue.EnqueueAndWait(ctx, req)
	if err != nil {
		log.Printf("[Ingest] error processing batch from %s (seq=%d): %v", req.HostId, req.Seq, err)
		telemetry.Metrics.IncCommitErrors()
		return &pulsewatchv1.BatchAck{
			HostId:       req.HostId,
			Seq:          req.Seq,
			Status:       pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR,
			ErrorMessage: err.Error(),
		}, nil
	}

	if ackStatus == pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED {
		telemetry.Metrics.IncBatchesCommitted()
		if s.alertEngine != nil {
			for _, pt := range req.Points {
				var ts time.Time
				if pt.TimestampUnixMs > 0 {
					ts = time.UnixMilli(pt.TimestampUnixMs)
				} else {
					ts = time.Now()
				}
				s.alertEngine.EvaluateMetric(req.HostId, pt.Name, pt.Value, ts)
			}
		}
	} else if ackStatus == pulsewatchv1.AckStatus_ACK_STATUS_DUPLICATE {
		telemetry.Metrics.IncBatchesDuplicate()
	}

	return &pulsewatchv1.BatchAck{
		HostId: req.HostId,
		Seq:    req.Seq,
		Status: ackStatus,
	}, nil
}

// SendHeartbeat handles host registration and periodic liveness heartbeats.
func (s *Server) SendHeartbeat(ctx context.Context, req *pulsewatchv1.HeartbeatRequest) (*pulsewatchv1.HeartbeatResponse, error) {
	if req.HostId == "" {
		return nil, status.Error(codes.InvalidArgument, "host_id is required")
	}

	if err := s.store.RecordHeartbeat(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to record heartbeat: %w", err)
	}

	return &pulsewatchv1.HeartbeatResponse{
		Acknowledged:     true,
		ServerTimeUnixMs: time.Now().UnixMilli(),
	}, nil
}
