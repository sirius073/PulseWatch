package sender

import (
	"context"
	"fmt"
	"log"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Sender manages network communication with the PulseWatch Ingest Server.
type Sender struct {
	client     pulsewatchv1.MetricsIngestServiceClient
	conn       *grpc.ClientConn
	hostID     string
	currentSeq uint64
}

// New constructs a new Sender connected to the given gRPC target address.
func New(target string, hostID string) (*Sender, error) {
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial ingest server %s: %w", target, err)
	}

	client := pulsewatchv1.NewMetricsIngestServiceClient(conn)
	return &Sender{
		client: client,
		conn:   conn,
		hostID: hostID,
	}, nil
}

// Close gracefully closes the underlying gRPC connection.
func (s *Sender) Close() error {
	return s.conn.Close()
}

// NextSeq increments and returns the next monotonic sequence number for this host.
func (s *Sender) NextSeq() uint64 {
	s.currentSeq++
	return s.currentSeq
}

// SendBatch packages fresh points with a new sequence number and sends them.
func (s *Sender) SendBatch(ctx context.Context, points []*pulsewatchv1.MetricPoint) (*pulsewatchv1.BatchAck, *pulsewatchv1.MetricBatch, error) {
	if len(points) == 0 {
		return nil, nil, nil
	}

	seq := s.NextSeq()
	batch := &pulsewatchv1.MetricBatch{
		HostId:       s.hostID,
		Seq:          seq,
		SentAtUnixMs: time.Now().UnixMilli(),
		Points:       points,
	}

	ack, err := s.SendPreparedBatch(ctx, batch)
	return ack, batch, err
}

// SendPreparedBatch transmits a pre-formed batch (used for fresh dispatches and WAL replays).
func (s *Sender) SendPreparedBatch(ctx context.Context, batch *pulsewatchv1.MetricBatch) (*pulsewatchv1.BatchAck, error) {
	ack, err := s.client.IngestBatch(ctx, batch)
	if err != nil {
		return nil, fmt.Errorf("rpc IngestBatch failed: %w", err)
	}

	if ack.Status == pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED {
		log.Printf("[Agent] Batch seq=%d committed (%d points)", batch.Seq, len(batch.Points))
	} else if ack.Status == pulsewatchv1.AckStatus_ACK_STATUS_DUPLICATE {
		log.Printf("[Agent] Batch seq=%d recognized as duplicate by server (safe ack)", batch.Seq)
	} else {
		log.Printf("[Agent] Batch seq=%d ack status: %v (%s)", batch.Seq, ack.Status, ack.ErrorMessage)
	}

	return ack, nil
}

// SendHeartbeat transmits a host heartbeat with system metadata.
func (s *Sender) SendHeartbeat(ctx context.Context, hostname, osName, arch, version string) (*pulsewatchv1.HeartbeatResponse, error) {
	req := &pulsewatchv1.HeartbeatRequest{
		HostId:           s.hostID,
		Hostname:         hostname,
		Os:               osName,
		Arch:             arch,
		AgentVersion:     version,
		TimestampUnixMs: time.Now().UnixMilli(),
	}
	return s.client.SendHeartbeat(ctx, req)
}
