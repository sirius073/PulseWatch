package sender

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Sender manages network communication with the PulseWatch Ingest Server.
// Supports both gRPC (port 50051) and standard HTTP/HTTPS (ports 80/443).
type Sender struct {
	isHTTP     bool
	baseURL    string
	httpClient *http.Client

	grpcClient pulsewatchv1.MetricsIngestServiceClient
	grpcConn   *grpc.ClientConn

	hostID     string
	currentSeq uint64
}

// New constructs a new Sender connected to the given target address.
// If target starts with "http://" or "https://", it operates in HTTPS mode.
// Otherwise, it initializes a gRPC channel.
func New(target string, hostID string) (*Sender, error) {
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		return &Sender{
			isHTTP:     true,
			baseURL:    strings.TrimSuffix(target, "/"),
			httpClient: &http.Client{Timeout: 10 * time.Second},
			hostID:     hostID,
		}, nil
	}

	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial ingest server %s: %w", target, err)
	}

	client := pulsewatchv1.NewMetricsIngestServiceClient(conn)
	return &Sender{
		isHTTP:     false,
		grpcClient: client,
		grpcConn:   conn,
		hostID:     hostID,
	}, nil
}

// Close gracefully closes any underlying network resources.
func (s *Sender) Close() error {
	if s.isHTTP {
		return nil
	}
	return s.grpcConn.Close()
}

// NextSeq atomically increments and returns the next monotonic sequence number for this host.
func (s *Sender) NextSeq() uint64 {
	return atomic.AddUint64(&s.currentSeq, 1)
}

// SetSeq sets the sequence baseline so the next call to NextSeq returns seq + 1.
func (s *Sender) SetSeq(seq uint64) {
	atomic.StoreUint64(&s.currentSeq, seq)
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
	if s.isHTTP {
		return s.sendPreparedBatchHTTP(ctx, batch)
	}
	return s.sendPreparedBatchGRPC(ctx, batch)
}

func (s *Sender) sendPreparedBatchGRPC(ctx context.Context, batch *pulsewatchv1.MetricBatch) (*pulsewatchv1.BatchAck, error) {
	ack, err := s.grpcClient.IngestBatch(ctx, batch)
	if err != nil {
		return nil, fmt.Errorf("rpc IngestBatch failed: %w", err)
	}

	s.logAck(batch, ack.Status, ack.ErrorMessage)
	return ack, nil
}

func (s *Sender) sendPreparedBatchHTTP(ctx context.Context, batch *pulsewatchv1.MetricBatch) (*pulsewatchv1.BatchAck, error) {
	type pointPayload struct {
		Name            string            `json:"name"`
		Value           float64           `json:"value"`
		TimestampUnixMs int64             `json:"timestamp_unix_ms"`
		Labels          map[string]string `json:"labels"`
	}

	pts := make([]pointPayload, 0, len(batch.Points))
	for _, p := range batch.Points {
		pts = append(pts, pointPayload{
			Name:            p.Name,
			Value:           p.Value,
			TimestampUnixMs: p.TimestampUnixMs,
			Labels:          p.Labels,
		})
	}

	reqBody := map[string]interface{}{
		"host_id":         batch.HostId,
		"seq":             batch.Seq,
		"sent_at_unix_ms": batch.SentAtUnixMs,
		"points":          pts,
	}

	jsonBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch: %w", err)
	}

	url := s.baseURL + "/api/v1/ingest/batch"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http POST to %s failed: %w", url, err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)

	var res struct {
		HostID string `json:"host_id"`
		Seq    uint64 `json:"seq"`
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	_ = json.Unmarshal(bodyBytes, &res)

	ackStatus := pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED
	if res.Status == "DUPLICATE" {
		ackStatus = pulsewatchv1.AckStatus_ACK_STATUS_DUPLICATE
	} else if resp.StatusCode != http.StatusOK {
		ackStatus = pulsewatchv1.AckStatus_ACK_STATUS_RETRYABLE_ERROR
	}

	ack := &pulsewatchv1.BatchAck{
		HostId:       batch.HostId,
		Seq:          batch.Seq,
		Status:       ackStatus,
		ErrorMessage: res.Error,
	}

	s.logAck(batch, ack.Status, ack.ErrorMessage)
	return ack, nil
}

func (s *Sender) logAck(batch *pulsewatchv1.MetricBatch, status pulsewatchv1.AckStatus, errMsg string) {
	if status == pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED {
		log.Printf("[Agent] Batch seq=%d committed (%d points)", batch.Seq, len(batch.Points))
	} else if status == pulsewatchv1.AckStatus_ACK_STATUS_DUPLICATE {
		log.Printf("[Agent] Batch seq=%d recognized as duplicate by server (safe ack)", batch.Seq)
	} else {
		log.Printf("[Agent] Batch seq=%d ack status: %v (%s)", batch.Seq, status, errMsg)
	}
}

// SendHeartbeat transmits a host heartbeat with system metadata.
func (s *Sender) SendHeartbeat(ctx context.Context, hostname, osName, arch, version string) (*pulsewatchv1.HeartbeatResponse, error) {
	if s.isHTTP {
		reqBody := map[string]interface{}{
			"host_id":       s.hostID,
			"hostname":      hostname,
			"os":            osName,
			"arch":          arch,
			"agent_version": version,
		}
		jsonBytes, err := json.Marshal(reqBody)
		if err != nil {
			return nil, err
		}

		url := s.baseURL + "/api/v1/ingest/heartbeat"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonBytes))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		return &pulsewatchv1.HeartbeatResponse{
			Acknowledged:     resp.StatusCode == http.StatusOK,
			ServerTimeUnixMs: time.Now().UnixMilli(),
		}, nil
	}

	req := &pulsewatchv1.HeartbeatRequest{
		HostId:          s.hostID,
		Hostname:        hostname,
		Os:              osName,
		Arch:            arch,
		AgentVersion:    version,
		TimestampUnixMs: time.Now().UnixMilli(),
	}
	return s.grpcClient.SendHeartbeat(ctx, req)
}
