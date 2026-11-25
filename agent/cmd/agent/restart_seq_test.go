package main

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"pulsewatch/agent/internal/sender"
	"pulsewatch/agent/internal/wal"
	pulsewatchv1 "pulsewatch/proto/v1"

	"google.golang.org/grpc"
)

type stubIngestServer struct {
	pulsewatchv1.UnimplementedMetricsIngestServiceServer
	mu         sync.Mutex
	lastAcked  uint64
	duplicates []uint64
	committed  int
}

func (s *stubIngestServer) IngestBatch(_ context.Context, req *pulsewatchv1.MetricBatch) (*pulsewatchv1.BatchAck, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Seq <= s.lastAcked {
		s.duplicates = append(s.duplicates, req.Seq)
		return &pulsewatchv1.BatchAck{
			HostId: req.HostId,
			Seq:    req.Seq,
			Status: pulsewatchv1.AckStatus_ACK_STATUS_DUPLICATE,
		}, nil
	}

	s.lastAcked = req.Seq
	s.committed++
	return &pulsewatchv1.BatchAck{
		HostId: req.HostId,
		Seq:    req.Seq,
		Status: pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED,
	}, nil
}

func (s *stubIngestServer) SendHeartbeat(_ context.Context, _ *pulsewatchv1.HeartbeatRequest) (*pulsewatchv1.HeartbeatResponse, error) {
	return &pulsewatchv1.HeartbeatResponse{Acknowledged: true}, nil
}

func TestAgentStopAndRestartPreservesSequence(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	srv := grpc.NewServer()
	stub := &stubIngestServer{}
	pulsewatchv1.RegisterMetricsIngestServiceServer(srv, stub)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	walDir := t.TempDir()
	seqPath := filepath.Join(walDir, "last_seq.txt")
	addr := lis.Addr().String()
	ctx := context.Background()

	// Phase 1: send 3 batches, then stop ("turn off") agent components.
	w1, err := wal.Open(wal.Config{Dir: walDir})
	if err != nil {
		t.Fatalf("wal.Open failed: %v", err)
	}
	s1, err := sender.New(addr, "host-restart-test")
	if err != nil {
		t.Fatalf("sender.New failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		_, batch, err := s1.SendBatch(ctx, []*pulsewatchv1.MetricPoint{{
			Name:            "system.cpu.total",
			Value:           float64(i + 1),
			TimestampUnixMs: time.Now().UnixMilli(),
		}})
		if err != nil {
			t.Fatalf("SendBatch %d failed: %v", i+1, err)
		}
		saveSeqState(seqPath, batch.Seq)
	}
	_ = s1.Close()
	_ = w1.Close()

	// Phase 2: restart agent components, recover sequence, and send a fresh batch.
	w2, err := wal.Open(wal.Config{Dir: walDir})
	if err != nil {
		t.Fatalf("wal.Open on restart failed: %v", err)
	}
	defer w2.Close()

	s2, err := sender.New(addr, "host-restart-test")
	if err != nil {
		t.Fatalf("sender.New on restart failed: %v", err)
	}
	defer s2.Close()

	initSeq, err := loadInitialSeq(w2, seqPath)
	if err != nil {
		t.Fatalf("loadInitialSeq failed: %v", err)
	}
	if initSeq != 3 {
		t.Fatalf("expected initial seq 3 after restart, got %d", initSeq)
	}
	s2.SetSeq(initSeq)

	ack, batch, err := s2.SendBatch(ctx, []*pulsewatchv1.MetricPoint{{
		Name:            "system.cpu.total",
		Value:           42.0,
		TimestampUnixMs: time.Now().UnixMilli(),
	}})
	if err != nil {
		t.Fatalf("post-restart SendBatch failed: %v", err)
	}
	if batch.Seq != 4 {
		t.Fatalf("expected post-restart batch seq 4, got %d", batch.Seq)
	}
	if ack.Status != pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED {
		t.Fatalf("expected COMMITTED status, got %v", ack.Status)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.duplicates) != 0 {
		t.Fatalf("expected no duplicate batches, got %v", stub.duplicates)
	}
	if stub.committed != 4 {
		t.Fatalf("expected 4 committed batches total, got %d", stub.committed)
	}
}
