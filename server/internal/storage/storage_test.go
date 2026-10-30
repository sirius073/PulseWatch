package storage_test

import (
	"context"
	"testing"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"
	"pulsewatch/server/internal/storage"
)

const testConnString = "postgres://pulsewatch:pulsewatch_secret@localhost:5432/pulsewatch?sslmode=disable"

func TestStorageIngestAndQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := storage.New(ctx, testConnString)
	if err != nil {
		t.Skipf("skipping storage integration test (DB not reachable): %v", err)
		return
	}
	defer s.Close()

	hostID := "test-host-skeleton-01"
	nowMs := time.Now().UnixMilli()

	batch := &pulsewatchv1.MetricBatch{
		HostId:       hostID,
		Seq:          1,
		SentAtUnixMs: nowMs,
		Points: []*pulsewatchv1.MetricPoint{
			{
				Name:            "system.cpu.total",
				Value:           45.8,
				TimestampUnixMs: nowMs,
				Labels: map[string]string{
					"type": "total",
				},
			},
		},
	}

	// 1. Ingest batch
	status, err := s.IngestBatch(ctx, batch)
	if err != nil {
		t.Fatalf("unexpected error ingesting batch: %v", err)
	}
	if status != pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED {
		t.Fatalf("expected status ACK_STATUS_COMMITTED, got %v", status)
	}

	// 2. Query latest metric
	rec, err := s.QueryLatest(ctx, hostID, "system.cpu.total")
	if err != nil {
		t.Fatalf("failed to query latest metric: %v", err)
	}
	if rec.Value != 45.8 {
		t.Errorf("expected value 45.8, got %f", rec.Value)
	}
	if rec.Labels["type"] != "total" {
		t.Errorf("expected label type=total, got %v", rec.Labels)
	}

	// 3. Test Idempotency: Ingest the exact same sequence number
	dupStatus, err := s.IngestBatch(ctx, batch)
	if err != nil {
		t.Fatalf("unexpected error re-ingesting duplicate batch: %v", err)
	}
	if dupStatus != pulsewatchv1.AckStatus_ACK_STATUS_DUPLICATE {
		t.Errorf("expected ACK_STATUS_DUPLICATE for replayed batch, got %v", dupStatus)
	}
}
