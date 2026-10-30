package wal_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pulsewatch/agent/internal/wal"
	pulsewatchv1 "pulsewatch/proto/v1"
)

func createTestBatch(hostID string, seq uint64, val float64) *pulsewatchv1.MetricBatch {
	return &pulsewatchv1.MetricBatch{
		HostId:       hostID,
		Seq:          seq,
		SentAtUnixMs: time.Now().UnixMilli(),
		Points: []*pulsewatchv1.MetricPoint{
			{
				Name:            "system.cpu.total",
				Value:           val,
				TimestampUnixMs: time.Now().UnixMilli(),
				Labels: map[string]string{
					"core": "0",
				},
			},
		},
	}
}

func TestWALAppendAndReplay(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pulsewatch_wal_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	w, err := wal.Open(wal.Config{
		Dir:               tempDir,
		MaxSegmentBytes:   1024 * 1024,
		MaxTotalDiskBytes: 5 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer w.Close()

	// Append 10 batches
	for i := uint64(1); i <= 10; i++ {
		batch := createTestBatch("host-wal-test", i, float64(i)*10.0)
		if err := w.Append(batch); err != nil {
			t.Fatalf("failed to append batch %d: %v", i, err)
		}
	}

	// Replay all batches
	replayed, err := w.ReplayAll()
	if err != nil {
		t.Fatalf("failed to replay batches: %v", err)
	}

	if len(replayed) != 10 {
		t.Fatalf("expected 10 replayed batches, got %d", len(replayed))
	}

	for i, b := range replayed {
		expectedSeq := uint64(i + 1)
		if b.Seq != expectedSeq {
			t.Errorf("expected batch seq %d, got %d", expectedSeq, b.Seq)
		}
		if b.Points[0].Value != float64(expectedSeq)*10.0 {
			t.Errorf("expected metric value %f, got %f", float64(expectedSeq)*10.0, b.Points[0].Value)
		}
	}

	// Test Purge
	if err := w.PurgeAll(); err != nil {
		t.Fatalf("failed to purge wal: %v", err)
	}

	replayedAfterPurge, err := w.ReplayAll()
	if err != nil {
		t.Fatalf("failed to replay after purge: %v", err)
	}
	if len(replayedAfterPurge) != 0 {
		t.Errorf("expected 0 batches after purge, got %d", len(replayedAfterPurge))
	}
}

func TestWALSegmentRolling(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pulsewatch_wal_roll_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Small segment size to force rapid rolling
	w, err := wal.Open(wal.Config{
		Dir:               tempDir,
		MaxSegmentBytes:   250, // 250 bytes per segment
		MaxTotalDiskBytes: 10 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer w.Close()

	// Append 15 batches (will roll multiple times)
	for i := uint64(1); i <= 15; i++ {
		batch := createTestBatch("host-roll-test", i, float64(i))
		if err := w.Append(batch); err != nil {
			t.Fatalf("failed appending batch %d: %v", i, err)
		}
	}

	// Verify multiple segment files exist on disk
	entries, _ := os.ReadDir(tempDir)
	var segmentCount int
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			segmentCount++
		}
	}
	if segmentCount < 2 {
		t.Errorf("expected multiple segment files, got %d", segmentCount)
	}

	// Verify all 15 batches can be read back in exact sequence
	replayed, err := w.ReplayAll()
	if err != nil {
		t.Fatalf("failed to replay from rolled segments: %v", err)
	}
	if len(replayed) != 15 {
		t.Fatalf("expected 15 replayed batches, got %d", len(replayed))
	}
	for i, b := range replayed {
		if b.Seq != uint64(i+1) {
			t.Errorf("seq mismatch at index %d: expected %d, got %d", i, i+1, b.Seq)
		}
	}
}

func TestWALCorruptionDetection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pulsewatch_wal_corrupt_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	w, err := wal.Open(wal.Config{
		Dir:               tempDir,
		MaxSegmentBytes:   1024 * 1024,
		MaxTotalDiskBytes: 5 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	batch := createTestBatch("host-corrupt", 1, 99.9)
	if err := w.Append(batch); err != nil {
		t.Fatalf("failed to append batch: %v", err)
	}
	_ = w.Close()

	// Corrupt a byte in the segment payload
	segPath := filepath.Join(tempDir, fmt.Sprintf("segment_%010d.wal", 1))
	data, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("failed reading segment: %v", err)
	}

	// Mutate byte in the payload area
	if len(data) > 10 {
		data[10] ^= 0xFF
	}
	if err := os.WriteFile(segPath, data, 0644); err != nil {
		t.Fatalf("failed writing corrupted segment: %v", err)
	}

	// Re-open and replay
	w2, err := wal.Open(wal.Config{
		Dir:               tempDir,
		MaxSegmentBytes:   1024 * 1024,
		MaxTotalDiskBytes: 5 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed re-opening wal: %v", err)
	}
	defer w2.Close()

	_, err = w2.ReplayAll()
	if err == nil {
		t.Fatal("expected ErrCorruptRecord, but replay succeeded")
	}
}
