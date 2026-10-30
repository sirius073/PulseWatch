package pulsewatchv1_test

import (
	"testing"
	"time"

	pulsewatchv1 "pulsewatch/proto/v1"

	"google.golang.org/protobuf/proto"
)

func TestMetricBatchSerialization(t *testing.T) {
	nowMs := time.Now().UnixMilli()

	batch := &pulsewatchv1.MetricBatch{
		HostId:         "host-worker-01",
		Seq:            42,
		SentAtUnixMs:   nowMs,
		Points: []*pulsewatchv1.MetricPoint{
			{
				Name:            "system.cpu.utilization",
				Value:           78.5,
				TimestampUnixMs: nowMs,
				Labels: map[string]string{
					"core": "0",
					"mode": "user",
				},
			},
			{
				Name:            "system.memory.used_bytes",
				Value:           8589934592, // 8 GB
				TimestampUnixMs: nowMs,
				Labels: map[string]string{
					"type": "ram",
				},
			},
		},
	}

	// 1. Marshal to wire binary format
	data, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("failed to marshal MetricBatch: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("marshaled data must not be empty")
	}

	// 2. Unmarshal back to struct
	deserialized := &pulsewatchv1.MetricBatch{}
	if err := proto.Unmarshal(data, deserialized); err != nil {
		t.Fatalf("failed to unmarshal MetricBatch: %v", err)
	}

	// 3. Verify fields
	if deserialized.HostId != batch.HostId {
		t.Errorf("expected HostId %q, got %q", batch.HostId, deserialized.HostId)
	}
	if deserialized.Seq != batch.Seq {
		t.Errorf("expected Seq %d, got %d", batch.Seq, deserialized.Seq)
	}
	if len(deserialized.Points) != len(batch.Points) {
		t.Fatalf("expected %d points, got %d", len(batch.Points), len(deserialized.Points))
	}
	if deserialized.Points[0].Name != "system.cpu.utilization" {
		t.Errorf("expected metric name %q, got %q", "system.cpu.utilization", deserialized.Points[0].Name)
	}
	if deserialized.Points[0].Value != 78.5 {
		t.Errorf("expected value %f, got %f", 78.5, deserialized.Points[0].Value)
	}
}

func TestBatchAckStatus(t *testing.T) {
	ack := &pulsewatchv1.BatchAck{
		HostId: "host-worker-01",
		Seq:    42,
		Status: pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED,
	}

	data, err := proto.Marshal(ack)
	if err != nil {
		t.Fatalf("failed to marshal BatchAck: %v", err)
	}

	deserialized := &pulsewatchv1.BatchAck{}
	if err := proto.Unmarshal(data, deserialized); err != nil {
		t.Fatalf("failed to unmarshal BatchAck: %v", err)
	}

	if deserialized.Status != pulsewatchv1.AckStatus_ACK_STATUS_COMMITTED {
		t.Errorf("expected status ACK_STATUS_COMMITTED, got %v", deserialized.Status)
	}
}
