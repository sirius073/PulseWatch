package main

import (
	"testing"
	"time"
)

func TestLatencyTracker(t *testing.T) {
	tracker := &LatencyTracker{}

	// Test empty tracker
	p50, p90, p95, p99, min, max := tracker.Percentiles()
	if p50 != 0 || p90 != 0 || p95 != 0 || p99 != 0 || min != 0 || max != 0 {
		t.Fatalf("expected 0 for empty tracker, got %v, %v, %v, %v, %v, %v", p50, p90, p95, p99, min, max)
	}

	// Record 100 values from 1ms to 100ms
	for i := 1; i <= 100; i++ {
		tracker.Record(time.Duration(i) * time.Millisecond)
	}

	p50, p90, p95, p99, min, max = tracker.Percentiles()

	if min != 1*time.Millisecond {
		t.Errorf("expected min 1ms, got %v", min)
	}
	if max != 100*time.Millisecond {
		t.Errorf("expected max 100ms, got %v", max)
	}
	if p50 != 51*time.Millisecond {
		t.Errorf("expected p50 around 51ms, got %v", p50)
	}
	if p90 != 91*time.Millisecond {
		t.Errorf("expected p90 around 91ms, got %v", p90)
	}
	if p95 != 96*time.Millisecond {
		t.Errorf("expected p95 around 96ms, got %v", p95)
	}
	if p99 != 100*time.Millisecond {
		t.Errorf("expected p99 around 100ms, got %v", p99)
	}
}
