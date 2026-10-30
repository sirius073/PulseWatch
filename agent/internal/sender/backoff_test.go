package sender_test

import (
	"testing"
	"time"

	"pulsewatch/agent/internal/sender"
)

func TestBackoffFullJitterBounds(t *testing.T) {
	base := 100 * time.Millisecond
	max := 2 * time.Second

	b := sender.NewBackoff(base, max)

	// Sample 50 delays across multiple attempts
	for i := 0; i < 50; i++ {
		d := b.NextDelay()
		if d < 0 {
			t.Errorf("delay cannot be negative, got %v", d)
		}
		if d > max {
			t.Errorf("delay %v exceeded maxDelay %v", d, max)
		}
	}

	b.Reset()
	d := b.NextDelay()
	if d > base*2 {
		t.Errorf("expected initial delay to be within base range, got %v", d)
	}
}
