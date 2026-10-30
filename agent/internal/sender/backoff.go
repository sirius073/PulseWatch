package sender

import (
	"math/rand"
	"time"
)

// Backoff implements exponential backoff with full jitter.
// Prevents thundering herd problems when a recovering server comes back online.
type Backoff struct {
	baseDelay time.Duration
	maxDelay  time.Duration
	attempt   int
	rng       *rand.Rand
}

// NewBackoff creates a new backoff calculator with base and maximum delays.
func NewBackoff(baseDelay, maxDelay time.Duration) *Backoff {
	if baseDelay <= 0 {
		baseDelay = 500 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}
	return &Backoff{
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		attempt:   0,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NextDelay calculates the next sleep duration using Full Jitter:
// sleep = random_between(0, min(maxDelay, baseDelay * 2^attempt))
func (b *Backoff) NextDelay() time.Duration {
	// Calculate exponential ceiling: base * 2^attempt
	multiplier := int64(1) << b.attempt
	ceiling := time.Duration(int64(b.baseDelay) * multiplier)
	if ceiling > b.maxDelay || multiplier < 1 { // handle overflow
		ceiling = b.maxDelay
	}

	b.attempt++

	// Apply full jitter: random value between 0 and ceiling
	if ceiling <= 0 {
		return b.baseDelay
	}
	return time.Duration(b.rng.Int63n(int64(ceiling)))
}

// Reset resets the attempt counter after a successful transmission.
func (b *Backoff) Reset() {
	b.attempt = 0
}
