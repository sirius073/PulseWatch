// Package detect provides streaming real-time anomaly detection algorithms
// in Go: Z-Score (rolling standard deviation), EWMA (exponentially weighted
// moving average), and MAD (Median Absolute Deviation).
package detect

import (
	"math"
	"sort"
	"sync"
)

// Detector defines the contract for streaming anomaly detectors.
type Detector interface {
	// Update feeds a new observation into the detector and returns whether
	// the point is anomalous and its anomaly score.
	Update(val float64) (isAnomaly bool, score float64)
	// Name returns the identifier of the detector algorithm.
	Name() string
	// Reset clears the internal state and window.
	Reset()
}

// ─── 1. Z-Score Detector ───────────────────────────────────────────────────────

// ZScoreDetector computes a rolling z-score: |x - mean| / stddev.
// It uses a fixed-capacity circular buffer to track recent observations.
type ZScoreDetector struct {
	mu         sync.Mutex
	windowSize int
	threshold  float64
	minWarmup  int
	buffer     []float64
	index      int
	count      int
}

// NewZScore creates a ZScoreDetector.
// windowSize: number of points to keep in the rolling window (e.g. 50).
// threshold: z-score boundary to classify as anomaly (e.g. 3.0 for 99.7% confidence).
// minWarmup: minimum samples required before emitting an anomaly (e.g. 10).
func NewZScore(windowSize int, threshold float64, minWarmup int) *ZScoreDetector {
	if windowSize <= 0 {
		windowSize = 50
	}
	if threshold <= 0 {
		threshold = 3.0
	}
	if minWarmup <= 0 {
		minWarmup = 10
	}
	return &ZScoreDetector{
		windowSize: windowSize,
		threshold:  threshold,
		minWarmup:  minWarmup,
		buffer:     make([]float64, windowSize),
	}
}

func (d *ZScoreDetector) Name() string {
	return "zscore"
}

func (d *ZScoreDetector) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.index = 0
	d.count = 0
}

func (d *ZScoreDetector) Update(val float64) (bool, float64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// If during warmup, collect history without triggering anomalies
	if d.count < d.minWarmup {
		d.buffer[d.index] = val
		d.index = (d.index + 1) % d.windowSize
		d.count++
		return false, 0.0
	}

	// Compute mean and std dev over current window
	n := d.count
	if n > d.windowSize {
		n = d.windowSize
	}

	var sum float64
	for i := 0; i < n; i++ {
		sum += d.buffer[i]
	}
	mean := sum / float64(n)

	var varianceSum float64
	for i := 0; i < n; i++ {
		diff := d.buffer[i] - mean
		varianceSum += diff * diff
	}
	stdDev := math.Sqrt(varianceSum / float64(n))

	var score float64
	if stdDev > 1e-9 {
		score = math.Abs(val-mean) / stdDev
	} else {
		if math.Abs(val-mean) > 1e-9 {
			score = math.Abs(val - mean)
		} else {
			score = 0.0
		}
	}

	// Update circular buffer with the new value
	d.buffer[d.index] = val
	d.index = (d.index + 1) % d.windowSize
	if d.count < d.windowSize {
		d.count++
	}

	isAnomaly := score >= d.threshold
	return isAnomaly, score
}

// ─── 2. EWMA Detector ─────────────────────────────────────────────────────────

// EWMADetector computes Exponentially Weighted Moving Average and variance.
// It prioritizes recent observations and responds gracefully to gradual trend shifts.
type EWMADetector struct {
	mu        sync.Mutex
	alpha     float64 // smoothing factor, 0 < alpha <= 1 (typically 0.2)
	threshold float64 // standard deviations threshold (typically 3.0)
	minWarmup int
	count     int
	mean      float64
	variance  float64
}

// NewEWMA creates an EWMADetector.
// alpha: decay parameter (e.g. 0.2 means current observation has 20% weight).
// threshold: number of standard deviations for anomaly trigger.
// minWarmup: warmup samples before triggering.
func NewEWMA(alpha float64, threshold float64, minWarmup int) *EWMADetector {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.2
	}
	if threshold <= 0 {
		threshold = 3.0
	}
	if minWarmup <= 0 {
		minWarmup = 10
	}
	return &EWMADetector{
		alpha:     alpha,
		threshold: threshold,
		minWarmup: minWarmup,
	}
}

func (d *EWMADetector) Name() string {
	return "ewma"
}

func (d *EWMADetector) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.count = 0
	d.mean = 0
	d.variance = 0
}

func (d *EWMADetector) Update(val float64) (bool, float64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.count == 0 {
		d.mean = val
		d.variance = 0
		d.count++
		return false, 0.0
	}

	stdDev := math.Sqrt(d.variance)
	var score float64
	if stdDev > 1e-9 {
		score = math.Abs(val-d.mean) / stdDev
	} else {
		score = 0.0
	}

	// Update EWMA mean and variance
	diff := val - d.mean
	d.mean = d.alpha*val + (1.0-d.alpha)*d.mean
	// Incremental variance calculation
	d.variance = (1.0-d.alpha)*(d.variance + d.alpha*diff*diff)
	d.count++

	if d.count <= d.minWarmup {
		return false, 0.0
	}

	isAnomaly := score >= d.threshold
	return isAnomaly, score
}

// ─── 3. MAD (Median Absolute Deviation) Detector ─────────────────────────────

// MADDetector computes Median Absolute Deviation over a rolling window.
// Unlike standard deviation, MAD is robust to extreme outliers and does
// not suffer from masking effects.
type MADDetector struct {
	mu         sync.Mutex
	windowSize int
	threshold  float64
	minWarmup  int
	buffer     []float64
	index      int
	count      int
}

// NewMAD creates a MADDetector.
// threshold: normalized MAD multiplier (e.g. 3.0 or 3.5).
func NewMAD(windowSize int, threshold float64, minWarmup int) *MADDetector {
	if windowSize <= 0 {
		windowSize = 50
	}
	if threshold <= 0 {
		threshold = 3.0
	}
	if minWarmup <= 0 {
		minWarmup = 10
	}
	return &MADDetector{
		windowSize: windowSize,
		threshold:  threshold,
		minWarmup:  minWarmup,
		buffer:     make([]float64, windowSize),
	}
}

func (d *MADDetector) Name() string {
	return "mad"
}

func (d *MADDetector) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.index = 0
	d.count = 0
}

func (d *MADDetector) Update(val float64) (bool, float64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.count < d.minWarmup {
		d.buffer[d.index] = val
		d.index = (d.index + 1) % d.windowSize
		d.count++
		return false, 0.0
	}

	n := d.count
	if n > d.windowSize {
		n = d.windowSize
	}

	// Copy and sort window to find median
	window := make([]float64, n)
	copy(window, d.buffer[:n])
	sort.Float64s(window)
	median := medianOfSorted(window)

	// Compute absolute deviations from median
	deviations := make([]float64, n)
	for i := 0; i < n; i++ {
		deviations[i] = math.Abs(window[i] - median)
	}
	sort.Float64s(deviations)
	mad := medianOfSorted(deviations)

	// Normal distribution consistency factor: 1.4826 (1 / 0.6745)
	var score float64
	if mad > 1e-9 {
		score = math.Abs(val-median) / (1.4826 * mad)
	} else {
		if math.Abs(val-median) > 1e-9 {
			score = math.Abs(val - median)
		} else {
			score = 0.0
		}
	}

	// Update circular buffer
	d.buffer[d.index] = val
	d.index = (d.index + 1) % d.windowSize
	if d.count < d.windowSize {
		d.count++
	}

	isAnomaly := score >= d.threshold
	return isAnomaly, score
}

func medianOfSorted(sorted []float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2.0
}
