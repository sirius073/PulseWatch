package detect_test

import (
	"math"
	"testing"

	"pulsewatch/server/internal/detect"
)

func TestDetectorsWarmup(t *testing.T) {
	detectors := []detect.Detector{
		detect.NewZScore(30, 3.0, 10),
		detect.NewEWMA(0.2, 3.0, 10),
		detect.NewMAD(30, 3.0, 10),
	}

	for _, d := range detectors {
		t.Run(d.Name()+"_Warmup", func(t *testing.T) {
			// Feed fewer than 10 points (even with high values)
			for i := 0; i < 9; i++ {
				anomaly, score := d.Update(100.0)
				if anomaly {
					t.Fatalf("%s: emitted anomaly during warmup at step %d", d.Name(), i)
				}
				if score != 0.0 {
					t.Fatalf("%s: expected score 0.0 during warmup, got %f", d.Name(), score)
				}
			}
		})
	}
}

func TestDetectorsSpikeDetection(t *testing.T) {
	tests := []struct {
		name     string
		detector detect.Detector
	}{
		{"ZScore", detect.NewZScore(30, 3.0, 10)},
		{"EWMA", detect.NewEWMA(0.2, 3.0, 10)},
		{"MAD", detect.NewMAD(30, 3.0, 10)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := tt.detector
			// Feed 20 baseline points around 25.0
			for i := 0; i < 20; i++ {
				val := 25.0 + float64(i%3)*0.5 // 25.0, 25.5, 26.0
				anomaly, _ := d.Update(val)
				if anomaly {
					t.Fatalf("%s: unexpected anomaly during baseline at step %d", d.Name(), i)
				}
			}

			// Inject a massive spike: 150.0
			isAnomaly, score := d.Update(150.0)
			if !isAnomaly {
				t.Fatalf("%s: failed to detect massive spike (150.0), score was %f", d.Name(), score)
			}
			if score <= 3.0 {
				t.Fatalf("%s: expected score > 3.0, got %f", d.Name(), score)
			}
		})
	}
}

func TestDetectorsZeroVariance(t *testing.T) {
	detectors := []detect.Detector{
		detect.NewZScore(30, 3.0, 10),
		detect.NewEWMA(0.2, 3.0, 10),
		detect.NewMAD(30, 3.0, 10),
	}

	for _, d := range detectors {
		t.Run(d.Name()+"_ZeroVariance", func(t *testing.T) {
			// Feed 20 identical points
			for i := 0; i < 20; i++ {
				anomaly, score := d.Update(42.0)
				if anomaly {
					t.Fatalf("%s: identical points triggered anomaly at step %d", d.Name(), i)
				}
				if math.IsNaN(score) || math.IsInf(score, 0) {
					t.Fatalf("%s: score is NaN or Inf for identical points: %f", d.Name(), score)
				}
			}
		})
	}
}

func TestDetectorsReset(t *testing.T) {
	detectors := []detect.Detector{
		detect.NewZScore(30, 3.0, 10),
		detect.NewEWMA(0.2, 3.0, 10),
		detect.NewMAD(30, 3.0, 10),
	}

	for _, d := range detectors {
		t.Run(d.Name()+"_Reset", func(t *testing.T) {
			for i := 0; i < 25; i++ {
				d.Update(50.0)
			}
			d.Reset()
			// First update after reset should be in warmup (anomaly=false, score=0.0)
			anomaly, score := d.Update(50.0)
			if anomaly || score != 0.0 {
				t.Fatalf("%s: Reset failed to restore warmup state", d.Name())
			}
		})
	}
}
