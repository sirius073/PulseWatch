package detect_test

import (
	"testing"

	"pulsewatch/server/internal/detect"
)

// TestDetectorSpikeBenchmark evaluates detector precision, recall, and MTTD
// on sudden burst/spike fault injections.
func TestDetectorSpikeBenchmark(t *testing.T) {
	// Generate spike-only ground truth suite
	var suite []detect.GroundTruthPoint
	// 50 baseline points around 20.0
	for i := 0; i < 50; i++ {
		suite = append(suite, detect.GroundTruthPoint{Value: 20.0, IsAnomaly: false})
	}
	// Spike 1: 3 points at 90.0
	for i := 0; i < 3; i++ {
		suite = append(suite, detect.GroundTruthPoint{Value: 90.0, IsAnomaly: true, AnomalyID: 1})
	}
	// 40 baseline points
	for i := 0; i < 40; i++ {
		suite = append(suite, detect.GroundTruthPoint{Value: 20.0, IsAnomaly: false})
	}
	// Spike 2: 2 points at 95.0
	for i := 0; i < 2; i++ {
		suite = append(suite, detect.GroundTruthPoint{Value: 95.0, IsAnomaly: true, AnomalyID: 2})
	}
	// 30 baseline points
	for i := 0; i < 30; i++ {
		suite = append(suite, detect.GroundTruthPoint{Value: 20.0, IsAnomaly: false})
	}

	detectors := []detect.Detector{
		detect.NewZScore(30, 3.0, 10),
		detect.NewEWMA(0.2, 3.0, 10),
		detect.NewMAD(30, 3.0, 10),
	}

	for _, d := range detectors {
		m := detect.Evaluate(d, suite)
		t.Logf("%s: Precision=%.2f%%, Recall=%.2f%%, MTTD=%.2f pts",
			m.DetectorName, m.Precision*100, m.Recall*100, m.MeanTimeToDetect)

		if m.TruePositives == 0 {
			t.Errorf("%s failed to detect any spikes", d.Name())
		}
		if m.MeanTimeToDetect > 1.0 {
			t.Errorf("%s MTTD too high: %.2f", d.Name(), m.MeanTimeToDetect)
		}
	}
}

// TestDetectorComprehensiveBenchmark runs the full mixed benchmark suite
// (spikes, leaks, recovery) and prints the comparative evaluation table.
func TestDetectorComprehensiveBenchmark(t *testing.T) {
	suite := detect.GenerateSyntheticSuite(42)

	detectors := []detect.Detector{
		detect.NewZScore(40, 3.0, 15),
		detect.NewEWMA(0.2, 3.0, 15),
		detect.NewMAD(40, 3.0, 15),
	}

	var results []detect.EvaluationMetrics
	for _, d := range detectors {
		m := detect.Evaluate(d, suite)
		results = append(results, m)
		if m.TruePositives == 0 {
			t.Errorf("%s failed to detect any anomalies in comprehensive suite", d.Name())
		}
	}

	table := detect.PrintComparisonTable(results)
	t.Log(table)
}
