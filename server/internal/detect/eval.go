// Package detect provides streaming anomaly detection and evaluation tools.
package detect

import (
	"fmt"
	"math"
	"math/rand"
)

// GroundTruthPoint represents a single data point in an evaluation series
// with a known label (normal vs anomalous) and optional AnomalyID.
type GroundTruthPoint struct {
	Value     float64
	IsAnomaly bool
	// AnomalyID distinguishes separate anomaly episodes for MTTD calculation.
	// 0 means normal baseline.
	AnomalyID int
}

// EvaluationMetrics summarizes detection quality against ground truth.
type EvaluationMetrics struct {
	DetectorName     string  `json:"detector_name"`
	TotalPoints      int     `json:"total_points"`
	TruePositives    int     `json:"true_positives"`
	FalsePositives   int     `json:"false_positives"`
	FalseNegatives   int     `json:"false_negatives"`
	TrueNegatives    int     `json:"true_negatives"`
	Precision        float64 `json:"precision"`
	Recall           float64 `json:"recall"`
	F1Score          float64 `json:"f1_score"`
	MeanTimeToDetect float64 `json:"mean_time_to_detect"` // average step delay until first alarm per episode
}

// Evaluate runs a detector over a labeled dataset and calculates precision,
// recall, F1, and mean time to detect (MTTD).
func Evaluate(d Detector, dataset []GroundTruthPoint) EvaluationMetrics {
	d.Reset()

	var tp, fp, fn, tn int

	// Track first detection per anomaly episode for MTTD
	episodeStart := make(map[int]int)
	episodeDetected := make(map[int]int)

	for i, pt := range dataset {
		isAnomaly, _ := d.Update(pt.Value)

		if pt.IsAnomaly {
			if _, exists := episodeStart[pt.AnomalyID]; !exists {
				episodeStart[pt.AnomalyID] = i
			}
			if isAnomaly {
				tp++
				if _, detected := episodeDetected[pt.AnomalyID]; !detected {
					episodeDetected[pt.AnomalyID] = i - episodeStart[pt.AnomalyID]
				}
			} else {
				fn++
			}
		} else {
			if isAnomaly {
				fp++
			} else {
				tn++
			}
		}
	}

	var precision, recall, f1 float64
	if tp+fp > 0 {
		precision = float64(tp) / float64(tp+fp)
	}
	if tp+fn > 0 {
		recall = float64(tp) / float64(tp+fn)
	}
	if precision+recall > 0 {
		f1 = 2.0 * (precision * recall) / (precision + recall)
	}

	// Calculate average MTTD across detected episodes
	var totalDelay float64
	var detectedCount int
	for _, delay := range episodeDetected {
		totalDelay += float64(delay)
		detectedCount++
	}

	var mttd float64
	if detectedCount > 0 {
		mttd = totalDelay / float64(detectedCount)
	} else if len(episodeStart) > 0 {
		mttd = math.NaN() // none detected
	}

	return EvaluationMetrics{
		DetectorName:     d.Name(),
		TotalPoints:      len(dataset),
		TruePositives:    tp,
		FalsePositives:   fp,
		FalseNegatives:   fn,
		TrueNegatives:    tn,
		Precision:        precision,
		Recall:           recall,
		F1Score:          f1,
		MeanTimeToDetect: mttd,
	}
}

// ─── Synthetic Dataset Generators with Labeled Ground Truth ───────────────────

// GenerateSyntheticSuite generates a standardized benchmark dataset combining:
// 1. Stable baseline with Gaussian noise (warmup & baseline)
// 2. Sudden CPU spikes (short burst anomalies)
// 3. Gradual memory leak (linear slope ramp)
// 4. Step-function level shift (sudden mean change)
func GenerateSyntheticSuite(seed int64) []GroundTruthPoint {
	r := rand.New(rand.NewSource(seed))
	var data []GroundTruthPoint

	anomalyCounter := 1

	// 1. Initial stable baseline: 100 points around 20.0 (stddev ~ 1.5)
	for i := 0; i < 100; i++ {
		noise := r.NormFloat64() * 1.5
		data = append(data, GroundTruthPoint{
			Value:     math.Max(0, 20.0+noise),
			IsAnomaly: false,
		})
	}

	// 2. Sudden CPU Spike Episode 1: 3-point spike reaching 95.0
	for i := 0; i < 3; i++ {
		data = append(data, GroundTruthPoint{
			Value:     90.0 + r.Float64()*5.0,
			IsAnomaly: true,
			AnomalyID: anomalyCounter,
		})
	}
	anomalyCounter++

	// Recovery to baseline: 50 points
	for i := 0; i < 50; i++ {
		noise := r.NormFloat64() * 1.5
		data = append(data, GroundTruthPoint{
			Value:     math.Max(0, 20.0+noise),
			IsAnomaly: false,
		})
	}

	// 3. Gradual Memory Leak Episode 2: ramps up from 25.0 to 85.0 over 40 points
	for i := 0; i < 40; i++ {
		val := 25.0 + float64(i)*1.5 + r.NormFloat64()*1.0
		isAnom := i >= 20 // labeled anomalous once it breaches ~55.0
		id := 0
		if isAnom {
			id = anomalyCounter
		}
		data = append(data, GroundTruthPoint{
			Value:     val,
			IsAnomaly: isAnom,
			AnomalyID: id,
		})
	}
	anomalyCounter++

	// Recovery to baseline: 50 points
	for i := 0; i < 50; i++ {
		noise := r.NormFloat64() * 1.5
		data = append(data, GroundTruthPoint{
			Value:     math.Max(0, 22.0+noise),
			IsAnomaly: false,
		})
	}

	// 4. Sudden CPU Spike Episode 3: sharp isolated 2-point burst
	for i := 0; i < 2; i++ {
		data = append(data, GroundTruthPoint{
			Value:     95.0 + r.Float64()*4.0,
			IsAnomaly: true,
			AnomalyID: anomalyCounter,
		})
	}

	// Final baseline: 50 points
	for i := 0; i < 50; i++ {
		noise := r.NormFloat64() * 1.5
		data = append(data, GroundTruthPoint{
			Value:     math.Max(0, 20.0+noise),
			IsAnomaly: false,
		})
	}

	return data
}

// PrintComparisonTable formats the evaluation metrics of multiple detectors.
func PrintComparisonTable(metrics []EvaluationMetrics) string {
	out := "\n=== STREAMING ANOMALY DETECTOR BENCHMARK EVALUATION ===\n"
	out += fmt.Sprintf("%-12s | %-9s | %-9s | %-9s | %-7s | %-7s | %-7s | %-7s | %-6s\n",
		"Detector", "Precision", "Recall", "F1-Score", "MTTD", "TP", "FP", "FN", "TN")
	out += "-------------+-----------+-----------+-----------+---------+---------+---------+---------+-------\n"

	for _, m := range metrics {
		mttdStr := fmt.Sprintf("%.2f pts", m.MeanTimeToDetect)
		if math.IsNaN(m.MeanTimeToDetect) {
			mttdStr = "N/A"
		}
		out += fmt.Sprintf("%-12s | %8.2f%% | %8.2f%% | %8.2f%% | %-7s | %-7d | %-7d | %-7d | %-6d\n",
			m.DetectorName,
			m.Precision*100,
			m.Recall*100,
			m.F1Score*100,
			mttdStr,
			m.TruePositives,
			m.FalsePositives,
			m.FalseNegatives,
			m.TrueNegatives,
		)
	}
	out += "=======================================================\n"
	return out
}
