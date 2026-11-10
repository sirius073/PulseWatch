import pytest
from sidecar.models.isolation_forest import MultivariateAnomalyDetector


def test_multivariate_detector_normal_vs_anomaly():
    detector = MultivariateAnomalyDetector(contamination=0.05, random_state=42)

    # 40 baseline records with small natural noise
    baseline = []
    for i in range(40):
        baseline.append({
            "ts": f"2026-11-10T10:{i:02d}:00Z",
            "cpu": 20.0 + (i % 3) * 1.5,
            "memory": 45.0 + (i % 4) * 0.5,
            "disk": 50.0,
            "net_sent": 5000.0 + (i % 5) * 200.0,
        })

    detector.fit(baseline)
    assert detector.is_fitted

    # Test scoring baseline: should have low anomaly scores
    baseline_scores = detector.score(baseline[:5])
    for s in baseline_scores:
        assert s["anomaly_score"] < 0.80

    # Inject extreme anomaly on CPU and Net
    anomaly_record = [{
        "ts": "2026-11-10T11:00:00Z",
        "cpu": 99.0,          # massive spike
        "memory": 45.0,
        "disk": 50.0,
        "net_sent": 950000.0,  # massive network flood
    }]

    anomaly_scores = detector.score(anomaly_record)
    assert len(anomaly_scores) == 1
    anom = anomaly_scores[0]

    # Should detect anomaly
    assert anom["is_anomaly"] is True
    # Anomaly score should be elevated
    assert anom["anomaly_score"] > 0.50
    # Top contributor should identify either cpu or net_sent
    assert anom["top_contributor"] in ["cpu", "net_sent"]
