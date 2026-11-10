"""
Multivariate Isolation Forest Anomaly Detector
Detects anomalies across multiple correlated system metrics simultaneously
(e.g., CPU, Memory, Disk usage, Network I/O) and isolates the top contributing feature.
"""

from typing import List, Dict, Any, Tuple
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler


class MultivariateAnomalyDetector:
    FEATURE_NAMES = ["cpu", "memory", "disk", "net_sent"]

    def __init__(self, contamination: float = 0.05, random_state: int = 42):
        self.contamination = contamination
        self.scaler = RobustScaler()
        self.model = IsolationForest(
            n_estimators=100,
            contamination=contamination,
            random_state=random_state,
            n_jobs=-1,
        )
        self.is_fitted = False
        self.feature_medians: np.ndarray = np.zeros(len(self.FEATURE_NAMES))
        self.feature_iqrs: np.ndarray = np.ones(len(self.FEATURE_NAMES))

    def _extract_matrix(self, records: List[Dict[str, Any]]) -> np.ndarray:
        matrix = []
        for r in records:
            row = [
                float(r.get("cpu", 0.0)),
                float(r.get("memory", 0.0)),
                float(r.get("disk", 0.0)),
                float(r.get("net_sent", 0.0)),
            ]
            matrix.append(row)
        return np.array(matrix, dtype=np.float64)

    def fit(self, baseline_records: List[Dict[str, Any]]) -> "MultivariateAnomalyDetector":
        if len(baseline_records) < 10:
            raise ValueError(f"Need at least 10 baseline records to fit model (got {len(baseline_records)})")

        X = self._extract_matrix(baseline_records)
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)

        # Store baseline medians and IQRs for feature attribution
        self.feature_medians = np.median(X, axis=0)
        q75, q25 = np.percentile(X, [75, 25], axis=0)
        iqr = q75 - q25
        self.feature_iqrs = np.where(iqr > 1e-6, iqr, 1.0)
        self.is_fitted = True
        return self

    def score(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Scores input records. If model is not yet fitted, fits on provided data.
        Returns:
            list of dicts with:
                - is_anomaly: bool
                - anomaly_score: float in [0.0, 1.0] (higher = more anomalous)
                - top_contributor: feature name that deviated most
                - feature_deviations: dict of z-deviations per feature
        """
        if not records:
            return []

        X = self._extract_matrix(records)

        if not self.is_fitted:
            if len(records) >= 10:
                self.fit(records)
            else:
                # Fallback simple deviation scoring for small datasets
                return self._fallback_score(records)

        X_scaled = self.scaler.transform(X)
        predictions = self.model.predict(X_scaled)  # -1 = anomaly, 1 = normal
        raw_scores = self.model.score_samples(X_scaled)  # lower = more abnormal

        # Normalize raw score to [0.0, 1.0] where 1.0 is most anomalous
        # Typical raw_scores range from -0.8 to -0.2
        min_s = -0.8
        max_s = -0.2
        norm_scores = np.clip(1.0 - (raw_scores - min_s) / (max_s - min_s), 0.0, 1.0)

        results = []
        for i, r in enumerate(records):
            is_anom = bool(predictions[i] == -1)

            # Calculate feature contributions via standardized distance from baseline
            row_deviations = np.abs(X[i] - self.feature_medians) / self.feature_iqrs
            max_idx = int(np.argmax(row_deviations))
            top_feature = self.FEATURE_NAMES[max_idx]

            dev_dict = {
                name: round(float(row_deviations[j]), 2)
                for j, name in enumerate(self.FEATURE_NAMES)
            }

            results.append({
                "ts": r.get("ts"),
                "is_anomaly": is_anom,
                "anomaly_score": round(float(norm_scores[i]), 4),
                "top_contributor": top_feature,
                "feature_deviations": dev_dict,
                "input_values": {
                    "cpu": round(float(X[i][0]), 2),
                    "memory": round(float(X[i][1]), 2),
                    "disk": round(float(X[i][2]), 2),
                    "net_sent": round(float(X[i][3]), 2),
                },
            })

        return results

    def _fallback_score(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        for r in records:
            cpu = float(r.get("cpu", 0.0))
            mem = float(r.get("memory", 0.0))
            score = max(cpu / 100.0, mem / 100.0)
            is_anom = score > 0.85
            top = "cpu" if cpu > mem else "memory"
            results.append({
                "ts": r.get("ts"),
                "is_anomaly": is_anom,
                "anomaly_score": round(score, 4),
                "top_contributor": top,
                "feature_deviations": {"cpu": round(cpu, 1), "memory": round(mem, 1)},
                "input_values": {"cpu": cpu, "memory": mem},
            })
        return results
