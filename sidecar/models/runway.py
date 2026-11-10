"""
Runway Capacity Forecaster
Predicts time-to-exhaustion (hours to reach 100% capacity) for disk or memory.
Fits a trend curve on recent consumption history and calculates confidence metrics.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import numpy as np


class RunwayPrediction:
    def __init__(
        self,
        current_value: float,
        growth_rate_per_hour: float,
        hours_to_exhaustion: Optional[float],
        exhaustion_timestamp: Optional[str],
        r2_score: float,
        status: str,  # "critical", "warning", "stable", "decreasing"
        message: str,
    ):
        self.current_value = current_value
        self.growth_rate_per_hour = growth_rate_per_hour
        self.hours_to_exhaustion = hours_to_exhaustion
        self.exhaustion_timestamp = exhaustion_timestamp
        self.r2_score = r2_score
        self.status = status
        self.message = message

    def to_dict(self) -> Dict[str, Any]:
        return {
            "current_value": round(self.current_value, 2),
            "growth_rate_per_hour": round(self.growth_rate_per_hour, 4),
            "hours_to_exhaustion": round(self.hours_to_exhaustion, 2) if self.hours_to_exhaustion is not None else None,
            "exhaustion_timestamp": self.exhaustion_timestamp,
            "r2_score": round(self.r2_score, 4),
            "status": self.status,
            "message": self.message,
        }


def predict_runway(
    points: List[Dict[str, Any]],
    capacity_limit: float = 100.0,
    min_points: int = 5,
) -> RunwayPrediction:
    """
    Fits linear regression on points: [{'ts': iso_str, 'value': float}]
    Computes slope (growth per hour) and extrapolates time until capacity_limit.
    """
    if len(points) < min_points:
        curr = points[-1]["value"] if points else 0.0
        return RunwayPrediction(
            current_value=curr,
            growth_rate_per_hour=0.0,
            hours_to_exhaustion=None,
            exhaustion_timestamp=None,
            r2_score=0.0,
            status="insufficient_data",
            message=f"Need at least {min_points} data points for runway forecast (got {len(points)})",
        )

    # Parse timestamps to hours elapsed since t0
    t0 = datetime.fromisoformat(points[0]["ts"].replace("Z", "+00:00"))
    x_hours = []
    y_vals = []

    for pt in points:
        t = datetime.fromisoformat(pt["ts"].replace("Z", "+00:00"))
        delta_h = (t - t0).total_seconds() / 3600.0
        x_hours.append(delta_h)
        y_vals.append(float(pt["value"]))

    x = np.array(x_hours)
    y = np.array(y_vals)

    # Ordinary Least Squares fit: y = slope * x + intercept
    A = np.vstack([x, np.ones(len(x))]).T
    slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]

    # Calculate coefficient of determination (R^2)
    y_pred = slope * x + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-9 else 1.0
    r2 = max(0.0, min(1.0, float(r2)))

    current_val = y_vals[-1]
    last_t = datetime.fromisoformat(points[-1]["ts"].replace("Z", "+00:00"))

    # If usage is already at or above limit
    if current_val >= capacity_limit:
        return RunwayPrediction(
            current_value=current_val,
            growth_rate_per_hour=float(slope),
            hours_to_exhaustion=0.0,
            exhaustion_timestamp=last_t.isoformat(),
            r2_score=r2,
            status="critical",
            message=f"Resource capacity already exhausted ({current_val:.1f}% >= {capacity_limit}%)",
        )

    # Decreasing or flat usage: runway is infinite/safe
    if slope <= 1e-5:
        status = "decreasing" if slope < -1e-4 else "stable"
        msg = "Usage is trending downwards" if slope < -1e-4 else "Usage is flat and stable; no exhaustion projected"
        return RunwayPrediction(
            current_value=current_val,
            growth_rate_per_hour=float(slope),
            hours_to_exhaustion=None,
            exhaustion_timestamp=None,
            r2_score=r2,
            status=status,
            message=msg,
        )

    # Positive growth slope: extrapolate time to reach limit
    remaining_capacity = capacity_limit - current_val
    hours_left = remaining_capacity / slope
    exhaustion_dt = last_t + timedelta(hours=hours_left)

    if hours_left <= 24.0:
        status = "critical"
    elif hours_left <= 72.0:
        status = "warning"
    else:
        status = "normal"

    msg = f"Projected capacity exhaustion in {hours_left:.1f} hours at current growth of {slope:.2f}%/hour"

    return RunwayPrediction(
        current_value=current_val,
        growth_rate_per_hour=float(slope),
        hours_to_exhaustion=float(hours_left),
        exhaustion_timestamp=exhaustion_dt.isoformat(),
        r2_score=r2,
        status=status,
        message=msg,
    )


def backtest_runway(
    synthetic_curve: List[Dict[str, Any]],
    true_exhaustion_hours: float,
    capacity_limit: float = 100.0,
) -> Dict[str, Any]:
    """
    Backtests the runway predictor on a synthetic curve where true exhaustion time is known.
    Returns prediction accuracy and absolute error margin in hours.
    """
    prediction = predict_runway(synthetic_curve, capacity_limit=capacity_limit)
    if prediction.hours_to_exhaustion is None:
        return {
            "predicted_hours": None,
            "true_hours": true_exhaustion_hours,
            "error_hours": None,
            "accuracy_percent": 0.0,
        }

    abs_error = abs(prediction.hours_to_exhaustion - true_exhaustion_hours)
    accuracy = max(0.0, 100.0 - (abs_error / true_exhaustion_hours) * 100.0)

    return {
        "predicted_hours": round(prediction.hours_to_exhaustion, 2),
        "true_hours": round(true_exhaustion_hours, 2),
        "error_hours": round(abs_error, 2),
        "accuracy_percent": round(accuracy, 2),
        "r2_score": prediction.r2_score,
        "status": prediction.status,
    }
