from datetime import datetime, timedelta, timezone
import pytest
from sidecar.models.runway import predict_runway, backtest_runway


def test_runway_linear_growth():
    # Base timestamp
    t0 = datetime(2026, 11, 10, 10, 0, 0, tzinfo=timezone.utc)
    # 6 points over 5 hours: starts at 50% and grows by 5% every hour (50, 55, 60, 65, 70, 75%)
    points = [
        {"ts": (t0 + timedelta(hours=i)).isoformat(), "value": 50.0 + i * 5.0}
        for i in range(6)
    ]

    pred = predict_runway(points, capacity_limit=100.0)

    # Current value should be 75.0%
    assert pred.current_value == 75.0
    # Growth rate should be ~5.0% / hour
    assert abs(pred.growth_rate_per_hour - 5.0) < 0.01
    # Remaining capacity: 25.0%. At 5%/hour, exhaustion should be in exactly 5.0 hours!
    assert pred.hours_to_exhaustion is not None
    assert abs(pred.hours_to_exhaustion - 5.0) < 0.1
    # Status should be "critical" (since <= 24 hours)
    assert pred.status == "critical"
    # R2 should be perfect linear fit (1.0)
    assert pred.r2_score > 0.99


def test_runway_stable_flat():
    t0 = datetime(2026, 11, 10, 10, 0, 0, tzinfo=timezone.utc)
    points = [
        {"ts": (t0 + timedelta(hours=i)).isoformat(), "value": 45.0}
        for i in range(10)
    ]

    pred = predict_runway(points)
    assert pred.current_value == 45.0
    assert pred.hours_to_exhaustion is None
    assert pred.status == "stable"


def test_runway_decreasing():
    t0 = datetime(2026, 11, 10, 10, 0, 0, tzinfo=timezone.utc)
    points = [
        {"ts": (t0 + timedelta(hours=i)).isoformat(), "value": 80.0 - i * 2.0}
        for i in range(8)
    ]

    pred = predict_runway(points)
    assert pred.hours_to_exhaustion is None
    assert pred.status == "decreasing"


def test_runway_already_exhausted():
    t0 = datetime(2026, 11, 10, 10, 0, 0, tzinfo=timezone.utc)
    points = [
        {"ts": (t0 + timedelta(hours=i)).isoformat(), "value": 95.0 + i * 2.0}
        for i in range(5)
    ]

    pred = predict_runway(points, capacity_limit=100.0)
    # Last value is 103.0%
    assert pred.hours_to_exhaustion == 0.0
    assert pred.status == "critical"


def test_runway_backtesting():
    # Synthetic disk fill curve: starts at 20% and grows by 8% per hour.
    # At t=10h, usage reaches 100%.
    t0 = datetime(2026, 11, 10, 0, 0, 0, tzinfo=timezone.utc)
    # We sample the first 4 hours (from 20% to 52%)
    sample = [
        {"ts": (t0 + timedelta(hours=i)).isoformat(), "value": 20.0 + i * 8.0}
        for i in range(5)
    ]
    # Current value at t=4h is 52%. Remaining to 100% is 48%.
    # 48% / 8% = 6.0 hours from now (which is true exhaustion = 6.0h remaining)
    res = backtest_runway(sample, true_exhaustion_hours=6.0, capacity_limit=100.0)

    assert res["predicted_hours"] == 6.0
    assert res["true_hours"] == 6.0
    assert res["error_hours"] == 0.0
    assert res["accuracy_percent"] == 100.0
