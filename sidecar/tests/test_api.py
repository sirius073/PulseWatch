from starlette.testclient import TestClient
from sidecar.main import app

client = TestClient(app)


def test_api_health():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["service"] == "pulsewatch-ml-sidecar"


def test_api_post_runway_custom():
    payload = {
        "points": [
            {"ts": "2026-11-10T10:00:00Z", "value": 60.0},
            {"ts": "2026-11-10T11:00:00Z", "value": 65.0},
            {"ts": "2026-11-10T12:00:00Z", "value": 70.0},
            {"ts": "2026-11-10T13:00:00Z", "value": 75.0},
            {"ts": "2026-11-10T14:00:00Z", "value": 80.0},
        ],
        "capacity_limit": 100.0,
    }
    response = client.post("/predict/runway", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["current_value"] == 80.0
    assert abs(data["growth_rate_per_hour"] - 5.0) < 0.05
    assert abs(data["hours_to_exhaustion"] - 4.0) < 0.1
    assert data["status"] == "critical"


def test_api_post_runway_backtest():
    payload = {
        "points": [
            {"ts": "2026-11-10T10:00:00Z", "value": 40.0},
            {"ts": "2026-11-10T11:00:00Z", "value": 50.0},
            {"ts": "2026-11-10T12:00:00Z", "value": 60.0},
            {"ts": "2026-11-10T13:00:00Z", "value": 70.0},
            {"ts": "2026-11-10T14:00:00Z", "value": 80.0},
        ],
        "true_exhaustion_hours": 2.0,
        "capacity_limit": 100.0,
    }
    response = client.post("/predict/runway/backtest", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["predicted_hours"] == 2.0
    assert data["error_hours"] == 0.0
    assert data["accuracy_percent"] == 100.0


def test_api_post_anomalies():
    records = []
    for i in range(20):
        records.append({
            "cpu": 15.0 + (i % 2),
            "memory": 40.0,
            "disk": 55.0,
            "net_sent": 1000.0,
        })
    # Add an extreme outlier
    records.append({
        "cpu": 98.0,
        "memory": 95.0,
        "disk": 55.0,
        "net_sent": 999999.0,
    })

    response = client.post("/predict/anomalies", json=records)
    assert response.status_code == 200
    data = response.json()
    assert data["total_records"] == 21
    assert data["anomalies_detected"] >= 1
