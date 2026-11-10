from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, Query, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sidecar.config import settings
from sidecar.db import db
from sidecar.models.runway import predict_runway, backtest_runway
from sidecar.models.isolation_forest import MultivariateAnomalyDetector


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: connect to TimescaleDB
    await db.connect()
    yield
    # Shutdown: close DB connection
    await db.disconnect()


app = FastAPI(
    title="PulseWatch ML Predictive Sidecar",
    description="Capacity Runway Forecasting & Multivariate Isolation Forest Anomaly Detection",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global in-memory anomaly detector instance
anomaly_detector = MultivariateAnomalyDetector()


class MetricPointPayload(BaseModel):
    ts: str
    value: float


class RunwayRequest(BaseModel):
    points: List[MetricPointPayload]
    capacity_limit: float = 100.0


class BacktestRequest(BaseModel):
    points: List[MetricPointPayload]
    true_exhaustion_hours: float
    capacity_limit: float = 100.0


class MultivariateRecord(BaseModel):
    ts: Optional[str] = None
    cpu: float
    memory: float
    disk: Optional[float] = 0.0
    net_sent: Optional[float] = 0.0


@app.get("/health")
async def health_check():
    db_connected = db.pool is not None
    return {
        "status": "ok",
        "service": "pulsewatch-ml-sidecar",
        "database_connected": db_connected,
    }


@app.get("/predict/runway")
async def get_runway_prediction(
    host: str = Query(..., description="Target host ID"),
    metric: str = Query("system.disk.used_percent", description="Metric to forecast (e.g. disk or memory)"),
    hours: int = Query(6, description="Lookback window in hours"),
    capacity_limit: float = Query(100.0, description="Threshold limit (default 100%)"),
):
    """
    Fetches historical readings from TimescaleDB and calculates remaining runway in hours.
    """
    records = await db.fetch_metric_history(host, metric, hours=hours)
    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"No metric history found for host '{host}' and metric '{metric}' in last {hours} hours",
        )

    prediction = predict_runway(records, capacity_limit=capacity_limit)
    return {
        "host": host,
        "metric": metric,
        "lookback_hours": hours,
        "sample_count": len(records),
        "forecast": prediction.to_dict(),
    }


@app.post("/predict/runway")
async def post_runway_custom(payload: RunwayRequest):
    """
    Runs runway forecast on custom time-series data without database dependency.
    """
    pts = [{"ts": p.ts, "value": p.value} for p in payload.points]
    prediction = predict_runway(pts, capacity_limit=payload.capacity_limit)
    return prediction.to_dict()


@app.post("/predict/runway/backtest")
async def post_runway_backtest(payload: BacktestRequest):
    """
    Backtests runway forecast against known ground-truth exhaustion time.
    """
    pts = [{"ts": p.ts, "value": p.value} for p in payload.points]
    result = backtest_runway(
        pts,
        true_exhaustion_hours=payload.true_exhaustion_hours,
        capacity_limit=payload.capacity_limit,
    )
    return result


@app.get("/predict/anomalies/host/{host_id}")
async def get_host_anomalies(
    host_id: str,
    hours: int = Query(2, description="Lookback hours for multivariate analysis"),
):
    """
    Fetches synchronized multivariate metrics (CPU, Memory, Disk, Network)
    from TimescaleDB and evaluates anomaly scores using Isolation Forest.
    """
    records = await db.fetch_multivariate_snapshot(host_id, hours=hours)
    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"No multivariate metrics found for host '{host_id}' in last {hours} hours",
        )

    scores = anomaly_detector.score(records)
    anomalies = [s for s in scores if s["is_anomaly"]]

    return {
        "host": host_id,
        "total_snapshots": len(records),
        "anomaly_count": len(anomalies),
        "latest_score": scores[-1] if scores else None,
        "anomalies": anomalies,
    }


@app.post("/predict/anomalies")
async def post_custom_anomalies(records: List[MultivariateRecord]):
    """
    Scores custom multi-dimensional metric points via Isolation Forest.
    """
    data = [r.model_dump() for r in records]
    scores = anomaly_detector.score(data)
    return {
        "total_records": len(records),
        "anomalies_detected": sum(1 for s in scores if s["is_anomaly"]),
        "results": scores,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("sidecar.main:app", host=settings.HOST, port=settings.PORT, reload=True)
