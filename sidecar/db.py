import asyncpg
import logging
from typing import Optional, List, Dict, Any
from sidecar.config import settings

logger = logging.getLogger("sidecar.db")

class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(
                dsn=settings.DATABASE_URL,
                min_size=2,
                max_size=10,
                timeout=5.0
            )
            logger.info("Connected to TimescaleDB asyncpg pool")
        except Exception as e:
            logger.warning(f"Could not connect to TimescaleDB: {e}. Running in standalone model mode.")

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            logger.info("TimescaleDB pool closed")

    async def fetch_metric_history(
        self,
        host_id: str,
        metric_name: str,
        hours: int = 6
    ) -> List[Dict[str, Any]]:
        """
        Fetches historical time-series points for a host metric.
        Uses 1m continuous aggregate rollup when available, otherwise raw metrics.
        """
        if not self.pool:
            return []

        query = """
            SELECT bucket AS ts, (sum_val / NULLIF(count_val, 0)) AS val
            FROM metrics_rollup_1m r
            JOIN metric_definitions md ON md.id = r.metric_id
            JOIN hosts h ON h.host_id = r.host_id
            WHERE h.host_id = $1
              AND md.name   = $2
              AND r.bucket  >= NOW() - ($3 * INTERVAL '1 hour')
            ORDER BY r.bucket ASC;
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, host_id, metric_name, hours)
            if not rows:
                # Fallback to raw metrics
                raw_query = """
                    SELECT m.ts, m.value AS val
                    FROM metrics m
                    JOIN metric_definitions md ON md.id = m.metric_id
                    JOIN hosts h ON h.host_id = m.host_id
                    WHERE h.host_id = $1
                      AND md.name   = $2
                      AND m.ts      >= NOW() - ($3 * INTERVAL '1 hour')
                    ORDER BY m.ts ASC;
                """
                rows = await conn.fetch(raw_query, host_id, metric_name, hours)

            return [{"ts": r["ts"].isoformat(), "value": float(r["val"])} for r in rows]

    async def fetch_multivariate_snapshot(
        self,
        host_id: str,
        hours: int = 2
    ) -> List[Dict[str, Any]]:
        """
        Fetches synchronized multi-metric points (CPU, Memory, Disk, Network)
        bucketed per minute for multivariate anomaly detection.
        """
        if not self.pool:
            return []

        query = """
            SELECT 
                r.bucket AS ts,
                MAX(CASE WHEN md.name = 'system.cpu.total' THEN r.sum_val / NULLIF(r.count_val, 0) END) AS cpu,
                MAX(CASE WHEN md.name = 'system.memory.used_percent' THEN r.sum_val / NULLIF(r.count_val, 0) END) AS memory,
                MAX(CASE WHEN md.name = 'system.disk.used_percent' THEN r.sum_val / NULLIF(r.count_val, 0) END) AS disk,
                MAX(CASE WHEN md.name = 'system.network.bytes_sent' THEN r.sum_val / NULLIF(r.count_val, 0) END) AS net_sent
            FROM metrics_rollup_1m r
            JOIN metric_definitions md ON md.id = r.metric_id
            WHERE r.host_id = $1
              AND r.bucket >= NOW() - ($2 * INTERVAL '1 hour')
            GROUP BY r.bucket
            ORDER BY r.bucket ASC;
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, host_id, hours)
            result = []
            for r in rows:
                if r["cpu"] is not None and r["memory"] is not None:
                    result.append({
                        "ts": r["ts"].isoformat(),
                        "cpu": float(r["cpu"]),
                        "memory": float(r["memory"]),
                        "disk": float(r["disk"]) if r["disk"] is not None else 0.0,
                        "net_sent": float(r["net_sent"]) if r["net_sent"] is not None else 0.0,
                    })
            return result

db = Database()
