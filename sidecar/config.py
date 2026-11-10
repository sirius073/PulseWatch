import os
from pydantic import BaseModel

class Settings(BaseModel):
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgres://pulsewatch:pulsewatch_secret@localhost:5432/pulsewatch"
    )
    PORT: int = int(os.getenv("SIDECAR_PORT", "8000"))
    HOST: str = os.getenv("SIDECAR_HOST", "0.0.0.0")
    DEFAULT_LOOKBACK_HOURS: int = 6
    RUNWAY_CRITICAL_HOURS: float = 24.0  # Alert if exhaustion within 24h

settings = Settings()
