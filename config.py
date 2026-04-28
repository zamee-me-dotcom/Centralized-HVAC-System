"""
Centralized configuration using pydantic-settings.
All services import from here; env vars override defaults.
"""
from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── Service identity ──────────────────────────────────────────────
    SERVICE_NAME: str = "hvac-service"
    ENVIRONMENT: str = "development"  # development | staging | production
    LOG_LEVEL: str = "INFO"

    # ── PostgreSQL ────────────────────────────────────────────────────
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "hvac"
    POSTGRES_USER: str = "hvac_user"
    POSTGRES_PASSWORD: str = "changeme"
    POSTGRES_POOL_SIZE: int = 20
    POSTGRES_MAX_OVERFLOW: int = 10

    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def postgres_url_sync(self) -> str:
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # ── InfluxDB ──────────────────────────────────────────────────────
    INFLUX_URL: str = "http://localhost:8086"
    INFLUX_TOKEN: str = "changeme-influx-token"
    INFLUX_ORG: str = "hvac-org"
    INFLUX_BUCKET: str = "hvac-telemetry"

    # ── Redis ─────────────────────────────────────────────────────────
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: Optional[str] = None
    REDIS_DB: int = 0
    REDIS_TWIN_TTL_S: int = 300          # Digital twin key expiry
    REDIS_HEARTBEAT_TTL_S: int = 30      # Heartbeat expiry → offline detection

    @property
    def redis_url(self) -> str:
        auth = f":{self.REDIS_PASSWORD}@" if self.REDIS_PASSWORD else ""
        return f"redis://{auth}{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    # ── MQTT ──────────────────────────────────────────────────────────
    MQTT_HOST: str = "localhost"
    MQTT_PORT: int = 8883  # TLS port
    MQTT_PORT_PLAIN: int = 1883
    MQTT_USERNAME: str = "hvac_backend"
    MQTT_PASSWORD: str = "changeme"
    MQTT_CA_CERT: str = "/certs/ca.crt"
    MQTT_CLIENT_CERT: str = "/certs/backend.crt"
    MQTT_CLIENT_KEY: str = "/certs/backend.key"
    MQTT_KEEPALIVE: int = 60
    MQTT_QOS: int = 1
    MQTT_RETAIN_STATE: bool = True

    # MQTT topic templates
    TOPIC_TELEMETRY: str = "ac/unit/{unit_id}/telemetry"
    TOPIC_COMMAND: str = "ac/unit/{unit_id}/command"
    TOPIC_HEARTBEAT: str = "ac/unit/{unit_id}/heartbeat"
    TOPIC_ZONE_STATE: str = "ac/zone/{zone_id}/state"
    TOPIC_SYSTEM_ALERTS: str = "ac/system/alerts"
    TOPIC_RESPONSE: str = "ac/unit/{unit_id}/response"

    # ── Alert thresholds ──────────────────────────────────────────────
    ALERT_TEMP_HIGH_C: float = 30.0
    ALERT_TEMP_CRITICAL_C: float = 35.0
    ALERT_LOAD_HIGH_PCT: float = 90.0
    ALERT_LOAD_CRITICAL_PCT: float = 98.0
    ALERT_POWER_DEVIATION_PCT: float = 25.0  # % deviation from baseline
    ALERT_OFFLINE_TIMEOUT_S: int = 30

    # ── Control ───────────────────────────────────────────────────────
    LOAD_BALANCE_THRESHOLD_PCT: float = 90.0
    LOAD_BALANCE_MIN_IDLE_UNITS: int = 1

    # ── API ───────────────────────────────────────────────────────────
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_WORKERS: int = 4
    JWT_SECRET: str = "change-this-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 60

    # ── Observability ─────────────────────────────────────────────────
    OTEL_EXPORTER_OTLP_ENDPOINT: Optional[str] = None
    PROMETHEUS_PORT: int = 9090


@lru_cache
def get_settings() -> Settings:
    return Settings()
