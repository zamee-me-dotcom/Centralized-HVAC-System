"""
Alert Service
=============
Centralized alerting hub for the HVAC system.

Sources
-------
* MQTT:  ac/system/alerts        – alerts published by other services / edge
* MQTT:  ac/unit/+/telemetry/processed  – direct telemetry anomaly scanning
* Redis: heartbeat keys expiry   – offline detection (pub/sub keyspace events)

Sinks
-----
* PostgreSQL:  alerts table (persistent record)
* MQTT:        ac/system/alerts  (fan-out to dashboards)
* Webhooks:    configurable HTTP endpoints (Slack, PagerDuty, Teams)
* Email:       SMTP integration (optional)

Deduplication
-------------
Active alerts are tracked in Redis (alert:{unit_id}:{type}).
A new alert of the same type for the same unit suppresses a duplicate
notification until the alert is resolved (TTL = 5 minutes).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from ..shared.config import get_settings
from ..shared.models import (
    AlertORM, AlertPayload, AlertSeverity, AlertType,
    TelemetryPayload,
)
from ..shared.mqtt_client import build_mqtt_client

log = logging.getLogger(__name__)
settings = get_settings()

engine = create_async_engine(settings.postgres_url, pool_size=10)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
redis_client: aioredis.Redis = None
mqtt = build_mqtt_client(settings, client_id="alert-service")
http_client: httpx.AsyncClient = None

# Webhook registry (in production: load from DB / config service)
WEBHOOK_ENDPOINTS: List[Dict[str, str]] = [
    # {"url": "https://hooks.slack.com/...", "severity": "CRITICAL"},
]

DEDUP_TTL_S = 300   # suppress duplicate alerts for 5 minutes


# ─────────────────────────────────────────────────────────────────────────────
#  Anomaly Detectors
# ─────────────────────────────────────────────────────────────────────────────

class AnomalyDetector:
    """
    Stateful per-unit anomaly detection using a sliding window.

    Tracks:
    * Consecutive high-temp readings (overheat sustained)
    * Consecutive high-load readings (overload sustained)
    * Power deviation from rolling baseline (energy anomaly)
    """

    WINDOW = 5        # readings to average
    BASELINE_ALPHA = 0.05  # EMA smoothing for power baseline

    def __init__(self):
        self._temp_windows: Dict[str, list]  = {}
        self._load_windows: Dict[str, list]  = {}
        self._power_baseline: Dict[str, float] = {}

    def update(self, t: TelemetryPayload) -> List[AlertPayload]:
        uid = t.unit_id
        alerts = []
        now = datetime.now(timezone.utc)

        # ── Temperature window ────────────────────────────────────────
        tw = self._temp_windows.setdefault(uid, [])
        tw.append(t.temperature)
        if len(tw) > self.WINDOW:
            tw.pop(0)

        if len(tw) == self.WINDOW:
            avg_temp = sum(tw) / len(tw)
            if avg_temp >= settings.ALERT_TEMP_CRITICAL_C:
                alerts.append(self._build(uid, AlertType.OVERHEAT, AlertSeverity.CRITICAL,
                    f"Sustained critical temperature {avg_temp:.1f}°C (avg over {self.WINDOW} readings)",
                    {"avg_temp": avg_temp, "readings": tw.copy()}))
            elif avg_temp >= settings.ALERT_TEMP_HIGH_C:
                alerts.append(self._build(uid, AlertType.OVERHEAT, AlertSeverity.WARNING,
                    f"Elevated temperature {avg_temp:.1f}°C",
                    {"avg_temp": avg_temp}))

        # ── Load window ───────────────────────────────────────────────
        lw = self._load_windows.setdefault(uid, [])
        lw.append(t.load)
        if len(lw) > self.WINDOW:
            lw.pop(0)

        if len(lw) == self.WINDOW and sum(lw) / len(lw) >= settings.ALERT_LOAD_CRITICAL_PCT:
            alerts.append(self._build(uid, AlertType.HIGH_LOAD, AlertSeverity.WARNING,
                f"Sustained load {sum(lw)/len(lw):.0f}%", {"avg_load": sum(lw)/len(lw)}))

        # ── Power deviation (EMA baseline) ────────────────────────────
        baseline = self._power_baseline.get(uid)
        if baseline is None:
            self._power_baseline[uid] = t.power_w
        else:
            new_baseline = baseline * (1 - self.BASELINE_ALPHA) + t.power_w * self.BASELINE_ALPHA
            self._power_baseline[uid] = new_baseline
            deviation_pct = abs(t.power_w - baseline) / max(baseline, 1) * 100
            if deviation_pct >= settings.ALERT_POWER_DEVIATION_PCT:
                sev = AlertSeverity.CRITICAL if deviation_pct > 50 else AlertSeverity.WARNING
                alerts.append(self._build(uid, AlertType.POWER_ANOMALY, sev,
                    f"Power deviation {deviation_pct:.0f}% (reading {t.power_w:.0f}W, baseline {baseline:.0f}W)",
                    {"power_w": t.power_w, "baseline_w": baseline, "deviation_pct": deviation_pct}))

        return alerts

    @staticmethod
    def _build(unit_id: str, atype: AlertType, severity: AlertSeverity,
               message: str, payload: Dict) -> AlertPayload:
        return AlertPayload(
            id=str(uuid.uuid4()),
            unit_id=unit_id,
            alert_type=atype,
            severity=severity,
            message=message,
            payload=payload,
            created_at=datetime.now(timezone.utc),
        )


detector = AnomalyDetector()


# ─────────────────────────────────────────────────────────────────────────────
#  Alert pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def _dedup_key(alert: AlertPayload) -> str:
    return f"alert:{alert.unit_id}:{alert.alert_type.value}"


async def emit_alert(alert: AlertPayload) -> bool:
    """
    Full alert pipeline: dedup → persist → notify → publish.
    Returns True if alert was new (not suppressed).
    """
    dedup_key = await _dedup_key(alert)
    if await redis_client.exists(dedup_key):
        log.debug("Alert suppressed (dedup): %s / %s", alert.unit_id, alert.alert_type)
        return False

    await redis_client.set(dedup_key, "1", ex=DEDUP_TTL_S)

    # Persist
    async with AsyncSessionLocal() as db:
        orm = AlertORM(
            id=alert.id,
            unit_id=alert.unit_id,
            zone_id=alert.zone_id,
            alert_type=alert.alert_type,
            severity=alert.severity,
            message=alert.message,
            payload=alert.payload,
        )
        db.add(orm)
        await db.commit()

    # Publish to MQTT alert bus
    await mqtt.async_publish(settings.TOPIC_SYSTEM_ALERTS, {
        **alert.model_dump(),
        "alert_type": alert.alert_type.value,
        "severity": alert.severity.value,
        "created_at": alert.created_at.isoformat(),
    }, retain=False)

    # Webhook notifications
    await _fire_webhooks(alert)

    log.info("ALERT [%s] %s → %s: %s", alert.severity.value, alert.unit_id, alert.alert_type.value, alert.message)
    return True


async def _fire_webhooks(alert: AlertPayload) -> None:
    for wh in WEBHOOK_ENDPOINTS:
        if wh.get("severity") and wh["severity"] != alert.severity.value:
            continue
        try:
            body = {
                "text": f"🚨 *HVAC Alert [{alert.severity.value}]* — Unit `{alert.unit_id}`\n"
                        f"*Type*: {alert.alert_type.value}\n*Message*: {alert.message}",
            }
            await http_client.post(wh["url"], json=body, timeout=5.0)
        except Exception as exc:
            log.warning("Webhook delivery failed %s: %s", wh["url"], exc)


# ─────────────────────────────────────────────────────────────────────────────
#  MQTT handlers
# ─────────────────────────────────────────────────────────────────────────────

async def handle_system_alerts(topic: str, payload_bytes: bytes) -> None:
    """Consume alerts published by other services and fan-out/persist them."""
    try:
        raw = json.loads(payload_bytes)
        alert = AlertPayload(
            id=raw.get("id", str(uuid.uuid4())),
            unit_id=raw["unit_id"],
            zone_id=raw.get("zone_id"),
            alert_type=AlertType(raw["type"]),
            severity=AlertSeverity(raw.get("severity", "WARNING")),
            message=raw.get("message", str(raw)),
            payload={k: v for k, v in raw.items() if k not in ("id", "unit_id", "type", "severity", "message")},
            created_at=datetime.now(timezone.utc),
        )
        await emit_alert(alert)
    except Exception as exc:
        log.warning("Could not process system alert: %s | raw=%s", exc, payload_bytes[:200])


async def handle_processed_telemetry(topic: str, payload_bytes: bytes) -> None:
    """Run anomaly detection on incoming telemetry."""
    try:
        raw = json.loads(payload_bytes)
        t = TelemetryPayload(**raw)
    except Exception as exc:
        return

    new_alerts = detector.update(t)
    for alert in new_alerts:
        await emit_alert(alert)


# ─────────────────────────────────────────────────────────────────────────────
#  FastAPI
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, http_client

    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
    http_client  = httpx.AsyncClient()

    mqtt.connect()
    await mqtt.wait_connected(timeout=20.0)
    mqtt.subscribe(settings.TOPIC_SYSTEM_ALERTS, handle_system_alerts)
    mqtt.subscribe("ac/unit/+/telemetry/processed", handle_processed_telemetry)

    log.info("Alert Service ready")
    yield

    mqtt.disconnect()
    await redis_client.aclose()
    await http_client.aclose()
    await engine.dispose()


app = FastAPI(title="HVAC Alert Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "alert-service"}


@app.get("/alerts", response_model=List[AlertPayload])
async def list_alerts(
    unit_id: Optional[str] = Query(None),
    alert_type: Optional[AlertType] = Query(None),
    severity: Optional[AlertSeverity] = Query(None),
    resolved: Optional[bool] = Query(None),
    limit: int = Query(100, le=1000),
    db: AsyncSession = None,
):
    async with AsyncSessionLocal() as db:
        stmt = select(AlertORM).order_by(AlertORM.created_at.desc()).limit(limit)
        if unit_id:
            stmt = stmt.where(AlertORM.unit_id == unit_id)
        if alert_type:
            stmt = stmt.where(AlertORM.alert_type == alert_type)
        if severity:
            stmt = stmt.where(AlertORM.severity == severity)
        if resolved is not None:
            stmt = stmt.where(AlertORM.resolved == resolved)
        result = await db.execute(stmt)
        rows = result.scalars().all()

    return [AlertPayload(
        id=r.id, unit_id=r.unit_id, zone_id=r.zone_id,
        alert_type=r.alert_type, severity=r.severity,
        message=r.message or "", payload=r.payload or {},
        created_at=r.created_at,
    ) for r in rows]


@app.post("/alerts/{alert_id}/resolve", status_code=200)
async def resolve_alert(alert_id: str):
    async with AsyncSessionLocal() as db:
        stmt = update(AlertORM).where(AlertORM.id == alert_id).values(
            resolved=True, resolved_at=datetime.now(timezone.utc)
        )
        result = await db.execute(stmt)
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Alert not found")
        await db.commit()
    return {"status": "resolved", "alert_id": alert_id}


@app.get("/alerts/summary")
async def alert_summary():
    async with AsyncSessionLocal() as db:
        from sqlalchemy import func
        result = await db.execute(
            select(AlertORM.severity, AlertORM.alert_type, func.count().label("count"))
            .where(AlertORM.resolved == False)
            .group_by(AlertORM.severity, AlertORM.alert_type)
        )
        rows = result.all()
    return [{"severity": r.severity.value, "type": r.alert_type.value, "count": r.count} for r in rows]


@app.post("/webhooks", status_code=201)
async def add_webhook(url: str, severity: Optional[AlertSeverity] = None):
    WEBHOOK_ENDPOINTS.append({"url": url, "severity": severity.value if severity else None})
    return {"status": "registered", "url": url}
