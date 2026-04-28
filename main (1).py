"""
Digital Twin Service
====================
Maintains a real-time virtual replica of every AC unit.

Responsibilities
----------------
* Consume processed telemetry events and keep an in-memory + Redis cache of
  each unit's current state.
* Expose REST & WebSocket endpoints so dashboards can query or stream twin data.
* Detect state drift (e.g., unit temp rising despite ON command → potential fault).
* Emit zone-level aggregate state to ac/zone/{zone_id}/state.

Architecture
------------
State lives in Redis hashes:
    twin:{unit_id}     → flattened current state (fast read)
    twins:index        → sorted set of unit IDs by last-seen timestamp

Zone aggregates are also cached:
    zone:{zone_id}:agg → JSON aggregate pushed every ZONE_AGG_INTERVAL_S
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from ..shared.config import get_settings
from ..shared.models import (
    ACUnitORM, ZoneORM, DigitalTwinState, TelemetryPayload,
    UnitStatus, FanSpeed,
)
from ..shared.mqtt_client import build_mqtt_client

log = logging.getLogger(__name__)
settings = get_settings()
ZONE_AGG_INTERVAL_S = 5.0
DRIFT_CHECK_INTERVAL_S = 30.0
DRIFT_THRESHOLD_C = 3.0  # if temp deviates this much from setpoint for >N cycles → alert

engine = create_async_engine(settings.postgres_url, pool_size=10)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
redis_client: aioredis.Redis = None
mqtt = build_mqtt_client(settings, client_id="digital-twin")

# In-process cache (mirror of Redis for zero-latency reads)
_twin_cache: Dict[str, DigitalTwinState] = {}
_ws_connections: List[WebSocket] = []


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _load_unit_metadata() -> Dict[str, ACUnitORM]:
    """Pre-load all unit metadata into memory at startup."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(ACUnitORM))
        units = result.scalars().all()
    return {u.id: u for u in units}


async def _bootstrap_twins(unit_map: Dict[str, ACUnitORM]) -> None:
    """Seed digital twin cache from DB state (cold start)."""
    for uid, unit in unit_map.items():
        state = DigitalTwinState(
            unit_id=uid,
            zone_id=unit.zone_id,
            status=unit.status,
            current_temp=unit.current_temp or 25.0,
            target_temp=unit.target_temp,
            humidity=50.0,
            load_percentage=unit.load_percentage,
            power_consumption_w=unit.power_consumption_w,
            fan_speed=unit.fan_speed,
            online=(unit.status not in (UnitStatus.OFFLINE, UnitStatus.FAULT)),
        )
        _twin_cache[uid] = state
        await _persist_to_redis(state)


async def _persist_to_redis(state: DigitalTwinState) -> None:
    key = f"twin:{state.unit_id}"
    mapping = {
        "unit_id": state.unit_id,
        "zone_id": state.zone_id,
        "status": state.status.value,
        "current_temp": state.current_temp,
        "target_temp": state.target_temp,
        "humidity": state.humidity,
        "load_percentage": state.load_percentage,
        "power_consumption_w": state.power_consumption_w,
        "fan_speed": state.fan_speed.value,
        "pid_output": state.pid_output,
        "online": int(state.online),
        "alert_flags": json.dumps(state.alert_flags),
        "last_telemetry": state.last_telemetry.isoformat() if state.last_telemetry else "",
    }
    await redis_client.hset(key, mapping=mapping)
    await redis_client.expire(key, settings.REDIS_TWIN_TTL_S)
    await redis_client.zadd("twins:index", {state.unit_id: datetime.now(timezone.utc).timestamp()})


# ── MQTT handler ──────────────────────────────────────────────────────────────

async def handle_processed_telemetry(topic: str, payload_bytes: bytes) -> None:
    unit_id = topic.split("/")[2]

    try:
        raw = json.loads(payload_bytes)
        t = TelemetryPayload(**raw)
    except Exception as exc:
        log.warning("Bad processed telemetry for %s: %s", unit_id, exc)
        return

    # Fetch or create twin
    twin = _twin_cache.get(unit_id)
    if not twin:
        # New unit appeared – create stub
        twin = DigitalTwinState(
            unit_id=unit_id,
            zone_id="unknown",
            status=UnitStatus.ON,
            current_temp=t.temperature,
            target_temp=22.0,
            humidity=t.humidity,
            load_percentage=t.load,
            power_consumption_w=t.power_w,
            fan_speed=FanSpeed.AUTO,
            online=True,
        )

    # Update twin fields
    twin.current_temp = t.temperature
    twin.humidity = t.humidity
    twin.load_percentage = t.load
    twin.power_consumption_w = t.power_w
    twin.last_telemetry = t.timestamp
    twin.online = True
    twin.status = UnitStatus.ON

    # Simple drift detection
    drift = abs(twin.current_temp - twin.target_temp)
    if drift > DRIFT_THRESHOLD_C:
        flag = f"DRIFT:{drift:.1f}C"
        if flag not in twin.alert_flags:
            twin.alert_flags.append(flag)
            log.warning("Twin %s temp drift %.1f°C", unit_id, drift)
    else:
        twin.alert_flags = [f for f in twin.alert_flags if not f.startswith("DRIFT")]

    _twin_cache[unit_id] = twin
    await _persist_to_redis(twin)

    # Broadcast to WebSocket clients
    if _ws_connections:
        msg = json.dumps({"event": "telemetry", "unit_id": unit_id, "state": twin.model_dump(), "ts": datetime.now(timezone.utc).isoformat()}, default=str)
        dead = []
        for ws in _ws_connections:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            _ws_connections.remove(ws)


# ── Zone aggregation loop ─────────────────────────────────────────────────────

async def zone_aggregation_loop() -> None:
    while True:
        await asyncio.sleep(ZONE_AGG_INTERVAL_S)
        zone_buckets: Dict[str, List[DigitalTwinState]] = defaultdict(list)
        for twin in _twin_cache.values():
            zone_buckets[twin.zone_id].append(twin)

        for zone_id, twins in zone_buckets.items():
            online = [t for t in twins if t.online]
            if not online:
                continue
            agg = {
                "zone_id": zone_id,
                "total_units": len(twins),
                "online_units": len(online),
                "avg_temp": round(sum(t.current_temp for t in online) / len(online), 2),
                "avg_load": round(sum(t.load_percentage for t in online) / len(online), 2),
                "total_power_w": round(sum(t.power_consumption_w for t in twins), 2),
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            topic = settings.TOPIC_ZONE_STATE.format(zone_id=zone_id)
            await mqtt.async_publish(topic, agg, retain=True)
            await redis_client.set(f"zone:{zone_id}:agg", json.dumps(agg), ex=60)


# ── App ───────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
    mqtt.connect()
    await mqtt.wait_connected(timeout=20.0)

    unit_map = await _load_unit_metadata()
    await _bootstrap_twins(unit_map)

    mqtt.subscribe("ac/unit/+/telemetry/processed", handle_processed_telemetry)
    zone_task = asyncio.create_task(zone_aggregation_loop())

    log.info("Digital Twin Service ready – %d twins bootstrapped", len(unit_map))
    yield

    zone_task.cancel()
    mqtt.disconnect()
    await redis_client.aclose()
    await engine.dispose()


app = FastAPI(title="HVAC Digital Twin Service", version="1.0.0", lifespan=lifespan)


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "digital-twin", "twins": len(_twin_cache)}


@app.get("/twins", response_model=List[DigitalTwinState])
async def list_twins(zone_id: Optional[str] = None, online_only: bool = False):
    twins = list(_twin_cache.values())
    if zone_id:
        twins = [t for t in twins if t.zone_id == zone_id]
    if online_only:
        twins = [t for t in twins if t.online]
    return twins


@app.get("/twins/{unit_id}", response_model=DigitalTwinState)
async def get_twin(unit_id: str):
    twin = _twin_cache.get(unit_id)
    if not twin:
        # Try Redis fallback
        raw = await redis_client.hgetall(f"twin:{unit_id}")
        if not raw:
            raise HTTPException(status_code=404, detail="Twin not found")
        return DigitalTwinState(
            unit_id=raw["unit_id"],
            zone_id=raw["zone_id"],
            status=UnitStatus(raw["status"]),
            current_temp=float(raw["current_temp"]),
            target_temp=float(raw["target_temp"]),
            humidity=float(raw["humidity"]),
            load_percentage=float(raw["load_percentage"]),
            power_consumption_w=float(raw["power_consumption_w"]),
            fan_speed=FanSpeed(raw["fan_speed"]),
            online=bool(int(raw.get("online", 0))),
        )
    return twin


@app.get("/zones/{zone_id}/aggregate")
async def zone_aggregate(zone_id: str):
    raw = await redis_client.get(f"zone:{zone_id}:agg")
    if not raw:
        raise HTTPException(status_code=404, detail="No aggregate data for this zone yet")
    return json.loads(raw)


# ── WebSocket streaming ───────────────────────────────────────────────────────

@app.websocket("/ws/stream")
async def telemetry_stream(websocket: WebSocket):
    """Stream all twin updates in real-time. Clients can filter by zone_id."""
    await websocket.accept()
    params = websocket.query_params
    filter_zone = params.get("zone_id")
    _ws_connections.append(websocket)

    try:
        # Send initial snapshot
        twins = [t for t in _twin_cache.values() if not filter_zone or t.zone_id == filter_zone]
        await websocket.send_text(json.dumps({
            "event": "snapshot",
            "twins": [t.model_dump() for t in twins],
            "ts": datetime.now(timezone.utc).isoformat(),
        }, default=str))

        # Keep alive with ping
        while True:
            await asyncio.sleep(30)
            await websocket.send_text(json.dumps({"event": "ping"}))
    except WebSocketDisconnect:
        _ws_connections.remove(websocket)
