"""
Device Service
==============
Manages AC unit and zone registration, metadata, and lifecycle.

Endpoints
---------
GET    /health
GET    /units                 – list / filter units
POST   /units                 – register new unit
GET    /units/{id}            – get unit detail
PUT    /units/{id}            – update unit metadata
DELETE /units/{id}            – deregister unit
POST   /units/{id}/command    – issue immediate control command
GET    /zones                 – list zones
POST   /zones                 – create zone
GET    /zones/{id}/status     – zone aggregate status
"""
from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Depends, Query, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from prometheus_fastapi_instrumentator import Instrumentator

from ..shared.config import get_settings, Settings
from ..shared.models import (
    ACUnitORM, ZoneORM, Base,
    ACUnitBase, ACUnitCreate, ACUnitUpdate,
    ZoneBase, ControlCommand, UnitStatus,
)
from ..shared.mqtt_client import build_mqtt_client

log = logging.getLogger(__name__)
settings = get_settings()

# ── Database ──────────────────────────────────────────────────────────────────
engine = create_async_engine(
    settings.postgres_url,
    pool_size=settings.POSTGRES_POOL_SIZE,
    max_overflow=settings.POSTGRES_MAX_OVERFLOW,
    echo=(settings.ENVIRONMENT == "development"),
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


# ── Redis ─────────────────────────────────────────────────────────────────────
redis_client: aioredis.Redis = None


async def get_redis() -> aioredis.Redis:
    return redis_client


# ── MQTT ──────────────────────────────────────────────────────────────────────
mqtt = build_mqtt_client(settings, client_id="device-service")


# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    # DB migrations (in production use Alembic)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)

    mqtt.connect()
    await mqtt.wait_connected(timeout=15.0)

    log.info("Device Service started")
    yield

    await redis_client.aclose()
    mqtt.disconnect()
    await engine.dispose()
    log.info("Device Service shutdown")


app = FastAPI(
    title="HVAC Device Service",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
Instrumentator().instrument(app).expose(app)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"])
async def health():
    return {"status": "ok", "service": "device-service", "ts": datetime.now(timezone.utc)}


# ── Zone endpoints ────────────────────────────────────────────────────────────

@app.get("/zones", response_model=List[ZoneBase], tags=["zones"])
async def list_zones(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ZoneORM))
    return result.scalars().all()


@app.post("/zones", response_model=ZoneBase, status_code=status.HTTP_201_CREATED, tags=["zones"])
async def create_zone(zone: ZoneBase, db: AsyncSession = Depends(get_db)):
    existing = await db.get(ZoneORM, zone.id)
    if existing:
        raise HTTPException(status_code=409, detail=f"Zone {zone.id} already exists")
    orm = ZoneORM(**zone.model_dump())
    db.add(orm)
    await db.commit()
    await db.refresh(orm)
    return orm


@app.get("/zones/{zone_id}/status", tags=["zones"])
async def zone_status(zone_id: str, db: AsyncSession = Depends(get_db), redis: aioredis.Redis = Depends(get_redis)):
    zone = await db.get(ZoneORM, zone_id)
    if not zone:
        raise HTTPException(status_code=404, detail="Zone not found")

    result = await db.execute(select(ACUnitORM).where(ACUnitORM.zone_id == zone_id))
    units = result.scalars().all()

    online = [u for u in units if u.status not in (UnitStatus.OFFLINE, UnitStatus.FAULT)]
    avg_temp = sum(u.current_temp for u in online if u.current_temp) / max(len(online), 1)
    total_power = sum(u.power_consumption_w for u in units)

    return {
        "zone_id": zone_id,
        "zone_name": zone.name,
        "total_units": len(units),
        "online_units": len(online),
        "avg_temperature": round(avg_temp, 2),
        "total_power_w": round(total_power, 2),
        "target_temp": zone.target_temp,
        "ts": datetime.now(timezone.utc),
    }


# ── Unit endpoints ────────────────────────────────────────────────────────────

@app.get("/units", response_model=List[ACUnitBase], tags=["units"])
async def list_units(
    zone_id: Optional[str] = Query(None),
    status: Optional[UnitStatus] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(ACUnitORM)
    if zone_id:
        stmt = stmt.where(ACUnitORM.zone_id == zone_id)
    if status:
        stmt = stmt.where(ACUnitORM.status == status)
    result = await db.execute(stmt)
    return result.scalars().all()


@app.post("/units", response_model=ACUnitBase, status_code=status.HTTP_201_CREATED, tags=["units"])
async def register_unit(payload: ACUnitCreate, db: AsyncSession = Depends(get_db)):
    # Ensure zone exists
    zone = await db.get(ZoneORM, payload.zone_id)
    if not zone:
        raise HTTPException(status_code=404, detail=f"Zone {payload.zone_id} not found")

    existing = await db.get(ACUnitORM, payload.id)
    if existing:
        raise HTTPException(status_code=409, detail=f"Unit {payload.id} already registered")

    orm = ACUnitORM(
        id=payload.id,
        zone_id=payload.zone_id,
        name=payload.name,
        model=payload.model,
        capacity_kw=payload.capacity_kw,
        target_temp=payload.target_temp,
        tags=payload.tags,
    )
    db.add(orm)
    await db.commit()
    await db.refresh(orm)

    log.info("Registered AC unit %s in zone %s", payload.id, payload.zone_id)
    return orm


@app.get("/units/{unit_id}", response_model=ACUnitBase, tags=["units"])
async def get_unit(unit_id: str, db: AsyncSession = Depends(get_db)):
    unit = await db.get(ACUnitORM, unit_id)
    if not unit:
        raise HTTPException(status_code=404, detail="Unit not found")
    return unit


@app.put("/units/{unit_id}", response_model=ACUnitBase, tags=["units"])
async def update_unit(unit_id: str, payload: ACUnitUpdate, db: AsyncSession = Depends(get_db)):
    unit = await db.get(ACUnitORM, unit_id)
    if not unit:
        raise HTTPException(status_code=404, detail="Unit not found")

    update_data = payload.model_dump(exclude_none=True)
    for k, v in update_data.items():
        setattr(unit, k, v)

    await db.commit()
    await db.refresh(unit)
    return unit


@app.delete("/units/{unit_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["units"])
async def deregister_unit(unit_id: str, db: AsyncSession = Depends(get_db)):
    unit = await db.get(ACUnitORM, unit_id)
    if not unit:
        raise HTTPException(status_code=404, detail="Unit not found")
    await db.delete(unit)
    await db.commit()


@app.post("/units/{unit_id}/command", tags=["units"])
async def send_command(unit_id: str, cmd: ControlCommand, db: AsyncSession = Depends(get_db)):
    """Issue an immediate control command to an AC unit via MQTT."""
    unit = await db.get(ACUnitORM, unit_id)
    if not unit:
        raise HTTPException(status_code=404, detail="Unit not found")

    if unit.status == UnitStatus.OFFLINE:
        raise HTTPException(status_code=409, detail="Unit is offline; command queued but delivery not guaranteed")

    cmd.unit_id = unit_id
    if not cmd.correlation_id:
        cmd.correlation_id = str(uuid.uuid4())

    topic = settings.TOPIC_COMMAND.format(unit_id=unit_id)
    await mqtt.async_publish(topic, cmd.model_dump())

    log.info("Issued command to %s: %s (corr=%s)", unit_id, cmd.model_dump(exclude_none=True), cmd.correlation_id)
    return {"status": "published", "topic": topic, "correlation_id": cmd.correlation_id}
