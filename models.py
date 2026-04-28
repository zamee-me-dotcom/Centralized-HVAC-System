"""
Shared Pydantic models and SQLAlchemy schemas for the HVAC Control System.
"""
from __future__ import annotations

import enum
from datetime import datetime
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field, validator
from sqlalchemy import (
    Column, String, Float, Boolean, DateTime, Enum as SAEnum,
    ForeignKey, Index, text
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, relationship


# ─────────────────────────────────────────────
#  Enums
# ─────────────────────────────────────────────

class UnitStatus(str, enum.Enum):
    ON = "ON"
    OFF = "OFF"
    FAULT = "FAULT"
    OFFLINE = "OFFLINE"
    STANDBY = "STANDBY"


class FanSpeed(str, enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    AUTO = "AUTO"


class AlertSeverity(str, enum.Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertType(str, enum.Enum):
    OVERHEAT = "OVERHEAT"
    OFFLINE = "OFFLINE"
    HIGH_LOAD = "HIGH_LOAD"
    POWER_ANOMALY = "POWER_ANOMALY"
    PID_DIVERGE = "PID_DIVERGE"
    SENSOR_FAULT = "SENSOR_FAULT"


# ─────────────────────────────────────────────
#  SQLAlchemy Base
# ─────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


# ─────────────────────────────────────────────
#  ORM Models
# ─────────────────────────────────────────────

class ACUnitORM(Base):
    __tablename__ = "ac_units"

    id = Column(String(64), primary_key=True)
    zone_id = Column(String(64), ForeignKey("zones.id"), nullable=False, index=True)
    name = Column(String(128), nullable=False)
    model = Column(String(128))
    firmware_version = Column(String(32))
    capacity_kw = Column(Float, nullable=False)
    status = Column(SAEnum(UnitStatus), default=UnitStatus.OFFLINE, nullable=False)
    current_temp = Column(Float)
    target_temp = Column(Float, default=22.0)
    fan_speed = Column(SAEnum(FanSpeed), default=FanSpeed.AUTO)
    load_percentage = Column(Float, default=0.0)
    power_consumption_w = Column(Float, default=0.0)
    last_seen = Column(DateTime(timezone=True))
    cert_fingerprint = Column(String(128))  # TLS client cert fingerprint
    tags = Column(JSONB, default={})
    created_at = Column(DateTime(timezone=True), server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), server_default=text("NOW()"), onupdate=text("NOW()"))

    zone = relationship("ZoneORM", back_populates="units")


class ZoneORM(Base):
    __tablename__ = "zones"

    id = Column(String(64), primary_key=True)
    name = Column(String(128), nullable=False)
    building = Column(String(128))
    floor = Column(String(32))
    area_sqm = Column(Float)
    target_temp = Column(Float, default=22.0)
    occupancy = Column(Boolean, default=False)
    metadata_ = Column("metadata", JSONB, default={})
    created_at = Column(DateTime(timezone=True), server_default=text("NOW()"))

    units = relationship("ACUnitORM", back_populates="zone")


class AlertORM(Base):
    __tablename__ = "alerts"
    __table_args__ = (
        Index("idx_alerts_unit_id", "unit_id"),
        Index("idx_alerts_created_at", "created_at"),
    )

    id = Column(String(64), primary_key=True)
    unit_id = Column(String(64), ForeignKey("ac_units.id"), nullable=False)
    zone_id = Column(String(64))
    alert_type = Column(SAEnum(AlertType), nullable=False)
    severity = Column(SAEnum(AlertSeverity), nullable=False)
    message = Column(String(512))
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime(timezone=True))
    payload = Column(JSONB, default={})
    created_at = Column(DateTime(timezone=True), server_default=text("NOW()"))


# ─────────────────────────────────────────────
#  Pydantic Schemas
# ─────────────────────────────────────────────

class ZoneBase(BaseModel):
    id: str
    name: str
    building: Optional[str] = None
    floor: Optional[str] = None
    area_sqm: Optional[float] = None
    target_temp: float = 22.0

    class Config:
        from_attributes = True


class ACUnitBase(BaseModel):
    id: str
    zone_id: str
    name: str
    model: Optional[str] = None
    capacity_kw: float
    status: UnitStatus = UnitStatus.OFFLINE
    current_temp: Optional[float] = None
    target_temp: float = 22.0
    fan_speed: FanSpeed = FanSpeed.AUTO
    load_percentage: float = 0.0
    power_consumption_w: float = 0.0
    last_seen: Optional[datetime] = None
    tags: Dict[str, Any] = {}

    class Config:
        from_attributes = True


class ACUnitCreate(BaseModel):
    id: str = Field(..., min_length=3, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    zone_id: str
    name: str
    model: Optional[str] = None
    capacity_kw: float = Field(..., gt=0, le=1000)
    target_temp: float = Field(22.0, ge=16.0, le=32.0)
    tags: Dict[str, Any] = {}


class ACUnitUpdate(BaseModel):
    name: Optional[str] = None
    zone_id: Optional[str] = None
    target_temp: Optional[float] = Field(None, ge=16.0, le=32.0)
    fan_speed: Optional[FanSpeed] = None
    tags: Optional[Dict[str, Any]] = None


class ControlCommand(BaseModel):
    unit_id: str
    status: Optional[UnitStatus] = None
    target_temp: Optional[float] = Field(None, ge=16.0, le=32.0)
    fan_speed: Optional[FanSpeed] = None
    issued_by: str = "system"
    correlation_id: Optional[str] = None

    @validator("status")
    def status_must_be_controllable(cls, v):
        if v and v not in (UnitStatus.ON, UnitStatus.OFF, UnitStatus.STANDBY):
            raise ValueError(f"Cannot command status: {v}")
        return v


class TelemetryPayload(BaseModel):
    unit_id: str
    timestamp: datetime
    temperature: float = Field(..., ge=-50.0, le=100.0)
    humidity: float = Field(..., ge=0.0, le=100.0)
    pressure: Optional[float] = Field(None, ge=800.0, le=1200.0)
    load: float = Field(..., ge=0.0, le=100.0)
    power_w: float = Field(..., ge=0.0)
    compressor_rpm: Optional[float] = None
    evap_temp: Optional[float] = None
    condenser_temp: Optional[float] = None
    error_codes: List[int] = []


class DigitalTwinState(BaseModel):
    unit_id: str
    zone_id: str
    status: UnitStatus
    current_temp: float
    target_temp: float
    humidity: float
    load_percentage: float
    power_consumption_w: float
    fan_speed: FanSpeed
    pid_output: float = 0.0
    last_telemetry: Optional[datetime] = None
    last_command: Optional[datetime] = None
    online: bool = True
    alert_flags: List[str] = []


class AlertPayload(BaseModel):
    id: str
    unit_id: str
    zone_id: Optional[str] = None
    alert_type: AlertType
    severity: AlertSeverity
    message: str
    payload: Dict[str, Any] = {}
    created_at: datetime


class HeartbeatPayload(BaseModel):
    unit_id: str
    timestamp: datetime
    uptime_s: int
    fw_version: str
    local_mode: bool = False
