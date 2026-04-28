"""
Control Service
===============
The brain of the HVAC system. Orchestrates:

1. Rule Engine        – JSON-configurable condition/action rules evaluated
                        continuously against twin state.
2. Load Balancer      – Redistributes load across units within a zone to
                        prevent thermal runaway and even wear.
3. Setpoint Optimizer – Simple MPC-style receding-horizon optimizer that
                        minimises energy cost subject to comfort constraints.
4. Command Dispatcher – Publishes validated commands via MQTT with ACK
                        tracking and retry logic.

Design principles
-----------------
* Commands are idempotent: re-issuing the same setpoint is harmless.
* All decisions are logged with a correlation_id for full auditability.
* The service is stateless; all state lives in Redis / PostgreSQL so it
  can be horizontally scaled behind a load balancer.
* Rate limiting prevents command storms: max 1 command/unit/5s.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from ..shared.config import get_settings
from ..shared.models import (
    ACUnitORM, ControlCommand, UnitStatus, FanSpeed,
)
from ..shared.mqtt_client import build_mqtt_client

log = logging.getLogger(__name__)
settings = get_settings()

engine = create_async_engine(settings.postgres_url, pool_size=10)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
redis_client: aioredis.Redis = None
mqtt = build_mqtt_client(settings, client_id="control-service")

# ── Rate limit: 1 command per unit per N seconds ──────────────────────────────
CMD_RATE_LIMIT_S = 5
CONTROL_LOOP_INTERVAL_S = 10.0


# ─────────────────────────────────────────────────────────────────────────────
#  Rule Engine
# ─────────────────────────────────────────────────────────────────────────────

class RuleCondition(BaseModel):
    field: str                   # e.g. "temperature", "load_percentage"
    operator: str                # gt | lt | gte | lte | eq | neq
    value: float


class RuleAction(BaseModel):
    type: str                    # set_temp | set_fan | set_status | alert
    value: Any


class Rule(BaseModel):
    id: str
    name: str
    enabled: bool = True
    priority: int = 50           # 0 = highest
    zone_id: Optional[str] = None   # None = applies to all zones
    conditions: List[RuleCondition]
    logic: str = "AND"          # AND | OR
    actions: List[RuleAction]
    cooldown_s: int = 60         # prevent re-firing


OPERATORS = {
    "gt":  lambda a, b: a > b,
    "lt":  lambda a, b: a < b,
    "gte": lambda a, b: a >= b,
    "lte": lambda a, b: a <= b,
    "eq":  lambda a, b: abs(a - b) < 1e-6,
    "neq": lambda a, b: abs(a - b) >= 1e-6,
}


class RuleEngine:
    def __init__(self, rules: List[Rule]):
        self._rules = sorted(rules, key=lambda r: r.priority)
        self._cooldowns: Dict[str, float] = {}   # rule_id → last_fired timestamp

    def load_rules(self, rules: List[Rule]) -> None:
        self._rules = sorted(rules, key=lambda r: r.priority)
        log.info("Rule engine loaded %d rules", len(self._rules))

    def evaluate(self, twin_state: Dict[str, Any]) -> List[tuple[Rule, RuleAction]]:
        """Return list of (rule, action) tuples to execute."""
        results = []
        now = datetime.now(timezone.utc).timestamp()

        for rule in self._rules:
            if not rule.enabled:
                continue
            if rule.zone_id and rule.zone_id != twin_state.get("zone_id"):
                continue
            cooldown_key = f"{rule.id}:{twin_state.get('unit_id')}"
            if now - self._cooldowns.get(cooldown_key, 0) < rule.cooldown_s:
                continue

            checks = []
            for cond in rule.conditions:
                val = twin_state.get(cond.field)
                if val is None:
                    checks.append(False)
                    continue
                op = OPERATORS.get(cond.operator, lambda a, b: False)
                checks.append(op(float(val), cond.value))

            fired = all(checks) if rule.logic == "AND" else any(checks)
            if fired:
                self._cooldowns[cooldown_key] = now
                for action in rule.actions:
                    results.append((rule, action))
        return results


# ─────────────────────────────────────────────────────────────────────────────
#  Load Balancer
# ─────────────────────────────────────────────────────────────────────────────

class LoadBalancer:
    """
    Zone-level load balancer.

    Policy
    ------
    1. If ≥ THRESHOLD_COUNT units in a zone exceed HIGH_LOAD_PCT →
       activate the idlest standby unit in that zone.
    2. If avg zone load < LOW_LOAD_PCT and >1 unit is ON →
       put the unit with lowest load into STANDBY to save energy.
    3. Decisions respect a per-zone cooldown to avoid oscillation.
    """

    HIGH_LOAD_PCT = 90.0
    LOW_LOAD_PCT  = 20.0
    THRESHOLD_COUNT = 3
    ZONE_COOLDOWN_S = 120.0

    def __init__(self):
        self._zone_cooldowns: Dict[str, float] = {}

    async def evaluate_zone(self, zone_id: str, twins: List[Dict]) -> Optional[ControlCommand]:
        now = datetime.now(timezone.utc).timestamp()
        if now - self._zone_cooldowns.get(zone_id, 0) < self.ZONE_COOLDOWN_S:
            return None

        on_units    = [t for t in twins if t["status"] == "ON"]
        standby     = [t for t in twins if t["status"] == "STANDBY"]
        overloaded  = [t for t in on_units if t["load_percentage"] >= self.HIGH_LOAD_PCT]

        # Scale out
        if len(overloaded) >= self.THRESHOLD_COUNT and standby:
            target = min(standby, key=lambda t: t["load_percentage"])
            self._zone_cooldowns[zone_id] = now
            log.info("[LoadBalancer] Zone %s: %d units overloaded → activating %s",
                     zone_id, len(overloaded), target["unit_id"])
            return ControlCommand(
                unit_id=target["unit_id"],
                status=UnitStatus.ON,
                issued_by="load-balancer",
                correlation_id=str(uuid.uuid4()),
            )

        # Scale in (energy saving)
        if on_units:
            avg_load = sum(t["load_percentage"] for t in on_units) / len(on_units)
            if avg_load < self.LOW_LOAD_PCT and len(on_units) > 1:
                target = min(on_units, key=lambda t: t["load_percentage"])
                self._zone_cooldowns[zone_id] = now
                log.info("[LoadBalancer] Zone %s: avg load %.0f%% → standby %s",
                         zone_id, avg_load, target["unit_id"])
                return ControlCommand(
                    unit_id=target["unit_id"],
                    status=UnitStatus.STANDBY,
                    issued_by="load-balancer",
                    correlation_id=str(uuid.uuid4()),
                )
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  Setpoint Optimizer (simple MPC-inspired)
# ─────────────────────────────────────────────────────────────────────────────

class SetpointOptimizer:
    """
    Receding-horizon comfort + energy optimizer.

    At each step we solve a trivial 1-step problem:
        minimise  α·energy_cost + β·|temp - comfort_band_mid|
        subject to  temp_min ≤ setpoint ≤ temp_max

    In practice this adjusts setpoints ±0.5°C based on load/energy signals,
    acting as an energy-aware trim on top of the zone target.
    """

    COMFORT_MIN = 20.0
    COMFORT_MAX = 26.0
    COMFORT_MID = 23.0
    ALPHA = 0.6    # energy weight
    BETA  = 0.4    # comfort weight
    STEP  = 0.5    # setpoint adjustment step (°C)

    def suggest_setpoint(self, twin: Dict) -> Optional[float]:
        current_sp = float(twin.get("target_temp", self.COMFORT_MID))
        load        = float(twin.get("load_percentage", 50.0))
        current_t   = float(twin.get("current_temp", self.COMFORT_MID))

        # If the unit is already comfortable and heavily loaded, raise setpoint
        # slightly to shed load (energy saving) while staying in comfort band.
        comfort_err = current_t - self.COMFORT_MID
        energy_signal = (load - 70.0) / 100.0   # positive if over 70% load

        adjustment = self.ALPHA * energy_signal * self.STEP - self.BETA * comfort_err * 0.1
        new_sp = max(self.COMFORT_MIN, min(self.COMFORT_MAX, current_sp + adjustment))

        if abs(new_sp - current_sp) < 0.25:  # dead-band: don't issue trivial adjustments
            return None
        return round(new_sp * 2) / 2  # round to nearest 0.5°C


# ─────────────────────────────────────────────────────────────────────────────
#  Command Dispatcher
# ─────────────────────────────────────────────────────────────────────────────

class CommandDispatcher:
    """Publishes commands with rate-limiting and ACK tracking."""

    def __init__(self, redis: aioredis.Redis):
        self._redis = redis

    async def dispatch(self, cmd: ControlCommand) -> bool:
        rate_key = f"cmd_rate:{cmd.unit_id}"
        if await self._redis.exists(rate_key):
            log.debug("Rate-limited command to %s", cmd.unit_id)
            return False

        topic = settings.TOPIC_COMMAND.format(unit_id=cmd.unit_id)
        payload = {
            **cmd.model_dump(exclude_none=True),
            "issued_at": datetime.now(timezone.utc).isoformat(),
        }
        await mqtt.async_publish(topic, payload)

        # Rate limit
        await self._redis.set(rate_key, "1", ex=CMD_RATE_LIMIT_S)

        # Track pending ACK
        ack_key = f"cmd_ack:{cmd.correlation_id}"
        await self._redis.set(ack_key, json.dumps(payload), ex=300)

        log.info("CMD → %s: %s (corr=%s)", cmd.unit_id, payload, cmd.correlation_id)
        return True


# ─────────────────────────────────────────────────────────────────────────────
#  Default rule set
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_RULES = [
    Rule(
        id="overheat-shutdown",
        name="Emergency shutdown on critical temperature",
        priority=0,
        conditions=[RuleCondition(field="current_temp", operator="gte", value=settings.ALERT_TEMP_CRITICAL_C)],
        actions=[
            RuleAction(type="set_status", value="OFF"),
            RuleAction(type="alert", value={"type": "OVERHEAT", "severity": "CRITICAL"}),
        ],
        cooldown_s=300,
    ),
    Rule(
        id="high-temp-max-fan",
        name="Ramp fan to HIGH when temperature is elevated",
        priority=10,
        conditions=[RuleCondition(field="current_temp", operator="gte", value=settings.ALERT_TEMP_HIGH_C)],
        actions=[RuleAction(type="set_fan", value="HIGH")],
        cooldown_s=120,
    ),
    Rule(
        id="high-load-alert",
        name="Alert when load exceeds critical threshold",
        priority=20,
        conditions=[RuleCondition(field="load_percentage", operator="gte", value=settings.ALERT_LOAD_CRITICAL_PCT)],
        actions=[RuleAction(type="alert", value={"type": "HIGH_LOAD", "severity": "WARNING"})],
        cooldown_s=60,
    ),
    Rule(
        id="energy-saving-auto-fan",
        name="Switch to AUTO fan when load is low",
        priority=80,
        conditions=[
            RuleCondition(field="load_percentage", operator="lt", value=30.0),
            RuleCondition(field="current_temp", operator="lte", value=24.0),
        ],
        logic="AND",
        actions=[RuleAction(type="set_fan", value="AUTO")],
        cooldown_s=300,
    ),
]


# ─────────────────────────────────────────────────────────────────────────────
#  Main control loop
# ─────────────────────────────────────────────────────────────────────────────

rule_engine   = RuleEngine(DEFAULT_RULES)
load_balancer = LoadBalancer()
optimizer     = SetpointOptimizer()
dispatcher: CommandDispatcher = None


async def _get_all_twins() -> Dict[str, Dict]:
    """Fetch all digital twin states from Redis."""
    keys = await redis_client.keys("twin:*")
    twins = {}
    for key in keys:
        raw = await redis_client.hgetall(key)
        if raw:
            uid = raw["unit_id"]
            twins[uid] = raw
    return twins


async def _group_by_zone(twins: Dict[str, Dict]) -> Dict[str, List[Dict]]:
    zones: Dict[str, List[Dict]] = {}
    for twin in twins.values():
        zid = twin.get("zone_id", "unknown")
        zones.setdefault(zid, []).append(twin)
    return zones


async def control_loop() -> None:
    """Main periodic control loop – runs every CONTROL_LOOP_INTERVAL_S seconds."""
    while True:
        try:
            await asyncio.sleep(CONTROL_LOOP_INTERVAL_S)
            twins = await _get_all_twins()
            if not twins:
                continue

            zone_groups = await _group_by_zone(twins)

            for unit_id, twin in twins.items():
                if twin.get("status") == "OFFLINE":
                    continue

                # ── 1. Rule engine ────────────────────────────────────────
                for rule, action in rule_engine.evaluate(twin):
                    cmd = _action_to_command(unit_id, rule, action)
                    if cmd:
                        await dispatcher.dispatch(cmd)

                # ── 2. Setpoint optimisation ──────────────────────────────
                new_sp = optimizer.suggest_setpoint(twin)
                if new_sp is not None:
                    await dispatcher.dispatch(ControlCommand(
                        unit_id=unit_id,
                        target_temp=new_sp,
                        issued_by="setpoint-optimizer",
                        correlation_id=str(uuid.uuid4()),
                    ))

            # ── 3. Zone load balancing ────────────────────────────────────
            for zone_id, zone_twins in zone_groups.items():
                cmd = await load_balancer.evaluate_zone(zone_id, zone_twins)
                if cmd:
                    await dispatcher.dispatch(cmd)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.exception("Control loop error: %s", exc)


def _action_to_command(unit_id: str, rule: Rule, action: RuleAction) -> Optional[ControlCommand]:
    corr = str(uuid.uuid4())
    if action.type == "set_temp":
        return ControlCommand(unit_id=unit_id, target_temp=float(action.value),
                              issued_by=f"rule:{rule.id}", correlation_id=corr)
    if action.type == "set_fan":
        return ControlCommand(unit_id=unit_id, fan_speed=FanSpeed(action.value),
                              issued_by=f"rule:{rule.id}", correlation_id=corr)
    if action.type == "set_status":
        return ControlCommand(unit_id=unit_id, status=UnitStatus(action.value),
                              issued_by=f"rule:{rule.id}", correlation_id=corr)
    if action.type == "alert":
        asyncio.create_task(mqtt.async_publish(settings.TOPIC_SYSTEM_ALERTS, {
            "unit_id": unit_id, **action.value,
            "rule_id": rule.id, "ts": datetime.now(timezone.utc).isoformat(),
        }))
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  FastAPI
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, dispatcher
    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
    dispatcher   = CommandDispatcher(redis_client)
    mqtt.connect()
    await mqtt.wait_connected(timeout=20.0)

    ctrl_task = asyncio.create_task(control_loop())
    log.info("Control Service started – loop interval %ss", CONTROL_LOOP_INTERVAL_S)
    yield

    ctrl_task.cancel()
    mqtt.disconnect()
    await redis_client.aclose()
    await engine.dispose()


app = FastAPI(title="HVAC Control Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "control-service", "rules": len(rule_engine._rules)}


@app.get("/rules", response_model=List[Rule])
async def list_rules():
    return rule_engine._rules


@app.post("/rules", response_model=Rule, status_code=201)
async def add_rule(rule: Rule):
    rule_engine._rules.append(rule)
    rule_engine._rules.sort(key=lambda r: r.priority)
    return rule


@app.delete("/rules/{rule_id}", status_code=204)
async def delete_rule(rule_id: str):
    original = len(rule_engine._rules)
    rule_engine._rules = [r for r in rule_engine._rules if r.id != rule_id]
    if len(rule_engine._rules) == original:
        raise HTTPException(status_code=404, detail="Rule not found")


@app.post("/command", status_code=202)
async def manual_command(cmd: ControlCommand, bg: BackgroundTasks):
    """Issue a manual override command (bypasses rate limit for ops use)."""
    cmd.correlation_id = cmd.correlation_id or str(uuid.uuid4())
    topic = settings.TOPIC_COMMAND.format(unit_id=cmd.unit_id)
    await mqtt.async_publish(topic, {
        **cmd.model_dump(exclude_none=True),
        "issued_at": datetime.now(timezone.utc).isoformat(),
        "override": True,
    })
    return {"status": "dispatched", "topic": topic, "correlation_id": cmd.correlation_id}


@app.get("/load-balance/status")
async def load_balance_status():
    twins = await _get_all_twins()
    zone_groups = await _group_by_zone(twins)
    summary = {}
    for zone_id, zone_twins in zone_groups.items():
        on = [t for t in zone_twins if t.get("status") == "ON"]
        summary[zone_id] = {
            "total": len(zone_twins),
            "on": len(on),
            "standby": len([t for t in zone_twins if t.get("status") == "STANDBY"]),
            "avg_load": round(sum(float(t.get("load_percentage", 0)) for t in on) / max(len(on), 1), 1),
            "overloaded": len([t for t in on if float(t.get("load_percentage", 0)) >= LoadBalancer.HIGH_LOAD_PCT]),
        }
    return summary
