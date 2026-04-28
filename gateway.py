"""
Edge Gateway
============
Runs at the building/floor level. Bridges physical AC units to the MQTT broker
and provides autonomous operation when the upstream connection is unavailable.

Architecture
------------

  AC Units (Modbus/BACnet/Serial)
          │
  ┌───────▼──────────────────────────────────────┐
  │  Protocol Adapters (Modbus / BACnet / Mock)  │
  │  – Poll unit registers at configurable rates  │
  │  – Translate to TelemetryPayload              │
  └───────┬──────────────────────────────────────┘
          │
  ┌───────▼──────────────────────────────────────┐
  │  Local Rule Engine                           │
  │  – JSON-configurable rules (same schema as   │
  │    cloud control service)                    │
  │  – Acts autonomously when broker is offline  │
  └───────┬──────────────────────────────────────┘
          │
  ┌───────▼──────────────────────────────────────┐
  │  Offline Message Buffer (SQLite WAL)         │
  │  – Persists telemetry when broker unreachable │
  │  – Replays in order when connection resumes  │
  └───────┬──────────────────────────────────────┘
          │
  ┌───────▼──────────────────────────────────────┐
  │  MQTT Publisher / Subscriber                 │
  │  – TLS, auto-reconnect, QoS 1                │
  │  – Subscribes to ac/unit/{id}/command        │
  └──────────────────────────────────────────────┘
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
import ssl

import paho.mqtt.client as mqtt
import yaml
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("edge-gateway")


# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class UnitConfig:
    unit_id: str
    zone_id: str
    protocol: str          # modbus | bacnet | mock
    address: str           # IP or serial port
    modbus_slave_id: int = 1
    poll_interval_s: float = 2.0
    setpoint: float = 22.0
    enabled: bool = True


@dataclass
class GatewayConfig:
    gateway_id: str
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str
    mqtt_password: str
    mqtt_ca_cert: Optional[str]
    mqtt_client_cert: Optional[str]
    mqtt_client_key: Optional[str]
    units: List[UnitConfig]
    rules_file: str = "rules.json"
    buffer_db: str = "/var/edge/buffer.db"
    heartbeat_interval_s: float = 5.0
    broker_reconnect_delay_max_s: float = 60.0


def load_config(path: str) -> GatewayConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    units = [UnitConfig(**u) for u in raw.pop("units", [])]
    return GatewayConfig(units=units, **raw)


# ─────────────────────────────────────────────────────────────────────────────
#  Offline Message Buffer (SQLite WAL)
# ─────────────────────────────────────────────────────────────────────────────

class OfflineBuffer:
    """
    Durable FIFO buffer backed by SQLite (WAL mode for concurrent access).
    Survives process crashes. Replays oldest-first on broker reconnect.
    """
    MAX_ROWS = 100_000   # cap to prevent runaway disk usage

    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS buffer (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                topic     TEXT NOT NULL,
                payload   TEXT NOT NULL,
                qos       INTEGER DEFAULT 1,
                retain    INTEGER DEFAULT 0,
                created   REAL NOT NULL
            )
        """)
        self._conn.commit()

    def push(self, topic: str, payload: dict | str, qos: int = 1, retain: bool = False) -> None:
        count = self._conn.execute("SELECT COUNT(*) FROM buffer").fetchone()[0]
        if count >= self.MAX_ROWS:
            # Drop oldest 10% to make room
            self._conn.execute(f"DELETE FROM buffer WHERE id IN (SELECT id FROM buffer ORDER BY id LIMIT {self.MAX_ROWS // 10})")
        if isinstance(payload, dict):
            payload = json.dumps(payload, default=str)
        self._conn.execute(
            "INSERT INTO buffer (topic, payload, qos, retain, created) VALUES (?,?,?,?,?)",
            (topic, payload, qos, int(retain), time.time()),
        )
        self._conn.commit()

    def pop_batch(self, n: int = 50) -> List[tuple]:
        rows = self._conn.execute(
            "SELECT id, topic, payload, qos, retain FROM buffer ORDER BY id LIMIT ?", (n,)
        ).fetchall()
        if rows:
            ids = [r[0] for r in rows]
            self._conn.execute(f"DELETE FROM buffer WHERE id IN ({','.join('?'*len(ids))})", ids)
            self._conn.commit()
        return rows

    def size(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM buffer").fetchone()[0]

    def close(self):
        self._conn.close()


# ─────────────────────────────────────────────────────────────────────────────
#  Protocol Adapters
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RawTelemetry:
    unit_id: str
    temperature: float
    humidity: float
    load: float
    power_w: float
    pressure: Optional[float] = None
    compressor_rpm: Optional[float] = None
    error_codes: List[int] = field(default_factory=list)


class MockAdapter:
    """Simulates an AC unit with realistic sensor physics."""
    def __init__(self, unit_id: str):
        self._uid = unit_id
        self._temp = random.uniform(20.0, 25.0)
        self._setpoint = 22.0
        self._load = random.uniform(30.0, 60.0)

    def poll(self) -> RawTelemetry:
        # Simple thermal model: temp drifts toward ambient, load affects it
        ambient = 28.0
        cooling = (self._temp - self._setpoint) * 0.1 * (self._load / 100.0)
        drift   = (ambient - self._temp) * 0.02
        self._temp = self._temp + drift - cooling + random.gauss(0, 0.05)
        self._temp = max(15.0, min(40.0, self._temp))
        self._load = max(0.0, min(100.0, self._load + random.gauss(0, 2)))
        power_w = self._load * 25 + random.gauss(0, 10)  # ~2.5kW at full load
        return RawTelemetry(
            unit_id=self._uid,
            temperature=round(self._temp, 2),
            humidity=round(random.uniform(40.0, 70.0), 1),
            load=round(self._load, 1),
            power_w=round(max(0.0, power_w), 1),
            pressure=round(random.uniform(1010.0, 1020.0), 1),
            compressor_rpm=round(self._load * 30 + 500, 0),
        )

    def set_setpoint(self, temp: float):
        self._setpoint = temp


class ModbusAdapter:
    """Real Modbus TCP/RTU adapter using pymodbus."""
    # Register map for a generic HVAC controller (example)
    REG_TEMP       = 0x0001
    REG_HUMIDITY   = 0x0002
    REG_LOAD       = 0x0003
    REG_POWER_W    = 0x0004
    REG_SETPOINT   = 0x0010
    REG_FAN_SPEED  = 0x0011
    REG_ON_OFF     = 0x0012
    SCALE_TEMP     = 0.1
    SCALE_POWER    = 1.0

    def __init__(self, unit_id: str, host: str, port: int = 502, slave_id: int = 1):
        from pymodbus.client import ModbusTcpClient
        self._uid = unit_id
        self._slave = slave_id
        self._client = ModbusTcpClient(host, port=port, timeout=3)
        self._connected = False

    def _ensure_connected(self) -> bool:
        if not self._connected:
            self._connected = self._client.connect()
        return self._connected

    def poll(self) -> Optional[RawTelemetry]:
        if not self._ensure_connected():
            log.error("Modbus not connected for %s", self._uid)
            return None
        try:
            rr = self._client.read_holding_registers(self.REG_TEMP, count=4, slave=self._slave)
            if rr.isError():
                return None
            regs = rr.registers
            return RawTelemetry(
                unit_id=self._uid,
                temperature=regs[0] * self.SCALE_TEMP,
                humidity=regs[1] * 0.1,
                load=regs[2] * 0.1,
                power_w=regs[3] * self.SCALE_POWER,
            )
        except Exception as exc:
            log.error("Modbus poll error %s: %s", self._uid, exc)
            self._connected = False
            return None

    def write_setpoint(self, temp: float):
        if self._ensure_connected():
            self._client.write_register(self.REG_SETPOINT, int(temp / self.SCALE_TEMP), slave=self._slave)

    def write_on_off(self, on: bool):
        if self._ensure_connected():
            self._client.write_register(self.REG_ON_OFF, int(on), slave=self._slave)


# ─────────────────────────────────────────────────────────────────────────────
#  Local Rule Engine
# ─────────────────────────────────────────────────────────────────────────────

class LocalRuleEngine:
    """
    Evaluates JSON-configurable rules locally.
    Identical schema to cloud control service – rules can be pushed from cloud.

    Example rule JSON:
    {
        "id": "local-overheat",
        "name": "Local emergency shutdown",
        "conditions": [{"field": "temperature", "operator": "gte", "value": 38}],
        "actions": [{"type": "set_on_off", "value": false}],
        "cooldown_s": 300
    }
    """

    OPERATORS = {
        "gt":  lambda a, b: a > b,
        "lt":  lambda a, b: a < b,
        "gte": lambda a, b: a >= b,
        "lte": lambda a, b: a <= b,
        "eq":  lambda a, b: abs(a - b) < 1e-6,
    }

    def __init__(self, rules_file: str):
        self._rules = []
        self._cooldowns: Dict[str, float] = {}
        self._load(rules_file)

    def _load(self, path: str):
        try:
            with open(path) as f:
                self._rules = json.load(f)
            log.info("Local rule engine loaded %d rules from %s", len(self._rules), path)
        except FileNotFoundError:
            log.warning("Rules file not found: %s – using empty ruleset", path)

    def reload(self, rules_file: str):
        self._load(rules_file)

    def evaluate(self, telemetry: RawTelemetry, adapter) -> None:
        now = time.time()
        state = asdict(telemetry)

        for rule in self._rules:
            if not rule.get("enabled", True):
                continue
            cooldown_key = f"{rule['id']}:{telemetry.unit_id}"
            if now - self._cooldowns.get(cooldown_key, 0) < rule.get("cooldown_s", 60):
                continue

            checks = []
            for cond in rule.get("conditions", []):
                val = state.get(cond["field"])
                if val is None:
                    checks.append(False)
                else:
                    op = self.OPERATORS.get(cond["operator"], lambda a, b: False)
                    checks.append(op(float(val), float(cond["value"])))

            logic = rule.get("logic", "AND")
            fired = all(checks) if logic == "AND" else any(checks)
            if not fired:
                continue

            self._cooldowns[cooldown_key] = now
            for action in rule.get("actions", []):
                self._apply(action, telemetry, adapter)

    def _apply(self, action: dict, t: RawTelemetry, adapter) -> None:
        atype = action.get("type")
        val   = action.get("value")
        log.info("[LocalRule] %s → %s(%s)", t.unit_id, atype, val)
        if atype == "set_on_off" and hasattr(adapter, "write_on_off"):
            adapter.write_on_off(bool(val))
        elif atype == "set_setpoint" and hasattr(adapter, "write_setpoint"):
            adapter.write_setpoint(float(val))
        elif atype == "set_setpoint" and hasattr(adapter, "set_setpoint"):
            adapter.set_setpoint(float(val))


# ─────────────────────────────────────────────────────────────────────────────
#  Local Load Balancer
# ─────────────────────────────────────────────────────────────────────────────

class LocalLoadBalancer:
    """
    Zone-level load balancing at the edge (works without cloud connectivity).
    If N units exceed HIGH_LOAD_PCT, activates the idlest unit.
    """

    HIGH_LOAD_PCT    = 90.0
    THRESHOLD_COUNT  = 3
    COOLDOWN_S       = 120.0

    def __init__(self):
        self._last_action: Dict[str, float] = {}

    def evaluate(self, telemetry_map: Dict[str, RawTelemetry], adapters: Dict[str, Any]) -> None:
        zone_groups: Dict[str, List[RawTelemetry]] = {}
        for t in telemetry_map.values():
            zone_groups.setdefault("default", []).append(t)

        now = time.time()
        for zone, readings in zone_groups.items():
            if now - self._last_action.get(zone, 0) < self.COOLDOWN_S:
                continue
            overloaded = [r for r in readings if r.load >= self.HIGH_LOAD_PCT]
            if len(overloaded) >= self.THRESHOLD_COUNT:
                idle = min(readings, key=lambda r: r.load)
                log.info("[LocalLB] Zone %s: %d units overloaded → activating %s",
                         zone, len(overloaded), idle.unit_id)
                adapter = adapters.get(idle.unit_id)
                if adapter and hasattr(adapter, "write_on_off"):
                    adapter.write_on_off(True)
                self._last_action[zone] = now


# ─────────────────────────────────────────────────────────────────────────────
#  MQTT Manager
# ─────────────────────────────────────────────────────────────────────────────

class MQTTManager:
    def __init__(self, cfg: GatewayConfig, buffer: OfflineBuffer,
                 command_handler: Callable[[str, dict], None]):
        self._cfg = cfg
        self._buffer = buffer
        self._on_command = command_handler
        self._connected = False
        self._reconnect_delay = 1.0

        client_id = f"{cfg.gateway_id}-{uuid.uuid4().hex[:6]}"
        self._client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv5,
                                   callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self._client.username_pw_set(cfg.mqtt_username, cfg.mqtt_password)

        if cfg.mqtt_ca_cert:
            ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cfg.mqtt_ca_cert)
            if cfg.mqtt_client_cert and cfg.mqtt_client_key:
                ctx.load_cert_chain(cfg.mqtt_client_cert, cfg.mqtt_client_key)
            self._client.tls_set_context(ctx)

        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message    = self._on_message

    def start(self):
        self._client.connect_async(self._cfg.mqtt_host, self._cfg.mqtt_port, keepalive=60)
        self._client.loop_start()

    def stop(self):
        self._client.loop_stop()
        self._client.disconnect()

    def publish(self, topic: str, payload: dict, qos: int = 1, retain: bool = False) -> bool:
        if isinstance(payload, dict):
            payload = json.dumps(payload, default=str).encode()
        if self._connected:
            self._client.publish(topic, payload, qos=qos, retain=retain)
            return True
        else:
            self._buffer.push(topic, payload.decode() if isinstance(payload, bytes) else payload, qos, retain)
            return False

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self._connected = True
            self._reconnect_delay = 1.0
            log.info("MQTT connected to %s:%s", self._cfg.mqtt_host, self._cfg.mqtt_port)
            # Subscribe to command topics for all configured units
            for unit in self._cfg.units:
                topic = f"ac/unit/{unit.unit_id}/command"
                client.subscribe(topic, qos=1)
                log.debug("Subscribed %s", topic)
            # Replay buffered messages
            asyncio.get_event_loop().call_soon_threadsafe(
                asyncio.ensure_future, self._replay_buffer()
            )
        else:
            log.error("MQTT connection failed: %s", reason_code)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        self._connected = False
        if reason_code != 0:
            log.warning("MQTT disconnected unexpectedly; retry in %.1fs", self._reconnect_delay)
            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, self._cfg.broker_reconnect_delay_max_s)

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        try:
            payload = json.loads(msg.payload)
            unit_id = msg.topic.split("/")[2]
            self._on_command(unit_id, payload)
        except Exception as exc:
            log.warning("Could not parse command: %s", exc)

    async def _replay_buffer(self):
        total = self._buffer.size()
        if total == 0:
            return
        log.info("Replaying %d buffered messages", total)
        replayed = 0
        while True:
            batch = self._buffer.pop_batch(50)
            if not batch:
                break
            for _, topic, payload, qos, retain in batch:
                self._client.publish(topic, payload.encode(), qos=qos, retain=bool(retain))
                replayed += 1
            await asyncio.sleep(0.1)
        log.info("Replay complete: %d messages", replayed)


# ─────────────────────────────────────────────────────────────────────────────
#  Edge Gateway (orchestrator)
# ─────────────────────────────────────────────────────────────────────────────

class EdgeGateway:
    def __init__(self, cfg: GatewayConfig):
        self._cfg       = cfg
        self._buffer    = OfflineBuffer(cfg.buffer_db)
        self._rule_eng  = LocalRuleEngine(cfg.rules_file)
        self._lb        = LocalLoadBalancer()
        self._adapters: Dict[str, Any] = {}
        self._latest:   Dict[str, RawTelemetry] = {}
        self._mqtt = MQTTManager(cfg, self._buffer, self._on_command)

        for unit in cfg.units:
            if unit.protocol == "modbus":
                host, port = unit.address.split(":")
                self._adapters[unit.unit_id] = ModbusAdapter(
                    unit.unit_id, host, int(port), unit.modbus_slave_id
                )
            else:
                self._adapters[unit.unit_id] = MockAdapter(unit.unit_id)

    async def run(self):
        self._mqtt.start()
        tasks = [
            asyncio.create_task(self._poll_loop()),
            asyncio.create_task(self._heartbeat_loop()),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
        finally:
            self._mqtt.stop()
            self._buffer.close()

    async def _poll_loop(self):
        while True:
            for unit in self._cfg.units:
                if not unit.enabled:
                    continue
                adapter = self._adapters[unit.unit_id]
                telemetry = adapter.poll()
                if telemetry is None:
                    continue

                self._latest[unit.unit_id] = telemetry

                # Local rules (always active)
                self._rule_eng.evaluate(telemetry, adapter)

                # Publish telemetry
                topic = f"ac/unit/{unit.unit_id}/telemetry"
                self._mqtt.publish(topic, {
                    "unit_id":        telemetry.unit_id,
                    "timestamp":      datetime.now(timezone.utc).isoformat(),
                    "temperature":    telemetry.temperature,
                    "humidity":       telemetry.humidity,
                    "load":           telemetry.load,
                    "power_w":        telemetry.power_w,
                    "pressure":       telemetry.pressure,
                    "compressor_rpm": telemetry.compressor_rpm,
                    "error_codes":    telemetry.error_codes,
                })
                await asyncio.sleep(0)   # yield

            # Local load balancing
            self._lb.evaluate(self._latest, self._adapters)

            # Wait for the smallest poll interval
            min_interval = min((u.poll_interval_s for u in self._cfg.units), default=2.0)
            await asyncio.sleep(min_interval)

    async def _heartbeat_loop(self):
        while True:
            for unit in self._cfg.units:
                topic = f"ac/unit/{unit.unit_id}/heartbeat"
                self._mqtt.publish(topic, {
                    "unit_id":     unit.unit_id,
                    "timestamp":   datetime.now(timezone.utc).isoformat(),
                    "uptime_s":    int(time.monotonic()),
                    "fw_version":  "edge-gw-1.0.0",
                    "local_mode":  not self._mqtt._connected,
                    "buffer_size": self._buffer.size(),
                })
            await asyncio.sleep(self._cfg.heartbeat_interval_s)

    def _on_command(self, unit_id: str, cmd: dict) -> None:
        adapter = self._adapters.get(unit_id)
        if not adapter:
            log.warning("Command for unknown unit: %s", unit_id)
            return

        log.info("Command received for %s: %s", unit_id, cmd)

        if "target_temp" in cmd and hasattr(adapter, "set_setpoint"):
            adapter.set_setpoint(float(cmd["target_temp"]))
        if "target_temp" in cmd and hasattr(adapter, "write_setpoint"):
            adapter.write_setpoint(float(cmd["target_temp"]))
        if "status" in cmd and hasattr(adapter, "write_on_off"):
            adapter.write_on_off(cmd["status"] == "ON")

        # ACK response
        topic = f"ac/unit/{unit_id}/response"
        self._mqtt.publish(topic, {
            "unit_id":        unit_id,
            "correlation_id": cmd.get("correlation_id"),
            "status":         "APPLIED",
            "ts":             datetime.now(timezone.utc).isoformat(),
        })


# ─────────────────────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    config_path = sys.argv[1] if len(sys.argv) > 1 else "gateway_config.yaml"
    cfg = load_config(config_path)
    gateway = EdgeGateway(cfg)

    log.info("Starting Edge Gateway %s → %s:%s", cfg.gateway_id, cfg.mqtt_host, cfg.mqtt_port)
    asyncio.run(gateway.run())
