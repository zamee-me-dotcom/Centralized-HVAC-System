"""
HVAC Fleet Simulator
=====================
Simulates 100–1000 AC units publishing realistic MQTT telemetry.
Invaluable for:
    • Integration testing without physical hardware
    • Load testing the backend (telemetry pipeline capacity)
    • Demonstrating the digital twin / alert system

Usage
-----
    # Simulate 100 units on localhost
    python -m scripts.simulate --units 100 --broker localhost --port 1883

    # Simulate 500 units with TLS, inject failures every 30s
    python -m scripts.simulate --units 500 --tls --fault-rate 0.02

Architecture
------------
Each simulated unit runs as an asyncio task:
    1. Initialises a thermal state (temp, load, mode)
    2. Publishes telemetry every TELEMETRY_INTERVAL_S
    3. Sends heartbeat every 5s
    4. Subscribes to its command topic and applies commands
    5. Runs a software PID to simulate realistic temperature dynamics
    6. Can enter FAULT mode (simulated sensor failure / overload)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s"
)
log = logging.getLogger("simulator")


# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SimConfig:
    num_units:          int   = 100
    broker_host:        str   = "localhost"
    broker_port:        int   = 1883
    username:           str   = "simulator"
    password:           str   = "changeme"
    use_tls:            bool  = False
    ca_cert:            Optional[str] = None
    telemetry_interval: float = 2.0       # seconds per telemetry publish
    heartbeat_interval: float = 5.0
    fault_rate:         float = 0.005     # probability per cycle of entering fault
    recovery_rate:      float = 0.05      # probability per cycle of recovering from fault
    zones:              int   = 10        # number of zones to distribute units across
    load_profile:       str   = "office"  # office | datacenter | residential


# ─────────────────────────────────────────────────────────────────────────────
#  Thermal physics model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ThermalState:
    unit_id:      str
    zone_id:      str
    temperature:  float
    humidity:     float
    setpoint:     float
    on:           bool
    fault:        bool = False

    # Internal PID state
    pid_integral: float = 0.0
    pid_prev_err: float = 0.0

    # Load model
    base_load:    float = field(default_factory=lambda: random.uniform(20, 50))
    power_base_w: float = field(default_factory=lambda: random.uniform(800, 2000))

    # Drift parameters (simulate unit-to-unit variability)
    ambient_temp: float = field(default_factory=lambda: random.uniform(26, 32))
    thermal_mass: float = field(default_factory=lambda: random.uniform(0.8, 1.5))
    cooling_coeff:float = field(default_factory=lambda: random.uniform(0.05, 0.15))

    def step(self, dt: float = 2.0) -> None:
        """Advance thermal simulation by dt seconds."""
        if self.fault:
            # In fault mode: temperature drifts toward ambient uncontrolled
            self.temperature += (self.ambient_temp - self.temperature) * 0.03 * dt
            self.temperature += random.gauss(0, 0.1)
            return

        if not self.on:
            self.temperature += (self.ambient_temp - self.temperature) * 0.02 * dt
            return

        # Proportional-only PID (simplified for simulation)
        error = self.setpoint - self.temperature
        self.pid_integral = max(-20, min(20, self.pid_integral + error * dt))
        derivative = (error - self.pid_prev_err) / dt
        pid_out = 1.5 * error + 0.3 * self.pid_integral + 0.1 * derivative
        self.pid_prev_err = error

        cooling = max(0, pid_out) * self.cooling_coeff * dt
        drift   = (self.ambient_temp - self.temperature) * 0.015 * dt
        noise   = random.gauss(0, 0.05)
        self.temperature = max(15, min(50,
            self.temperature + drift - cooling * self.thermal_mass + noise
        ))
        self.humidity = max(20, min(95,
            self.humidity + random.gauss(0, 0.3)
        ))

    def compute_load(self) -> float:
        """Compute load % from thermal state."""
        if self.fault:
            return random.uniform(95, 100)
        if not self.on:
            return 0.0
        temp_diff = max(0, self.temperature - self.setpoint)
        load = self.base_load + temp_diff * 8 + random.gauss(0, 3)
        return max(0, min(100, load))

    def compute_power(self, load: float) -> float:
        if not self.on:
            return random.uniform(0, 20)  # standby draw
        # Degrade efficiency in fault/high-load conditions
        efficiency = 1.0 if not self.fault else random.uniform(0.5, 0.8)
        return self.power_base_w * (load / 100) * efficiency + random.gauss(0, 30)


# ─────────────────────────────────────────────────────────────────────────────
#  MQTT per-unit task
# ─────────────────────────────────────────────────────────────────────────────

class UnitSimulator:
    def __init__(self, cfg: SimConfig, state: ThermalState):
        self._cfg   = cfg
        self._state = state
        self._client: Optional[mqtt.Client] = None
        self._connected = asyncio.Event()
        self._loop = asyncio.get_event_loop()

    def _build_client(self) -> mqtt.Client:
        uid = self._state.unit_id
        client = mqtt.Client(
            client_id=f"sim-{uid}",
            protocol=mqtt.MQTTv5,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        client.username_pw_set(self._cfg.username, self._cfg.password)
        if self._cfg.use_tls and self._cfg.ca_cert:
            client.tls_set(ca_certs=self._cfg.ca_cert)
        client.on_connect    = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message    = self._on_message
        return client

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self._loop.call_soon_threadsafe(self._connected.set)
            cmd_topic = f"ac/unit/{self._state.unit_id}/command"
            client.subscribe(cmd_topic, qos=1)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        self._loop.call_soon_threadsafe(self._connected.clear)

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        try:
            cmd = json.loads(msg.payload)
            if "target_temp" in cmd:
                self._state.setpoint = float(cmd["target_temp"])
            if "status" in cmd:
                self._state.on = (cmd["status"] == "ON")
                if cmd["status"] == "OFF":
                    self._state.fault = False

            # Publish ACK
            ack = {
                "unit_id": self._state.unit_id,
                "correlation_id": cmd.get("correlation_id"),
                "status": "APPLIED",
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            client.publish(
                f"ac/unit/{self._state.unit_id}/response",
                json.dumps(ack), qos=1
            )
        except Exception as exc:
            log.warning("[%s] Bad command: %s", self._state.unit_id, exc)

    async def run(self):
        self._client = self._build_client()
        self._client.connect_async(self._cfg.broker_host, self._cfg.broker_port, keepalive=60)
        self._client.loop_start()

        try:
            await asyncio.wait_for(self._connected.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            log.error("[%s] Connection timeout", self._state.unit_id)
            return

        telemetry_task = asyncio.create_task(self._telemetry_loop())
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        fault_task     = asyncio.create_task(self._fault_loop())

        await asyncio.gather(telemetry_task, heartbeat_task, fault_task,
                             return_exceptions=True)

    async def _telemetry_loop(self):
        while True:
            self._state.step(dt=self._cfg.telemetry_interval)
            load  = self._state.compute_load()
            power = self._state.compute_power(load)

            payload = {
                "unit_id":       self._state.unit_id,
                "timestamp":     datetime.now(timezone.utc).isoformat(),
                "temperature":   round(self._state.temperature, 2),
                "humidity":      round(self._state.humidity, 1),
                "pressure":      round(random.uniform(1010, 1020), 1),
                "load":          round(load, 1),
                "power_w":       round(max(0, power), 1),
                "compressor_rpm": round(load * 30 + 500, 0) if self._state.on else 0,
                "evap_temp":     round(self._state.setpoint - 5 + random.gauss(0, 0.5), 1),
                "condenser_temp": round(self._state.ambient_temp + 8 + random.gauss(0, 1), 1),
                "error_codes":   [42] if self._state.fault else [],
            }
            topic = f"ac/unit/{self._state.unit_id}/telemetry"
            self._client.publish(topic, json.dumps(payload), qos=0)  # QoS 0 for high-freq telemetry
            await asyncio.sleep(self._cfg.telemetry_interval)

    async def _heartbeat_loop(self):
        uptime_start = time.monotonic()
        while True:
            payload = {
                "unit_id":    self._state.unit_id,
                "timestamp":  datetime.now(timezone.utc).isoformat(),
                "uptime_s":   int(time.monotonic() - uptime_start),
                "fw_version": "sim-1.0.0",
                "local_mode": False,
                "fault":      self._state.fault,
            }
            topic = f"ac/unit/{self._state.unit_id}/heartbeat"
            self._client.publish(topic, json.dumps(payload), qos=1)
            await asyncio.sleep(self._cfg.heartbeat_interval)

    async def _fault_loop(self):
        """Randomly inject and recover from faults to exercise alert system."""
        while True:
            await asyncio.sleep(random.uniform(10, 30))
            if not self._state.fault and random.random() < self._cfg.fault_rate:
                log.info("[%s] 💥 Injecting FAULT", self._state.unit_id)
                self._state.fault = True
            elif self._state.fault and random.random() < self._cfg.recovery_rate:
                log.info("[%s] ✅ Recovering from fault", self._state.unit_id)
                self._state.fault = False


# ─────────────────────────────────────────────────────────────────────────────
#  Fleet bootstrapper
# ─────────────────────────────────────────────────────────────────────────────

def _build_unit_states(cfg: SimConfig) -> List[ThermalState]:
    states = []
    for i in range(cfg.num_units):
        zone_idx = i % cfg.zones
        unit_id  = f"ac-unit-{i+1:04d}"
        zone_id  = f"zone-{zone_idx+1:02d}"

        # Vary initial conditions by load profile
        if cfg.load_profile == "datacenter":
            initial_temp = random.uniform(18, 22)
            setpoint     = random.uniform(18, 20)
            base_load    = random.uniform(60, 80)
        elif cfg.load_profile == "residential":
            initial_temp = random.uniform(22, 28)
            setpoint     = random.uniform(22, 25)
            base_load    = random.uniform(10, 40)
        else:  # office
            initial_temp = random.uniform(20, 26)
            setpoint     = random.uniform(21, 23)
            base_load    = random.uniform(30, 60)

        state = ThermalState(
            unit_id=unit_id,
            zone_id=zone_id,
            temperature=initial_temp,
            humidity=random.uniform(40, 65),
            setpoint=setpoint,
            on=True,
            base_load=base_load,
        )
        states.append(state)
    return states


async def run_fleet(cfg: SimConfig) -> None:
    log.info("Starting HVAC fleet simulator")
    log.info("  Units: %d | Zones: %d | Broker: %s:%d | Profile: %s",
             cfg.num_units, cfg.zones, cfg.broker_host, cfg.broker_port, cfg.load_profile)

    states = _build_unit_states(cfg)
    simulators = [UnitSimulator(cfg, s) for s in states]

    # Stagger connection to avoid thundering herd
    BATCH_SIZE = 20
    STAGGER_MS  = 200

    tasks = []
    for i, sim in enumerate(simulators):
        if i % BATCH_SIZE == 0 and i > 0:
            await asyncio.sleep(STAGGER_MS / 1000)
        task = asyncio.create_task(sim.run(), name=f"unit-{states[i].unit_id}")
        tasks.append(task)

    log.info("All %d simulators launched", len(tasks))

    # Progress reporter
    async def report():
        while True:
            await asyncio.sleep(30)
            log.info("Fleet status: %d/%d units connected",
                     sum(1 for s in simulators if s._connected.is_set()), len(simulators))

    reporter = asyncio.create_task(report())

    try:
        await asyncio.gather(*tasks, reporter, return_exceptions=True)
    except asyncio.CancelledError:
        log.info("Simulator shutting down")
        for t in tasks:
            t.cancel()
        reporter.cancel()


# ─────────────────────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="HVAC Fleet Simulator")
    parser.add_argument("--units",        type=int,   default=100)
    parser.add_argument("--broker",       type=str,   default="localhost")
    parser.add_argument("--port",         type=int,   default=1883)
    parser.add_argument("--username",     type=str,   default="simulator")
    parser.add_argument("--password",     type=str,   default="changeme")
    parser.add_argument("--tls",          action="store_true")
    parser.add_argument("--ca-cert",      type=str,   default=None)
    parser.add_argument("--interval",     type=float, default=2.0)
    parser.add_argument("--zones",        type=int,   default=10)
    parser.add_argument("--fault-rate",   type=float, default=0.005)
    parser.add_argument("--profile",      type=str,   default="office",
                        choices=["office", "datacenter", "residential"])
    args = parser.parse_args()

    cfg = SimConfig(
        num_units=args.units,
        broker_host=args.broker,
        broker_port=args.port,
        username=args.username,
        password=args.password,
        use_tls=args.tls,
        ca_cert=args.ca_cert,
        telemetry_interval=args.interval,
        zones=args.zones,
        fault_rate=args.fault_rate,
        load_profile=args.profile,
    )
    asyncio.run(run_fleet(cfg))


if __name__ == "__main__":
    main()
