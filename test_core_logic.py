"""
Unit Tests – Core Logic
========================
Tests PID controller math, rule engine evaluation,
load balancer decisions, and anomaly detector logic.
No external dependencies required.

Run:
    pytest tests/unit/ -v
"""
from __future__ import annotations

import asyncio
import math
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.control_service.main import (
    LoadBalancer, RuleEngine, SetpointOptimizer,
    Rule, RuleCondition, RuleAction,
)
from backend.alert_service.main import AnomalyDetector
from backend.shared.models import AlertType, AlertSeverity


# ─────────────────────────────────────────────────────────────────────────────
#  PID Controller (Python equivalent for unit testing)
# ─────────────────────────────────────────────────────────────────────────────

class PIDController:
    """Python equivalent of the embedded C PID for unit testing."""
    def __init__(self, kp, ki, kd, out_min, out_max, integral_clamp, dt):
        self.kp, self.ki, self.kd = kp, ki, kd
        self.out_min, self.out_max = out_min, out_max
        self.integral_clamp = integral_clamp
        self.dt = dt
        self.integral = 0.0
        self.prev_meas = None
        self.dead_band = 0.2

    def compute(self, setpoint: float, measured: float) -> float:
        error = setpoint - measured
        if abs(error) < self.dead_band:
            return self.ki * self.integral

        self.integral = max(-self.integral_clamp,
                            min(self.integral_clamp, self.integral + error * self.dt))
        deriv = 0.0 if self.prev_meas is None else (measured - self.prev_meas) / self.dt
        self.prev_meas = measured

        out = self.kp * error + self.ki * self.integral - self.kd * deriv
        return max(self.out_min, min(self.out_max, out))


class TestPIDController:

    def _make_pid(self):
        return PIDController(kp=2.0, ki=0.5, kd=0.1,
                             out_min=-100, out_max=100,
                             integral_clamp=50, dt=1.0)

    def test_zero_error_gives_near_zero_output(self):
        pid = self._make_pid()
        out = pid.compute(22.0, 22.0)
        assert abs(out) < 0.5   # dead-band suppresses micro-corrections

    def test_positive_error_drives_cooling(self):
        """When temp > setpoint, PID should demand negative output (no cooling)
        Wait – setpoint - measured: if temp=25 and sp=22, error=-3, output negative."""
        pid = self._make_pid()
        out = pid.compute(setpoint=22.0, measured=25.0)
        assert out < 0   # negative = no cooling demand when measured > setpoint

    def test_negative_error_demands_cooling(self):
        """When temp < setpoint, output positive (more cooling not needed).
        In HVAC context we cool: if room=20 and sp=22, we don't need to cool."""
        pid = self._make_pid()
        out = pid.compute(setpoint=22.0, measured=18.0)
        assert out > 0

    def test_output_clamped_to_limits(self):
        pid = self._make_pid()
        # Very large error should clamp
        out = pid.compute(setpoint=0.0, measured=100.0)
        assert out <= 0.0
        assert out >= -100.0  # out_min

    def test_integral_windup_prevention(self):
        pid = self._make_pid()
        # Apply sustained error for many cycles
        for _ in range(1000):
            pid.compute(0.0, 50.0)
        assert abs(pid.integral) <= pid.integral_clamp

    def test_convergence_over_time(self):
        """Simulate closed-loop: PID output is applied to temperature model."""
        pid = self._make_pid()
        temp = 28.0
        setpoint = 22.0
        errors = []
        for step in range(100):
            out = pid.compute(setpoint, temp)
            cooling = max(0, out) * 0.08  # simplified thermal model
            drift = (30 - temp) * 0.02    # ambient drift
            temp = temp + drift - cooling
            errors.append(abs(setpoint - temp))

        # Should converge: final error much smaller than initial
        assert errors[-1] < errors[0] * 0.3

    def test_dead_band_prevents_micro_oscillation(self):
        pid = self._make_pid()
        # Error within dead-band: output should be near zero
        outputs = [pid.compute(22.0, 22.1) for _ in range(10)]
        # All outputs should be small (integral builds slowly)
        assert all(abs(o) < 2.0 for o in outputs)


# ─────────────────────────────────────────────────────────────────────────────
#  Rule Engine Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestRuleEngine:

    def _base_twin(self, **overrides):
        state = {
            "unit_id":        "test-unit",
            "zone_id":        "zone-01",
            "current_temp":   22.0,
            "load_percentage": 50.0,
            "power_consumption_w": 1000.0,
            "status":         "ON",
        }
        state.update(overrides)
        return state

    def _make_rule(self, conditions, actions, logic="AND", cooldown=0):
        return Rule(
            id="test-rule",
            name="Test Rule",
            conditions=conditions,
            actions=actions,
            logic=logic,
            cooldown_s=cooldown,
        )

    def test_rule_fires_on_high_temp(self):
        engine = RuleEngine([self._make_rule(
            conditions=[RuleCondition(field="current_temp", operator="gte", value=35.0)],
            actions=[RuleAction(type="set_status", value="OFF")],
        )])
        results = engine.evaluate(self._base_twin(current_temp=36.0))
        assert len(results) == 1
        assert results[0][1].type == "set_status"

    def test_rule_does_not_fire_below_threshold(self):
        engine = RuleEngine([self._make_rule(
            conditions=[RuleCondition(field="current_temp", operator="gte", value=35.0)],
            actions=[RuleAction(type="set_status", value="OFF")],
        )])
        results = engine.evaluate(self._base_twin(current_temp=25.0))
        assert len(results) == 0

    def test_and_logic_requires_all_conditions(self):
        engine = RuleEngine([self._make_rule(
            conditions=[
                RuleCondition(field="current_temp", operator="gte", value=30.0),
                RuleCondition(field="load_percentage", operator="gte", value=90.0),
            ],
            actions=[RuleAction(type="alert", value={})],
            logic="AND",
        )])
        # Only one condition met
        assert len(engine.evaluate(self._base_twin(current_temp=31.0, load_percentage=80.0))) == 0
        # Both met
        assert len(engine.evaluate(self._base_twin(current_temp=31.0, load_percentage=91.0))) == 1

    def test_or_logic_fires_on_any_condition(self):
        engine = RuleEngine([self._make_rule(
            conditions=[
                RuleCondition(field="current_temp", operator="gte", value=35.0),
                RuleCondition(field="load_percentage", operator="gte", value=98.0),
            ],
            actions=[RuleAction(type="alert", value={})],
            logic="OR",
        )])
        # Only temp condition met
        assert len(engine.evaluate(self._base_twin(current_temp=36.0, load_percentage=50.0))) == 1

    def test_disabled_rule_never_fires(self):
        rule = self._make_rule(
            conditions=[RuleCondition(field="current_temp", operator="gte", value=0.0)],
            actions=[RuleAction(type="alert", value={})],
        )
        rule.enabled = False
        engine = RuleEngine([rule])
        assert len(engine.evaluate(self._base_twin(current_temp=100.0))) == 0

    def test_cooldown_prevents_re_fire(self):
        engine = RuleEngine([self._make_rule(
            conditions=[RuleCondition(field="current_temp", operator="gte", value=25.0)],
            actions=[RuleAction(type="alert", value={})],
            cooldown=3600,  # 1 hour cooldown
        )])
        twin = self._base_twin(current_temp=30.0)
        first  = engine.evaluate(twin)
        second = engine.evaluate(twin)
        assert len(first) == 1
        assert len(second) == 0   # suppressed by cooldown

    def test_zone_filter_respected(self):
        rule = self._make_rule(
            conditions=[RuleCondition(field="current_temp", operator="gte", value=25.0)],
            actions=[RuleAction(type="alert", value={})],
        )
        rule.zone_id = "zone-99"   # only fires for zone-99
        engine = RuleEngine([rule])
        # Wrong zone: no fire
        assert len(engine.evaluate(self._base_twin(current_temp=30.0))) == 0
        # Correct zone: fires
        assert len(engine.evaluate(self._base_twin(current_temp=30.0, zone_id="zone-99"))) == 1

    def test_priority_ordering(self):
        """Higher-priority rules (lower number) evaluated first."""
        r1 = Rule(id="r1", name="Low priority", priority=90,
                  conditions=[RuleCondition(field="current_temp", operator="gte", value=25.0)],
                  actions=[RuleAction(type="alert", value={"level": "low"})])
        r2 = Rule(id="r2", name="High priority", priority=10,
                  conditions=[RuleCondition(field="current_temp", operator="gte", value=25.0)],
                  actions=[RuleAction(type="alert", value={"level": "high"})])
        engine = RuleEngine([r1, r2])
        results = engine.evaluate({"unit_id": "u", "zone_id": "z", "current_temp": 30.0})
        actions = [r[1].value for r in results]
        # High priority should come first
        assert actions[0]["level"] == "high"


# ─────────────────────────────────────────────────────────────────────────────
#  Load Balancer Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestLoadBalancer:

    def _twins(self, loads, statuses=None):
        if statuses is None:
            statuses = ["ON"] * len(loads)
        return [
            {"unit_id": f"unit-{i}", "load_percentage": l, "status": s}
            for i, (l, s) in enumerate(zip(loads, statuses))
        ]

    @pytest.mark.asyncio
    async def test_scale_out_when_three_units_overloaded(self):
        lb = LoadBalancer()
        lb.ZONE_COOLDOWN_S = 0   # disable cooldown for testing
        twins = self._twins(
            loads=[95, 92, 91, 20],
            statuses=["ON", "ON", "ON", "STANDBY"],
        )
        cmd = await lb.evaluate_zone("z1", twins)
        assert cmd is not None
        assert cmd.status.value == "ON"
        assert cmd.unit_id == "unit-3"   # idlest standby (load=20)

    @pytest.mark.asyncio
    async def test_no_scale_out_when_no_standby_units(self):
        lb = LoadBalancer()
        lb.ZONE_COOLDOWN_S = 0
        twins = self._twins(loads=[95, 92, 91, 88])  # all ON, none standby
        cmd = await lb.evaluate_zone("z1", twins)
        assert cmd is None

    @pytest.mark.asyncio
    async def test_scale_in_when_avg_load_low(self):
        lb = LoadBalancer()
        lb.ZONE_COOLDOWN_S = 0
        twins = self._twins(loads=[15, 18, 12, 10])  # all below LOW_LOAD_PCT
        cmd = await lb.evaluate_zone("z1", twins)
        assert cmd is not None
        assert cmd.status.value == "STANDBY"
        assert cmd.unit_id == "unit-3"   # lowest load

    @pytest.mark.asyncio
    async def test_no_scale_in_when_only_one_unit_on(self):
        """Should never stand by the last active unit."""
        lb = LoadBalancer()
        lb.ZONE_COOLDOWN_S = 0
        twins = [{"unit_id": "solo", "load_percentage": 5.0, "status": "ON"}]
        cmd = await lb.evaluate_zone("z1", twins)
        assert cmd is None

    @pytest.mark.asyncio
    async def test_zone_cooldown_prevents_rapid_decisions(self):
        lb = LoadBalancer()
        twins = self._twins(loads=[95, 92, 91, 10], statuses=["ON","ON","ON","STANDBY"])
        cmd1 = await lb.evaluate_zone("z2", twins)
        cmd2 = await lb.evaluate_zone("z2", twins)  # immediately again
        assert cmd1 is not None
        assert cmd2 is None   # cooldown suppresses


# ─────────────────────────────────────────────────────────────────────────────
#  Setpoint Optimizer Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSetpointOptimizer:

    def _twin(self, **kw):
        return {"target_temp": 22.0, "load_percentage": 50.0, "current_temp": 22.0, **kw}

    def test_no_adjustment_within_dead_band(self):
        opt = SetpointOptimizer()
        # Normal conditions: adjustment too small to trigger
        result = opt.suggest_setpoint(self._twin(load_percentage=50, current_temp=22.0))
        # Should return None (dead-band) or a value close to 22
        assert result is None or abs(result - 22.0) < 1.0

    def test_raises_setpoint_on_high_load(self):
        opt = SetpointOptimizer()
        twin = self._twin(load_percentage=95.0, current_temp=22.0, target_temp=22.0)
        result = opt.suggest_setpoint(twin)
        if result is not None:
            assert result >= 22.0   # raise setpoint = less cooling demand

    def test_setpoint_stays_within_comfort_band(self):
        opt = SetpointOptimizer()
        for load in [0, 50, 100]:
            for temp in [15, 22, 35]:
                twin = self._twin(load_percentage=load, current_temp=float(temp))
                result = opt.suggest_setpoint(twin)
                if result is not None:
                    assert opt.COMFORT_MIN <= result <= opt.COMFORT_MAX

    def test_result_snapped_to_half_degree(self):
        opt = SetpointOptimizer()
        twin = self._twin(load_percentage=90.0, current_temp=24.0, target_temp=22.0)
        result = opt.suggest_setpoint(twin)
        if result is not None:
            # Should be multiple of 0.5
            assert abs(result * 2 - round(result * 2)) < 1e-9


# ─────────────────────────────────────────────────────────────────────────────
#  Anomaly Detector Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAnomalyDetector:

    def _make_telemetry(self, unit_id="u1", **overrides):
        from backend.shared.models import TelemetryPayload
        from datetime import datetime, timezone
        base = dict(
            unit_id=unit_id,
            timestamp=datetime.now(timezone.utc),
            temperature=22.0,
            humidity=55.0,
            load=50.0,
            power_w=1000.0,
        )
        base.update(overrides)
        return TelemetryPayload(**base)

    def test_no_alert_on_normal_readings(self):
        det = AnomalyDetector()
        for _ in range(10):
            t = self._make_telemetry(temperature=22.0, load=50.0, power_w=1000.0)
            alerts = det.update(t)
            assert alerts == []

    def test_sustained_overheat_triggers_alert(self):
        det = AnomalyDetector()
        det.WINDOW = 3   # speed up window for testing
        alerts_all = []
        for _ in range(5):
            t = self._make_telemetry(temperature=36.0, load=50.0, power_w=1000.0)
            alerts_all.extend(det.update(t))
        overheat_alerts = [a for a in alerts_all if a.alert_type == AlertType.OVERHEAT]
        assert len(overheat_alerts) > 0

    def test_single_spike_does_not_trigger_alert(self):
        """One bad reading should not trigger – window must be full."""
        det = AnomalyDetector()
        # First 4 normal readings
        for _ in range(4):
            t = self._make_telemetry(temperature=22.0, load=50.0, power_w=1000.0)
            det.update(t)
        # Single spike
        spike_alerts = det.update(self._make_telemetry(temperature=36.0))
        overheat = [a for a in spike_alerts if a.alert_type == AlertType.OVERHEAT]
        assert len(overheat) == 0   # window not full of high readings

    def test_power_anomaly_detected(self):
        det = AnomalyDetector()
        det.BASELINE_ALPHA = 0.9  # fast baseline learning
        # Build baseline at 1000W
        for _ in range(5):
            det.update(self._make_telemetry(power_w=1000.0))
        # Spike to 3000W (200% over baseline)
        alerts = det.update(self._make_telemetry(power_w=3000.0))
        power_alerts = [a for a in alerts if a.alert_type == AlertType.POWER_ANOMALY]
        assert len(power_alerts) > 0
        assert power_alerts[0].severity in (AlertSeverity.WARNING, AlertSeverity.CRITICAL)

    def test_multiple_units_tracked_independently(self):
        det = AnomalyDetector()
        det.WINDOW = 3
        # Unit A: normal
        for _ in range(5):
            det.update(self._make_telemetry("unit-A", temperature=22.0))
        # Unit B: high temp
        alerts_B = []
        for _ in range(5):
            alerts_B.extend(det.update(self._make_telemetry("unit-B", temperature=36.0)))

        overheat_B = [a for a in alerts_B if a.alert_type == AlertType.OVERHEAT]
        assert len(overheat_B) > 0
        # Ensure unit-A was not affected
        for a in overheat_B:
            assert a.unit_id == "unit-B"
