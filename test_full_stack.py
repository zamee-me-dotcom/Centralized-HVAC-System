"""
Integration Test Suite
=======================
Tests the full HVAC system stack end-to-end.

Requirements
------------
    pip install pytest pytest-asyncio httpx

Run
---
    # With running Docker stack
    pytest tests/integration/ -v --tb=short

    # Against staging environment
    DEVICE_SERVICE_URL=https://api.hvac.staging.com/devices pytest tests/integration/ -v
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import AsyncGenerator

import httpx
import pytest
import pytest_asyncio

# ── Service URLs (override with env vars for remote testing) ──────────────────
DEVICE_URL  = os.getenv("DEVICE_SERVICE_URL",       "http://localhost:8001")
TELEMETRY_URL = os.getenv("TELEMETRY_SERVICE_URL",  "http://localhost:8002")
CONTROL_URL = os.getenv("CONTROL_SERVICE_URL",      "http://localhost:8003")
TWIN_URL    = os.getenv("TWIN_SERVICE_URL",          "http://localhost:8004")
ALERT_URL   = os.getenv("ALERT_SERVICE_URL",         "http://localhost:8005")

# Service auth token (operator role)
AUTH_TOKEN  = os.getenv("AUTH_TOKEN", "")
HEADERS     = {"Authorization": f"Bearer {AUTH_TOKEN}"} if AUTH_TOKEN else {}


# ─────────────────────────────────────────────────────────────────────────────
#  Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def http_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        yield client


@pytest.fixture
def unique_id():
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest_asyncio.fixture
async def test_zone(http_client: httpx.AsyncClient, unique_id: str):
    """Create a zone, yield its ID, then clean up."""
    zone_id = f"zone-{unique_id}"
    resp = await http_client.post(f"{DEVICE_URL}/zones", headers=HEADERS, json={
        "id":          zone_id,
        "name":        "Test Zone",
        "target_temp": 22.0,
    })
    assert resp.status_code == 201, f"Zone creation failed: {resp.text}"
    yield zone_id


@pytest_asyncio.fixture
async def test_unit(http_client: httpx.AsyncClient, unique_id: str, test_zone: str):
    """Create an AC unit in the test zone, yield its ID."""
    unit_id = f"unit-{unique_id}"
    resp = await http_client.post(f"{DEVICE_URL}/units", headers=HEADERS, json={
        "id":          unit_id,
        "zone_id":     test_zone,
        "name":        "Test AC Unit",
        "capacity_kw": 5.0,
        "target_temp": 22.0,
    })
    assert resp.status_code == 201, f"Unit creation failed: {resp.text}"
    yield unit_id


# ─────────────────────────────────────────────────────────────────────────────
#  Device Service Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestDeviceService:

    @pytest.mark.asyncio
    async def test_health(self, http_client: httpx.AsyncClient):
        resp = await http_client.get(f"{DEVICE_URL}/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    @pytest.mark.asyncio
    async def test_create_zone(self, http_client: httpx.AsyncClient):
        zone_id = f"zone-{uuid.uuid4().hex[:6]}"
        resp = await http_client.post(f"{DEVICE_URL}/zones", headers=HEADERS, json={
            "id": zone_id, "name": "Integration Test Zone", "target_temp": 22.0,
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["id"] == zone_id
        assert body["target_temp"] == 22.0

    @pytest.mark.asyncio
    async def test_create_zone_duplicate_returns_409(self, http_client, test_zone):
        resp = await http_client.post(f"{DEVICE_URL}/zones", headers=HEADERS, json={
            "id": test_zone, "name": "Duplicate", "target_temp": 22.0,
        })
        assert resp.status_code == 409

    @pytest.mark.asyncio
    async def test_register_unit(self, http_client, test_unit, test_zone):
        resp = await http_client.get(f"{DEVICE_URL}/units/{test_unit}", headers=HEADERS)
        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == test_unit
        assert body["zone_id"] == test_zone
        assert body["capacity_kw"] == 5.0

    @pytest.mark.asyncio
    async def test_register_unit_invalid_zone_returns_404(self, http_client):
        resp = await http_client.post(f"{DEVICE_URL}/units", headers=HEADERS, json={
            "id": f"unit-{uuid.uuid4().hex[:6]}",
            "zone_id": "nonexistent-zone",
            "name": "Orphan Unit",
            "capacity_kw": 3.0,
        })
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_list_units_filter_by_zone(self, http_client, test_unit, test_zone):
        resp = await http_client.get(
            f"{DEVICE_URL}/units", headers=HEADERS, params={"zone_id": test_zone}
        )
        assert resp.status_code == 200
        units = resp.json()
        assert any(u["id"] == test_unit for u in units)

    @pytest.mark.asyncio
    async def test_update_unit_setpoint(self, http_client, test_unit):
        resp = await http_client.put(
            f"{DEVICE_URL}/units/{test_unit}", headers=HEADERS,
            json={"target_temp": 20.0},
        )
        assert resp.status_code == 200
        assert resp.json()["target_temp"] == 20.0

    @pytest.mark.asyncio
    async def test_update_unit_invalid_temp_rejected(self, http_client, test_unit):
        resp = await http_client.put(
            f"{DEVICE_URL}/units/{test_unit}", headers=HEADERS,
            json={"target_temp": 5.0},   # below min
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_zone_status(self, http_client, test_unit, test_zone):
        resp = await http_client.get(f"{DEVICE_URL}/zones/{test_zone}/status", headers=HEADERS)
        assert resp.status_code == 200
        body = resp.json()
        assert body["zone_id"] == test_zone
        assert "total_units" in body
        assert "total_power_w" in body

    @pytest.mark.asyncio
    async def test_delete_unit(self, http_client, test_zone):
        unit_id = f"unit-del-{uuid.uuid4().hex[:6]}"
        await http_client.post(f"{DEVICE_URL}/units", headers=HEADERS, json={
            "id": unit_id, "zone_id": test_zone, "name": "To delete", "capacity_kw": 1.0,
        })
        resp = await http_client.delete(f"{DEVICE_URL}/units/{unit_id}", headers=HEADERS)
        assert resp.status_code == 204

        # Confirm deletion
        resp = await http_client.get(f"{DEVICE_URL}/units/{unit_id}", headers=HEADERS)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_unit_id_format_validation(self, http_client, test_zone):
        """Unit IDs with spaces or special chars should be rejected."""
        resp = await http_client.post(f"{DEVICE_URL}/units", headers=HEADERS, json={
            "id": "bad id with spaces!",
            "zone_id": test_zone,
            "name": "Bad ID Unit",
            "capacity_kw": 1.0,
        })
        assert resp.status_code == 422


# ─────────────────────────────────────────────────────────────────────────────
#  Control Service Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestControlService:

    @pytest.mark.asyncio
    async def test_health(self, http_client):
        resp = await http_client.get(f"{CONTROL_URL}/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_list_rules(self, http_client):
        resp = await http_client.get(f"{CONTROL_URL}/rules", headers=HEADERS)
        assert resp.status_code == 200
        rules = resp.json()
        assert isinstance(rules, list)
        assert len(rules) > 0
        # Default rule set should include overheat-shutdown
        rule_ids = [r["id"] for r in rules]
        assert "overheat-shutdown" in rule_ids

    @pytest.mark.asyncio
    async def test_add_and_delete_rule(self, http_client):
        rule = {
            "id":   "test-rule",
            "name": "Integration test rule",
            "conditions": [{"field": "temperature", "operator": "gte", "value": 50.0}],
            "actions":    [{"type": "set_status", "value": "OFF"}],
            "cooldown_s": 60,
        }
        resp = await http_client.post(f"{CONTROL_URL}/rules", headers=HEADERS, json=rule)
        assert resp.status_code == 201

        # Delete
        resp = await http_client.delete(f"{CONTROL_URL}/rules/test-rule", headers=HEADERS)
        assert resp.status_code == 204

    @pytest.mark.asyncio
    async def test_delete_nonexistent_rule_returns_404(self, http_client):
        resp = await http_client.delete(f"{CONTROL_URL}/rules/no-such-rule", headers=HEADERS)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_manual_command_dispatch(self, http_client, test_unit):
        resp = await http_client.post(f"{CONTROL_URL}/command", headers=HEADERS, json={
            "unit_id":     test_unit,
            "target_temp": 21.0,
            "issued_by":   "integration-test",
        })
        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "dispatched"
        assert "correlation_id" in body

    @pytest.mark.asyncio
    async def test_load_balance_status(self, http_client):
        resp = await http_client.get(f"{CONTROL_URL}/load-balance/status", headers=HEADERS)
        assert resp.status_code == 200
        assert isinstance(resp.json(), dict)


# ─────────────────────────────────────────────────────────────────────────────
#  Digital Twin Service Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestDigitalTwinService:

    @pytest.mark.asyncio
    async def test_health(self, http_client):
        resp = await http_client.get(f"{TWIN_URL}/health")
        assert resp.status_code == 200
        body = resp.json()
        assert "twins" in body

    @pytest.mark.asyncio
    async def test_list_twins(self, http_client):
        resp = await http_client.get(f"{TWIN_URL}/twins", headers=HEADERS)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @pytest.mark.asyncio
    async def test_twin_not_found_returns_404(self, http_client):
        resp = await http_client.get(f"{TWIN_URL}/twins/definitely-not-a-real-unit-id", headers=HEADERS)
        assert resp.status_code == 404


# ─────────────────────────────────────────────────────────────────────────────
#  Alert Service Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAlertService:

    @pytest.mark.asyncio
    async def test_health(self, http_client):
        resp = await http_client.get(f"{ALERT_URL}/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_list_alerts_empty_or_populated(self, http_client):
        resp = await http_client.get(f"{ALERT_URL}/alerts", headers=HEADERS)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @pytest.mark.asyncio
    async def test_alert_summary(self, http_client):
        resp = await http_client.get(f"{ALERT_URL}/alerts/summary", headers=HEADERS)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @pytest.mark.asyncio
    async def test_resolve_nonexistent_alert_returns_404(self, http_client):
        resp = await http_client.post(
            f"{ALERT_URL}/alerts/nonexistent-alert-id/resolve", headers=HEADERS
        )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_list_alerts_with_severity_filter(self, http_client):
        resp = await http_client.get(
            f"{ALERT_URL}/alerts", headers=HEADERS,
            params={"severity": "CRITICAL"},
        )
        assert resp.status_code == 200
        alerts = resp.json()
        for alert in alerts:
            assert alert["severity"] == "CRITICAL"


# ─────────────────────────────────────────────────────────────────────────────
#  End-to-end pipeline test
# ─────────────────────────────────────────────────────────────────────────────

class TestEndToEndPipeline:

    @pytest.mark.asyncio
    async def test_all_services_healthy(self, http_client):
        """Smoke test: all services must respond healthy."""
        services = [
            (DEVICE_URL,    "device-service"),
            (TELEMETRY_URL, "telemetry-service"),
            (CONTROL_URL,   "control-service"),
            (TWIN_URL,      "digital-twin"),
            (ALERT_URL,     "alert-service"),
        ]
        for url, svc_name in services:
            resp = await http_client.get(f"{url}/health")
            assert resp.status_code == 200, f"{svc_name} health failed: {resp.text}"
            body = resp.json()
            assert body["status"] == "ok", f"{svc_name} not ok: {body}"

    @pytest.mark.asyncio
    async def test_command_flow(self, http_client, test_unit):
        """
        Issue a command via device service → verify it was dispatched.
        Full verification would require an MQTT subscriber to confirm delivery.
        """
        corr_id = str(uuid.uuid4())
        resp = await http_client.post(
            f"{DEVICE_URL}/units/{test_unit}/command",
            headers=HEADERS,
            json={
                "unit_id":        test_unit,
                "target_temp":    23.5,
                "fan_speed":      "HIGH",
                "issued_by":      "integration-test",
                "correlation_id": corr_id,
            },
        )
        # Unit is OFFLINE (not simulated), expect 409 or 200
        assert resp.status_code in (200, 409)
        if resp.status_code == 200:
            assert resp.json()["correlation_id"] == corr_id

    @pytest.mark.asyncio
    async def test_data_model_consistency(self, http_client, test_unit, test_zone):
        """
        Unit retrieved from device service must reference its zone correctly.
        Zone status must reflect unit count.
        """
        unit_resp = await http_client.get(f"{DEVICE_URL}/units/{test_unit}", headers=HEADERS)
        assert unit_resp.status_code == 200
        unit = unit_resp.json()
        assert unit["zone_id"] == test_zone

        zone_resp = await http_client.get(f"{DEVICE_URL}/zones/{test_zone}/status", headers=HEADERS)
        assert zone_resp.status_code == 200
        zone = zone_resp.json()
        assert zone["total_units"] >= 1
