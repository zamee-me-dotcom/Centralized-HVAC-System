"""
Async-compatible MQTT client wrapper built on paho-mqtt.

Features
--------
* TLS mutual authentication
* Automatic reconnection with exponential back-off
* Per-topic handler registry
* Message serialisation (JSON/MessagePack)
* Publisher with QoS and retain support
* Prometheus metrics integration
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, Optional

import paho.mqtt.client as mqtt
from prometheus_client import Counter, Gauge

from .config import Settings

log = logging.getLogger(__name__)

# ── Prometheus metrics ────────────────────────────────────────────────────────
MQTT_MESSAGES_RX = Counter("mqtt_messages_received_total", "MQTT messages received", ["topic_prefix"])
MQTT_MESSAGES_TX = Counter("mqtt_messages_published_total", "MQTT messages published", ["topic_prefix"])
MQTT_CONNECTED = Gauge("mqtt_connected", "MQTT broker connection status (1=connected)")
MQTT_RECONNECTS = Counter("mqtt_reconnect_total", "MQTT reconnection attempts")

TopicHandler = Callable[[str, bytes], Awaitable[None]]


@dataclass
class MQTTConfig:
    host: str
    port: int
    username: str
    password: str
    client_id: str
    ca_cert: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    keepalive: int = 60
    qos: int = 1
    reconnect_delay_min: float = 1.0
    reconnect_delay_max: float = 60.0


class AsyncMQTTClient:
    """Thread-safe MQTT client that bridges paho callbacks into asyncio."""

    def __init__(self, config: MQTTConfig, loop: Optional[asyncio.AbstractEventLoop] = None):
        self._cfg = config
        self._loop: asyncio.AbstractEventLoop = loop or asyncio.get_event_loop()
        self._handlers: Dict[str, TopicHandler] = {}
        self._connected = asyncio.Event()
        self._reconnect_delay = config.reconnect_delay_min

        self._client = mqtt.Client(
            client_id=config.client_id,
            protocol=mqtt.MQTTv5,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        self._client.username_pw_set(config.username, config.password)

        if config.ca_cert:
            ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=config.ca_cert)
            if config.client_cert and config.client_key:
                ctx.load_cert_chain(certfile=config.client_cert, keyfile=config.client_key)
            ctx.check_hostname = True
            ctx.verify_mode = ssl.CERT_REQUIRED
            self._client.tls_set_context(ctx)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Start the MQTT network loop in a background thread."""
        self._client.connect_async(self._cfg.host, self._cfg.port, keepalive=self._cfg.keepalive)
        self._client.loop_start()
        log.info("MQTT connecting to %s:%s", self._cfg.host, self._cfg.port)

    def disconnect(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()
        MQTT_CONNECTED.set(0)
        log.info("MQTT disconnected cleanly")

    async def wait_connected(self, timeout: float = 30.0) -> None:
        await asyncio.wait_for(self._connected.wait(), timeout=timeout)

    # ── Publish ───────────────────────────────────────────────────────────────

    def publish(
        self,
        topic: str,
        payload: dict | str | bytes,
        qos: Optional[int] = None,
        retain: bool = False,
    ) -> mqtt.MQTTMessageInfo:
        if isinstance(payload, dict):
            payload = json.dumps(payload, default=str)
        if isinstance(payload, str):
            payload = payload.encode()

        info = self._client.publish(
            topic,
            payload,
            qos=qos if qos is not None else self._cfg.qos,
            retain=retain,
        )
        MQTT_MESSAGES_TX.labels(topic_prefix=topic.split("/")[0]).inc()
        return info

    async def async_publish(self, topic: str, payload: dict | str | bytes, **kwargs) -> None:
        await self._loop.run_in_executor(None, lambda: self.publish(topic, payload, **kwargs))

    # ── Subscribe ─────────────────────────────────────────────────────────────

    def subscribe(self, topic: str, handler: TopicHandler, qos: Optional[int] = None) -> None:
        self._handlers[topic] = handler
        self._client.subscribe(topic, qos=qos if qos is not None else self._cfg.qos)
        log.info("MQTT subscribed to %s", topic)

    # ── Internal callbacks ────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            log.info("MQTT connected (rc=0)")
            MQTT_CONNECTED.set(1)
            self._reconnect_delay = self._cfg.reconnect_delay_min
            # Re-subscribe after reconnect
            for topic in self._handlers:
                client.subscribe(topic, qos=self._cfg.qos)
            self._loop.call_soon_threadsafe(self._connected.set)
        else:
            log.error("MQTT connection refused: reason_code=%s", reason_code)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        MQTT_CONNECTED.set(0)
        self._loop.call_soon_threadsafe(self._connected.clear)
        if reason_code != 0:
            MQTT_RECONNECTS.inc()
            log.warning("MQTT unexpected disconnect rc=%s; retry in %.1fs", reason_code, self._reconnect_delay)
            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, self._cfg.reconnect_delay_max)
            client.reconnect()

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        MQTT_MESSAGES_RX.labels(topic_prefix=msg.topic.split("/")[0]).inc()

        # Find the best matching handler (exact > wildcard)
        handler = self._handlers.get(msg.topic)
        if not handler:
            for pattern, h in self._handlers.items():
                if mqtt.topic_matches_sub(pattern, msg.topic):
                    handler = h
                    break

        if handler:
            asyncio.run_coroutine_threadsafe(
                handler(msg.topic, msg.payload), self._loop
            )
        else:
            log.debug("No handler for topic: %s", msg.topic)


def build_mqtt_client(settings: Settings, client_id: str) -> AsyncMQTTClient:
    cfg = MQTTConfig(
        host=settings.MQTT_HOST,
        port=settings.MQTT_PORT,
        username=settings.MQTT_USERNAME,
        password=settings.MQTT_PASSWORD,
        client_id=client_id,
        ca_cert=settings.MQTT_CA_CERT,
        client_cert=settings.MQTT_CLIENT_CERT,
        client_key=settings.MQTT_CLIENT_KEY,
        keepalive=settings.MQTT_KEEPALIVE,
        qos=settings.MQTT_QOS,
    )
    return AsyncMQTTClient(cfg)
