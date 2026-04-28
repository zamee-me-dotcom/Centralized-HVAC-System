# HVAC Centralized Control System

**Production-grade, event-driven, fault-tolerant HVAC control platform**  
Manages 100–1000+ AC units with edge autonomy, digital twins, and real-time optimization.

---

## Architecture

```
                     ┌──────────────────────────────────────────────┐
                     │           CENTRAL CONTROL SYSTEM             │
                     │                                              │
                     │  device-service    → Unit registration/CRUD  │
                     │  telemetry-service → InfluxDB time-series    │
                     │  control-service   → Rules + load balancing  │
                     │  digital-twin-svc  → Real-time state mirror  │
                     │  alert-service     → Anomaly detection/ACK   │
                     └──────────────────┬───────────────────────────┘
                                        │ MQTT (TLS 8883)
                     ┌──────────────────▼───────────────────────────┐
                     │           EMQX MQTT BROKER                   │
                     │   QoS 1, TLS mTLS, ACL-based authorization   │
                     └──────────┬──────────────────┬────────────────┘
                                │                  │
              ┌─────────────────▼──┐   ┌───────────▼──────────────┐
              │   Edge Gateway 1   │   │   Edge Gateway N         │
              │  ─────────────     │   │  ─────────────           │
              │  Protocol adapter  │   │  Protocol adapter        │
              │  Local rules       │   │  Local rules             │
              │  SQLite buffer     │   │  SQLite buffer           │
              │  Load balancer     │   │  Load balancer           │
              └────────┬───────────┘   └──────────┬───────────────┘
                       │                          │
         ┌─────────────▼──────────┐  ┌────────────▼──────────────┐
         │  AC Unit Cluster       │  │  AC Unit Cluster          │
         │  FreeRTOS / ESP32      │  │  FreeRTOS / ESP32         │
         │  PID Controller        │  │  PID Controller           │
         │  Sensor stack          │  │  Sensor stack             │
         │  Local autonomy        │  │  Local autonomy           │
         └────────────────────────┘  └───────────────────────────┘
```

### Data Flow

```
AC Unit → Gateway → EMQX → telemetry-service → InfluxDB
                                             → digital-twin (Redis)
                                             → alert-service

control-service (10s loop) → Redis (twin state) → EMQX → Gateway → AC Unit
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend language | Python 3.12 + FastAPI + asyncio |
| Messaging | EMQX 5 (MQTT v5, TLS mTLS) |
| Time-series DB | InfluxDB 2.7 (30-day retention) |
| Relational DB | PostgreSQL 16 + asyncpg |
| Cache / Twin state | Redis 7.2 (allkeys-lru) |
| Edge gateway | Python 3.12 async + paho-mqtt |
| Embedded firmware | C99 + FreeRTOS (ESP32 / STM32) |
| Container runtime | Docker + Docker Compose |
| Orchestration | Kubernetes + NGINX Ingress |
| Observability | Prometheus + Grafana |
| Auth | TLS mTLS (MQTT) + JWT (REST API) |

---

## MQTT Topic Map

| Topic | Direction | QoS | Purpose |
|-------|-----------|-----|---------|
| `ac/unit/{id}/telemetry` | Edge → Cloud | 1 | Raw sensor data (2s interval) |
| `ac/unit/{id}/telemetry/processed` | Internal | 0 | Enriched telemetry for twins/alerts |
| `ac/unit/{id}/command` | Cloud → Edge | 1 | Control commands (setpoint, fan, on/off) |
| `ac/unit/{id}/heartbeat` | Edge → Cloud | 1 | Liveness ping (5s interval) |
| `ac/unit/{id}/response` | Edge → Cloud | 1 | Command acknowledgement |
| `ac/zone/{zone_id}/state` | Cloud pub | 1 retain | Zone aggregate state |
| `ac/system/alerts` | All → Cloud | 1 | Alert fan-out bus |

---

## Project Structure

```
hvac-control-system/
├── backend/
│   ├── shared/
│   │   ├── models.py           # Pydantic + SQLAlchemy models
│   │   ├── config.py           # Pydantic-settings (env-driven)
│   │   └── mqtt_client.py      # Async MQTT client wrapper
│   ├── device_service/
│   │   └── main.py             # Unit/zone registration & CRUD API
│   ├── telemetry_service/
│   │   └── main.py             # MQTT subscriber → InfluxDB + Redis
│   ├── control_service/
│   │   └── main.py             # Rule engine + load balancer + optimizer
│   ├── digital_twin_service/
│   │   └── main.py             # Real-time twin state + WebSocket stream
│   └── alert_service/
│       └── main.py             # Anomaly detection + webhook dispatch
│
├── edge_gateway/
│   ├── gateway.py              # Full edge gateway implementation
│   ├── gateway_config.yaml     # Per-gateway configuration
│   └── rules/
│       └── rules.json          # Local rule engine rules
│
├── embedded/
│   └── ac_unit_controller.c   # FreeRTOS firmware (PID + MQTT + watchdog)
│
├── docker/
│   ├── docker-compose.yml      # Full local stack
│   ├── backend.Dockerfile      # Multi-stage production image
│   ├── edge.Dockerfile         # Edge gateway image
│   ├── postgres/init.sql       # DB init
│   └── prometheus/prometheus.yml
│
├── k8s/
│   └── base/
│       ├── namespace-config.yaml   # Namespace, ConfigMap, Secrets
│       └── deployments.yaml        # All services + Ingress + HPA + RBAC
│
├── requirements.txt
└── README.md
```

---

## Quick Start (Local Docker)

### Prerequisites
- Docker 24+ and Docker Compose v2
- OpenSSL (for TLS cert generation)

### 1. Generate TLS certificates

```bash
cd docker/certs
# CA
openssl req -new -x509 -days 3650 -keyout ca.key -out ca.crt \
  -subj "/CN=HVAC-CA" -nodes

# Broker cert
openssl req -new -keyout broker.key -out broker.csr -subj "/CN=emqx" -nodes
openssl x509 -req -days 3650 -in broker.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out broker.crt

# Backend client cert
openssl req -new -keyout backend.key -out backend.csr -subj "/CN=backend" -nodes
openssl x509 -req -days 3650 -in backend.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out backend.crt
```

### 2. Configure environment

```bash
cp docker/.env.backend.example docker/.env.backend
# Edit secrets as needed
```

### 3. Start the stack

```bash
cd docker
docker compose up -d

# Watch logs
docker compose logs -f telemetry-service control-service
```

### 4. Register a zone and units

```bash
# Create zone
curl -X POST http://localhost:8001/zones \
  -H "Content-Type: application/json" \
  -d '{"id":"zone-01","name":"Floor 1 North","target_temp":22.0}'

# Register AC units
for i in 001 002 003; do
  curl -X POST http://localhost:8001/units \
    -H "Content-Type: application/json" \
    -d "{\"id\":\"ac-unit-$i\",\"zone_id\":\"zone-01\",\"name\":\"Unit $i\",\"capacity_kw\":5.0}"
done
```

### 5. Issue a control command

```bash
curl -X POST http://localhost:8001/units/ac-unit-001/command \
  -H "Content-Type: application/json" \
  -d '{"unit_id":"ac-unit-001","target_temp":21.5,"fan_speed":"HIGH","issued_by":"operator"}'
```

### 6. View digital twin state

```bash
# All twins
curl http://localhost:8004/twins | python3 -m json.tool

# WebSocket real-time stream
websocat ws://localhost:8004/ws/stream?zone_id=zone-01
```

---

## Kubernetes Deployment

```bash
# Apply base manifests
kubectl apply -k k8s/base/

# Wait for rollout
kubectl rollout status deployment/telemetry-service -n hvac-system

# Check pod health
kubectl get pods -n hvac-system

# Port-forward Grafana for dashboard access
kubectl port-forward -n hvac-system svc/grafana 3000:3000
```

---

## Key Design Decisions

### 1. Edge Autonomy (Fault Tolerance)
Every AC unit runs an independent FreeRTOS PID control loop. If the MQTT
broker is unreachable, units enter `LOCAL_AUTONOMY` mode using the last known
setpoint. The edge gateway also evaluates local rules independently. Telemetry
is buffered in SQLite and replayed atomically when connectivity resumes.

### 2. Digital Twin Architecture
Twin state is maintained in two layers:
- **Redis** (millisecond access): flat hash per unit, TTL-managed
- **In-process dict** (zero-copy reads): seeded from Redis at startup

This lets the REST API serve twin reads at <1ms without DB round-trips.

### 3. PID Controller (Embedded)
Uses the **derivative-on-measurement** variant to eliminate setpoint-change
derivative kicks. Anti-windup via integral clamping. A **3-minute compressor
minimum off-time** guard prevents short-cycling damage.

### 4. Alert Deduplication
Alerts are deduplicated in Redis for 5 minutes (configurable). The same alert
type from the same unit will not generate duplicate notifications within the
window, preventing notification storms during sustained failures.

### 5. Load Balancer Hysteresis
Zone load balancing uses a **120-second per-zone cooldown** to prevent
oscillation ("hunting") between scale-out and scale-in decisions.

---

## Observability Endpoints

| Service | Port | URL |
|---------|------|-----|
| Device Service API | 8001 | http://localhost:8001/docs |
| Telemetry Service API | 8002 | http://localhost:8002/docs |
| Control Service API | 8003 | http://localhost:8003/docs |
| Digital Twin API | 8004 | http://localhost:8004/docs |
| Alert Service API | 8005 | http://localhost:8005/docs |
| EMQX Dashboard | 18083 | http://localhost:18083 (admin/public) |
| InfluxDB UI | 8086 | http://localhost:8086 |
| Grafana | 3000 | http://localhost:3000 (admin/changeme) |
| Prometheus | 9090 | http://localhost:9090 |

---

## Security Checklist

- [x] TLS 1.3 for all MQTT connections (mTLS between gateway and broker)
- [x] Per-device X.509 certificates (fingerprint stored in DB)
- [x] JWT authentication for REST APIs
- [x] Non-root containers (UID 1000)
- [x] Kubernetes NetworkPolicy (default deny + explicit allow)
- [x] Kubernetes RBAC (service account with minimal permissions)
- [x] Redis password protection
- [x] Secrets via Kubernetes Secret (migrate to Vault in prod)
- [ ] EMQX ACL rules (template provided in docker/emqx/acl.conf)
- [ ] Rate limiting on REST API (add nginx rate-limit annotations)
- [ ] Audit logging (add middleware to log all commands)

---

## Performance Targets

| Metric | Target | Mechanism |
|--------|--------|-----------|
| Telemetry ingestion | 1000 units × 0.5 Hz = 500 msg/s | 3 telemetry replicas + async InfluxDB |
| Command latency | ≤ 500ms end-to-end | MQTT QoS 1 + direct publish |
| Twin read latency | ≤ 1ms | In-process dict + Redis hash |
| Control loop period | 10 seconds | Configurable per deployment |
| Heartbeat timeout | 30 seconds | Redis TTL-based detection |
| Offline buffer | 256 messages / unit | SQLite WAL ring buffer |
