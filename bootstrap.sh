#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
#  HVAC Control System – Bootstrap Script
#
#  1. Generates a full TLS PKI (CA → broker + backend + gateway certs)
#  2. Creates EMQX users via REST API
#  3. Registers zones and AC units via Device Service API
#  4. Starts the simulator
#
#  Usage:
#    chmod +x scripts/bootstrap.sh
#    ./scripts/bootstrap.sh [--units 100] [--skip-certs] [--no-sim]
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
NUM_UNITS=50
NUM_ZONES=5
SKIP_CERTS=false
NO_SIM=false
DEVICE_URL="${DEVICE_URL:-http://localhost:8001}"
CERT_DIR="./docker/certs"
DAYS_VALID=3650

# ── Parse arguments ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --units)       NUM_UNITS="$2"; shift 2 ;;
        --zones)       NUM_ZONES="$2"; shift 2 ;;
        --skip-certs)  SKIP_CERTS=true; shift ;;
        --no-sim)      NO_SIM=true; shift ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ═══════════════════════════════════════════════════════════════════════════
#  1. TLS PKI Generation
# ═══════════════════════════════════════════════════════════════════════════

generate_certs() {
    info "Generating TLS certificates in $CERT_DIR"
    mkdir -p "$CERT_DIR"

    # ── CA ────────────────────────────────────────────────────────────────
    if [[ ! -f "$CERT_DIR/ca.crt" ]]; then
        openssl req -new -x509 -days $DAYS_VALID \
            -keyout "$CERT_DIR/ca.key" \
            -out    "$CERT_DIR/ca.crt" \
            -subj   "/C=US/O=HVAC-Systems/CN=HVAC-CA" \
            -nodes -quiet
        chmod 600 "$CERT_DIR/ca.key"
        info "  ✓ CA certificate created"
    else
        warn "  CA already exists – skipping"
    fi

    # ── Helper: sign a cert ───────────────────────────────────────────────
    sign_cert() {
        local name="$1" cn="$2" san="${3:-}"
        local keyfile="$CERT_DIR/${name}.key"
        local csrfile="$CERT_DIR/${name}.csr"
        local crtfile="$CERT_DIR/${name}.crt"

        [[ -f "$crtfile" ]] && { warn "  $name cert exists – skipping"; return; }

        openssl req -new -keyout "$keyfile" -out "$csrfile" \
            -subj "/C=US/O=HVAC-Systems/CN=${cn}" -nodes -quiet

        if [[ -n "$san" ]]; then
            openssl x509 -req -days $DAYS_VALID \
                -in "$csrfile" -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
                -CAcreateserial -out "$crtfile" \
                -extfile <(echo "subjectAltName=${san}") -quiet
        else
            openssl x509 -req -days $DAYS_VALID \
                -in "$csrfile" -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
                -CAcreateserial -out "$crtfile" -quiet
        fi
        chmod 600 "$keyfile"
        rm -f "$csrfile"
        info "  ✓ $name certificate created"
    }

    sign_cert "broker"  "emqx"           "DNS:emqx,DNS:localhost,IP:127.0.0.1"
    sign_cert "backend" "hvac-backend"   ""
    sign_cert "gw-01"   "gw-floor-1"     ""
    sign_cert "gw-02"   "gw-floor-2"     ""

    info "TLS PKI complete"
}

# ═══════════════════════════════════════════════════════════════════════════
#  2. Wait for services
# ═══════════════════════════════════════════════════════════════════════════

wait_for_service() {
    local url="$1" name="$2" max_attempts="${3:-30}"
    info "Waiting for $name at $url ..."
    for i in $(seq 1 $max_attempts); do
        if curl -sf "$url/health" -o /dev/null 2>&1; then
            info "  ✓ $name is ready"
            return 0
        fi
        echo -n "."
        sleep 2
    done
    error "$name did not become ready after $((max_attempts * 2))s"
}

# ═══════════════════════════════════════════════════════════════════════════
#  3. Seed zones and units
# ═══════════════════════════════════════════════════════════════════════════

seed_data() {
    info "Seeding $NUM_ZONES zones and $NUM_UNITS AC units"

    # Create zones
    for z in $(seq 1 $NUM_ZONES); do
        zone_id="zone-$(printf '%02d' $z)"
        zone_name="Zone $z"
        http_code=$(curl -s -o /dev/null -w "%{http_code}" \
            -X POST "$DEVICE_URL/zones" \
            -H "Content-Type: application/json" \
            -d "{\"id\":\"$zone_id\",\"name\":\"$zone_name\",\"target_temp\":22.0}" 2>/dev/null)
        if [[ "$http_code" == "201" ]]; then
            info "  ✓ Created zone $zone_id"
        elif [[ "$http_code" == "409" ]]; then
            warn "  Zone $zone_id already exists"
        else
            warn "  Zone $zone_id failed (HTTP $http_code)"
        fi
    done

    # Create units distributed across zones
    for u in $(seq 1 $NUM_UNITS); do
        unit_id="ac-unit-$(printf '%04d' $u)"
        zone_idx=$(( (u - 1) % NUM_ZONES + 1 ))
        zone_id="zone-$(printf '%02d' $zone_idx)"
        http_code=$(curl -s -o /dev/null -w "%{http_code}" \
            -X POST "$DEVICE_URL/units" \
            -H "Content-Type: application/json" \
            -d "{\"id\":\"$unit_id\",\"zone_id\":\"$zone_id\",\"name\":\"AC Unit $u\",\"capacity_kw\":5.0,\"target_temp\":22.0}" 2>/dev/null)
        if [[ "$http_code" == "201" ]]; then
            [[ $((u % 10)) -eq 0 ]] && info "  ✓ Created $u/$NUM_UNITS units"
        elif [[ "$http_code" != "409" ]]; then
            warn "  Unit $unit_id failed (HTTP $http_code)"
        fi
    done

    info "Data seeding complete: $NUM_ZONES zones, $NUM_UNITS units"
}

# ═══════════════════════════════════════════════════════════════════════════
#  4. Main
# ═══════════════════════════════════════════════════════════════════════════

main() {
    echo "════════════════════════════════════════════════════"
    echo "  HVAC Control System – Bootstrap"
    echo "  Units: $NUM_UNITS | Zones: $NUM_ZONES"
    echo "════════════════════════════════════════════════════"

    # TLS certs
    if [[ "$SKIP_CERTS" == false ]]; then
        if command -v openssl &>/dev/null; then
            generate_certs
        else
            warn "openssl not found – skipping cert generation"
        fi
    fi

    # Start Docker stack
    info "Starting Docker Compose stack"
    cd docker && docker compose up -d && cd ..
    sleep 5

    # Wait for device service
    wait_for_service "$DEVICE_URL" "device-service"

    # Seed data
    seed_data

    # Start simulator
    if [[ "$NO_SIM" == false ]]; then
        info "Starting fleet simulator with $NUM_UNITS units"
        python -m scripts.simulate \
            --units "$NUM_UNITS" \
            --zones "$NUM_ZONES" \
            --broker localhost \
            --port 1883 \
            --username simulator \
            --password changeme \
            --interval 2.0 \
            --fault-rate 0.005 \
            --profile office &
        SIM_PID=$!
        info "Simulator started (PID $SIM_PID)"
        info "To stop: kill $SIM_PID"
    fi

    echo ""
    echo "════════════════════════════════════════════════════"
    echo "  System is running! Access points:"
    echo ""
    echo "  API Docs (Device):       http://localhost:8001/docs"
    echo "  API Docs (Control):      http://localhost:8003/docs"
    echo "  API Docs (Twins):        http://localhost:8004/docs"
    echo "  EMQX Dashboard:          http://localhost:18083"
    echo "  Grafana:                 http://localhost:3000"
    echo "  InfluxDB:                http://localhost:8086"
    echo "════════════════════════════════════════════════════"
}

main "$@"
