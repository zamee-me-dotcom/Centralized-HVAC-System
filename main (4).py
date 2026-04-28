"""
Predictive Maintenance Service
================================
Analyses historical telemetry to predict:

1. Remaining Useful Life (RUL) – estimated days until maintenance needed
2. Failure Risk Score  – 0.0 (healthy) → 1.0 (imminent failure)
3. Anomaly Detection   – Isolation Forest on multivariate telemetry
4. Degradation Trends  – linear regression on power efficiency

Pipeline
--------

  InfluxDB (30-day telemetry)
          │
  ┌───────▼────────────────────┐
  │  Feature Engineering       │
  │  • Rolling stats (mean/std)│
  │  • Efficiency ratio        │
  │  • Cycle counting          │
  │  • Trend slope             │
  └───────┬────────────────────┘
          │
  ┌───────▼────────────────────┐
  │  Isolation Forest           │
  │  (unsupervised anomaly)    │
  └───────┬────────────────────┘
          │
  ┌───────▼────────────────────┐
  │  RUL Regression             │
  │  (Gradient Boosting)       │
  └───────┬────────────────────┘
          │
  ┌───────▼────────────────────┐
  │  Results → PostgreSQL      │
  │  Risk alerts → MQTT        │
  └────────────────────────────┘

Scheduling: runs every hour via APScheduler.
REST API: exposes predictions for dashboard consumption.
"""
from __future__ import annotations

import asyncio
import json
import logging
import warnings
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, HTTPException
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from pydantic import BaseModel
from sklearn.ensemble import GradientBoostingRegressor, IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from ..shared.config import get_settings
from ..shared.mqtt_client import build_mqtt_client

warnings.filterwarnings("ignore", category=FutureWarning)

log = logging.getLogger(__name__)
settings = get_settings()

engine = create_async_engine(settings.postgres_url, pool_size=5)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
mqtt = build_mqtt_client(settings, client_id="predictive-maintenance")
influx_client: InfluxDBClientAsync = None

# ── In-memory model registry ──────────────────────────────────────────────────
# Production: persist models to MLflow / S3
_anomaly_models: Dict[str, IsolationForest] = {}   # per-unit
_rul_model: Optional[GradientBoostingRegressor] = None
_global_anomaly_model: Optional[Pipeline] = None   # fleet-level


# ─────────────────────────────────────────────────────────────────────────────
#  Feature Engineering
# ─────────────────────────────────────────────────────────────────────────────

TELEMETRY_FEATURES = [
    "temperature", "humidity", "load", "power_w",
    "compressor_rpm", "evap_temp", "condenser_temp",
]


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw telemetry into ML-ready feature vectors.

    Features produced
    -----------------
    - rolling_mean_{f}_1h       : 1-hour rolling mean
    - rolling_std_{f}_1h        : 1-hour rolling std (sensor volatility)
    - efficiency_ratio           : load / power_w (kPa/W proxy for COP)
    - temp_delta                 : temperature - evap_temp (heat transfer)
    - load_power_corr            : rolling 1h Pearson corr(load, power_w)
    - compressor_start_rate      : compressor starts per hour (cycle stress)
    - trend_power_slope          : linear slope of power_w (degradation proxy)
    """
    df = df.copy().sort_values("timestamp").set_index("timestamp")

    for col in TELEMETRY_FEATURES:
        if col in df.columns:
            df[f"rolling_mean_{col}_1h"] = df[col].rolling("1H", min_periods=1).mean()
            df[f"rolling_std_{col}_1h"]  = df[col].rolling("1H", min_periods=1).std().fillna(0)

    # Efficiency: load per watt (higher = better efficiency)
    if "load" in df.columns and "power_w" in df.columns:
        df["efficiency_ratio"] = df["load"] / df["power_w"].replace(0, np.nan)
        df["efficiency_ratio"].fillna(0, inplace=True)

    # Heat transfer indicator
    if "temperature" in df.columns and "evap_temp" in df.columns:
        df["temp_delta"] = df["temperature"] - df["evap_temp"]

    # Rolling load-power correlation (coefficient of determination)
    if "load" in df.columns and "power_w" in df.columns:
        df["load_power_corr"] = (
            df["load"].rolling("1H", min_periods=5)
            .corr(df["power_w"])
            .fillna(1.0)
        )

    # Compressor cycle rate (approximate from load crossing zero)
    if "load" in df.columns:
        starts = ((df["load"] > 5) & (df["load"].shift(1) <= 5)).rolling("1H").sum()
        df["compressor_start_rate"] = starts.fillna(0)

    # Degradation slope: linear trend of power_w over 4h window
    if "power_w" in df.columns:
        def _slope(s: pd.Series) -> float:
            if len(s) < 2:
                return 0.0
            x = np.arange(len(s))
            return float(np.polyfit(x, s.values, 1)[0])

        df["trend_power_slope"] = (
            df["power_w"].rolling("4H", min_periods=2)
            .apply(_slope, raw=False)
            .fillna(0)
        )

    return df.reset_index()


def _extract_feature_vector(df: pd.DataFrame) -> Optional[np.ndarray]:
    """Extract the latest feature vector from a feature-engineered DataFrame."""
    feature_cols = [c for c in df.columns if c not in
                    ("timestamp", "unit_id", "error_codes")]
    if df.empty:
        return None
    last_row = df[feature_cols].iloc[-1].fillna(0)
    return last_row.values.astype(np.float32)


# ─────────────────────────────────────────────────────────────────────────────
#  InfluxDB data loader
# ─────────────────────────────────────────────────────────────────────────────

async def _load_telemetry(unit_id: str, days: int = 30) -> pd.DataFrame:
    """Fetch historical telemetry for a unit from InfluxDB as a DataFrame."""
    query = f"""
    from(bucket: "{settings.INFLUX_BUCKET}")
      |> range(start: -{days}d)
      |> filter(fn: (r) => r._measurement == "ac_telemetry")
      |> filter(fn: (r) => r.unit_id == "{unit_id}")
      |> filter(fn: (r) => r._field =~ /temperature|humidity|load|power_w|compressor_rpm|evap_temp|condenser_temp/)
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    """
    async with InfluxDBClientAsync(
        url=settings.INFLUX_URL,
        token=settings.INFLUX_TOKEN,
        org=settings.INFLUX_ORG,
    ) as client:
        query_api = client.query_api()
        tables = await query_api.query(query)

    records = []
    for table in tables:
        for record in table.records:
            row = {"timestamp": record.get_time(), "unit_id": unit_id}
            row.update({k: v for k, v in record.values.items()
                        if k not in ("result", "table", "_start", "_stop", "_measurement")})
            records.append(row)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    numeric_cols = [c for c in df.columns if c not in ("timestamp", "unit_id")]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  Anomaly Detection (Isolation Forest per unit)
# ─────────────────────────────────────────────────────────────────────────────

async def _train_anomaly_model(unit_id: str, df: pd.DataFrame) -> Optional[IsolationForest]:
    """Fit an Isolation Forest on the unit's recent telemetry."""
    if len(df) < 50:  # insufficient data
        return None

    feat_df = _build_features(df)
    feature_cols = [c for c in feat_df.columns if c not in ("timestamp", "unit_id", "error_codes")]
    X = feat_df[feature_cols].fillna(0).values

    if X.shape[0] < 50:
        return None

    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,   # expect 5% anomaly rate
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X)
    log.info("Trained anomaly model for %s on %d samples", unit_id, len(X))
    return model


async def detect_anomalies(unit_id: str, df: pd.DataFrame) -> Tuple[bool, float]:
    """
    Returns (is_anomalous, anomaly_score).
    Score: -1 = most anomalous, 1 = most normal.
    """
    model = _anomaly_models.get(unit_id)
    if model is None:
        return False, 0.0

    feat_df = _build_features(df.tail(60))  # last 60 readings for inference
    feature_cols = [c for c in feat_df.columns if c not in ("timestamp", "unit_id", "error_codes")]
    if feat_df.empty:
        return False, 0.0

    X = feat_df[feature_cols].fillna(0).values[-1:, :]
    pred  = model.predict(X)[0]          # 1=normal, -1=anomaly
    score = model.score_samples(X)[0]    # log-likelihood; more negative = more anomalous

    # Normalise score to [0, 1] risk range
    normalised_score = max(0.0, min(1.0, (-score - 0.1) / 0.4))
    return pred == -1, normalised_score


# ─────────────────────────────────────────────────────────────────────────────
#  RUL Prediction (Gradient Boosting)
# ─────────────────────────────────────────────────────────────────────────────

def _generate_synthetic_training_data() -> Tuple[np.ndarray, np.ndarray]:
    """
    Generate synthetic RUL training data for cold-start.
    In production: replace with labelled maintenance records.

    Feature set mirrors _build_features output.
    Label: remaining useful life in days (0 = maintenance due now).
    """
    np.random.seed(42)
    N = 2000

    # Healthy units: low anomaly score, stable power, low compressor starts
    healthy_X = np.column_stack([
        np.random.uniform(18, 24, N//2),    # avg_temp
        np.random.uniform(40, 60, N//2),    # avg_humidity
        np.random.uniform(30, 70, N//2),    # avg_load
        np.random.uniform(500, 1500, N//2), # avg_power_w
        np.random.uniform(0.8, 1.0, N//2),  # efficiency_ratio
        np.random.uniform(0, 0.01, N//2),   # trend_power_slope (flat)
        np.random.uniform(0, 2, N//2),      # compressor_start_rate
        np.random.uniform(0, 0.2, N//2),    # anomaly_score
    ])
    healthy_Y = np.random.uniform(60, 365, N//2)   # 2-12 months

    # Degrading units: high anomaly score, rising power, frequent cycling
    degrading_X = np.column_stack([
        np.random.uniform(24, 32, N//2),    # higher avg temp
        np.random.uniform(55, 80, N//2),
        np.random.uniform(70, 98, N//2),    # high load
        np.random.uniform(1800, 3000, N//2),# high power (efficiency loss)
        np.random.uniform(0.3, 0.7, N//2),  # degraded efficiency
        np.random.uniform(0.05, 0.3, N//2), # rising power trend
        np.random.uniform(5, 20, N//2),     # frequent cycling
        np.random.uniform(0.5, 1.0, N//2),  # high anomaly score
    ])
    degrading_Y = np.random.uniform(0, 30, N//2)   # < 1 month

    X = np.vstack([healthy_X, degrading_X])
    Y = np.concatenate([healthy_Y, degrading_Y])

    # Shuffle
    idx = np.random.permutation(len(X))
    return X[idx], Y[idx]


def _train_rul_model() -> GradientBoostingRegressor:
    X, y = _generate_synthetic_training_data()
    model = GradientBoostingRegressor(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=4,
        subsample=0.8,
        random_state=42,
    )
    model.fit(X, y)
    log.info("Trained RUL model on %d synthetic samples (replace with real data)", len(X))
    return model


def _compute_rul(feature_vector: np.ndarray) -> Tuple[float, float]:
    """
    Returns (rul_days, risk_score).
    risk_score: 0 = healthy, 1 = immediate maintenance.
    """
    if _rul_model is None or feature_vector is None:
        return 180.0, 0.0

    # RUL model expects 8 summary features; take first 8 or pad
    n_features = 8
    if len(feature_vector) >= n_features:
        X = feature_vector[:n_features].reshape(1, -1)
    else:
        X = np.pad(feature_vector, (0, n_features - len(feature_vector))).reshape(1, -1)

    rul_days   = float(np.clip(_rul_model.predict(X)[0], 0, 730))
    risk_score = float(np.clip(1.0 - rul_days / 365.0, 0.0, 1.0))
    return rul_days, risk_score


# ─────────────────────────────────────────────────────────────────────────────
#  Main analysis pipeline (runs hourly)
# ─────────────────────────────────────────────────────────────────────────────

class MaintenancePrediction(BaseModel):
    unit_id: str
    rul_days: float
    risk_score: float
    is_anomalous: bool
    anomaly_score: float
    avg_load_30d: Optional[float]
    avg_power_30d: Optional[float]
    efficiency_trend: Optional[float]
    recommendation: str
    computed_at: datetime


def _make_recommendation(rul: float, risk: float, is_anomalous: bool) -> str:
    if risk >= 0.9 or rul < 7:
        return "🔴 IMMEDIATE MAINTENANCE REQUIRED"
    if risk >= 0.7 or rul < 21:
        return "🟠 Schedule maintenance within 1 week"
    if risk >= 0.5 or rul < 60:
        return "🟡 Plan maintenance within 2 months"
    if is_anomalous:
        return "🟡 Anomalous behaviour detected – inspect unit"
    return "🟢 Unit operating normally"


async def analyse_unit(unit_id: str) -> Optional[MaintenancePrediction]:
    """Full analysis pipeline for one AC unit."""
    try:
        df = await _load_telemetry(unit_id, days=30)
        if df.empty or len(df) < 10:
            log.debug("Insufficient data for %s – skipping", unit_id)
            return None

        # Train / update anomaly model
        model = await _train_anomaly_model(unit_id, df)
        if model:
            _anomaly_models[unit_id] = model

        is_anomalous, anomaly_score = await detect_anomalies(unit_id, df)

        feat_df = _build_features(df)
        fv = _extract_feature_vector(feat_df)
        rul_days, risk_score = _compute_rul(fv)

        avg_load  = float(df["load"].mean()) if "load" in df.columns else None
        avg_power = float(df["power_w"].mean()) if "power_w" in df.columns else None
        eff_trend = None
        if "trend_power_slope" in feat_df.columns:
            eff_trend = float(feat_df["trend_power_slope"].iloc[-1])

        prediction = MaintenancePrediction(
            unit_id=unit_id,
            rul_days=round(rul_days, 1),
            risk_score=round(risk_score, 3),
            is_anomalous=is_anomalous,
            anomaly_score=round(anomaly_score, 3),
            avg_load_30d=round(avg_load, 1) if avg_load else None,
            avg_power_30d=round(avg_power, 1) if avg_power else None,
            efficiency_trend=round(eff_trend, 4) if eff_trend else None,
            recommendation=_make_recommendation(rul_days, risk_score, is_anomalous),
            computed_at=datetime.now(timezone.utc),
        )

        # Persist to PostgreSQL
        async with AsyncSessionLocal() as db:
            await db.execute(text("""
                INSERT INTO maintenance_features
                    (unit_id, avg_load_30d, avg_power_30d, risk_score, rul_days,
                     anomaly_count_7d, last_computed)
                VALUES
                    (:uid, :al, :ap, :rs, :rul, :ac, NOW())
                ON CONFLICT (unit_id) DO UPDATE SET
                    avg_load_30d   = EXCLUDED.avg_load_30d,
                    avg_power_30d  = EXCLUDED.avg_power_30d,
                    risk_score     = EXCLUDED.risk_score,
                    rul_days       = EXCLUDED.rul_days,
                    anomaly_count_7d = EXCLUDED.anomaly_count_7d,
                    last_computed  = EXCLUDED.last_computed
            """), {
                "uid": unit_id, "al": avg_load, "ap": avg_power,
                "rs": risk_score, "rul": rul_days,
                "ac": int(is_anomalous),
            })
            await db.commit()

        # Alert if high risk
        if risk_score >= 0.7:
            severity = "CRITICAL" if risk_score >= 0.9 else "WARNING"
            await mqtt.async_publish(settings.TOPIC_SYSTEM_ALERTS, {
                "type":        "POWER_ANOMALY",
                "unit_id":     unit_id,
                "severity":    severity,
                "message":     prediction.recommendation,
                "rul_days":    rul_days,
                "risk_score":  risk_score,
                "ts":          datetime.now(timezone.utc).isoformat(),
            })

        log.info("[PredMaint] %s → RUL=%.0fd risk=%.2f anomaly=%s",
                 unit_id, rul_days, risk_score, is_anomalous)
        return prediction

    except Exception as exc:
        log.exception("Analysis failed for %s: %s", unit_id, exc)
        return None


async def run_fleet_analysis() -> None:
    """Hourly job: analyse every registered AC unit."""
    log.info("Starting fleet predictive maintenance analysis")
    async with AsyncSessionLocal() as db:
        from ..shared.models import ACUnitORM
        result = await db.execute(select(ACUnitORM.id))
        unit_ids = [row[0] for row in result.all()]

    log.info("Analysing %d units", len(unit_ids))
    for uid in unit_ids:
        await analyse_unit(uid)
        await asyncio.sleep(0.5)   # throttle InfluxDB


# ─────────────────────────────────────────────────────────────────────────────
#  FastAPI
# ─────────────────────────────────────────────────────────────────────────────

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _rul_model

    mqtt.connect()
    await mqtt.wait_connected(timeout=20.0)

    # Train baseline RUL model (replace with model loading from MLflow in prod)
    _rul_model = _train_rul_model()

    scheduler.add_job(
        run_fleet_analysis,
        trigger=IntervalTrigger(hours=1),
        id="fleet-analysis",
        replace_existing=True,
        next_run_time=datetime.now(timezone.utc),  # run immediately on startup
    )
    scheduler.start()
    log.info("Predictive Maintenance Service ready")
    yield

    scheduler.shutdown()
    mqtt.disconnect()
    await engine.dispose()


app = FastAPI(title="HVAC Predictive Maintenance Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "predictive-maintenance",
            "rul_model_ready": _rul_model is not None,
            "anomaly_models": len(_anomaly_models)}


@app.get("/predictions/{unit_id}", response_model=MaintenancePrediction)
async def get_prediction(unit_id: str):
    """Trigger on-demand analysis for a single unit."""
    result = await analyse_unit(unit_id)
    if not result:
        raise HTTPException(status_code=404, detail="Insufficient data or unit not found")
    return result


@app.get("/predictions", response_model=List[MaintenancePrediction])
async def list_predictions(risk_min: float = 0.0, limit: int = 100):
    """Return latest stored predictions from PostgreSQL."""
    async with AsyncSessionLocal() as db:
        rows = await db.execute(text("""
            SELECT mf.*, u.zone_id
            FROM maintenance_features mf
            JOIN ac_units u ON u.id = mf.unit_id
            WHERE mf.risk_score >= :rmin
            ORDER BY mf.risk_score DESC
            LIMIT :lim
        """), {"rmin": risk_min, "lim": limit})
        results = rows.mappings().all()

    return [MaintenancePrediction(
        unit_id=r["unit_id"],
        rul_days=r["rul_days"] or 180.0,
        risk_score=r["risk_score"] or 0.0,
        is_anomalous=(r["anomaly_count_7d"] or 0) > 0,
        anomaly_score=min(1.0, (r["anomaly_count_7d"] or 0) / 10),
        avg_load_30d=r["avg_load_30d"],
        avg_power_30d=r["avg_power_30d"],
        efficiency_trend=None,
        recommendation=_make_recommendation(
            r["rul_days"] or 180.0,
            r["risk_score"] or 0.0,
            (r["anomaly_count_7d"] or 0) > 0,
        ),
        computed_at=r["last_computed"] or datetime.now(timezone.utc),
    ) for r in results]


@app.get("/fleet/summary")
async def fleet_summary():
    """Aggregated fleet health overview."""
    async with AsyncSessionLocal() as db:
        row = await db.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE risk_score >= 0.9) AS critical,
                COUNT(*) FILTER (WHERE risk_score >= 0.7 AND risk_score < 0.9) AS warning,
                COUNT(*) FILTER (WHERE risk_score < 0.7) AS healthy,
                AVG(rul_days) AS avg_rul_days,
                AVG(risk_score) AS avg_risk_score
            FROM maintenance_features
        """))
        r = row.mappings().first()
    return {
        "critical_units": r["critical"] or 0,
        "warning_units":  r["warning"] or 0,
        "healthy_units":  r["healthy"] or 0,
        "avg_rul_days":   round(r["avg_rul_days"] or 180.0, 1),
        "avg_risk_score": round(r["avg_risk_score"] or 0.0, 3),
        "ts": datetime.now(timezone.utc),
    }


@app.post("/analysis/run", status_code=202)
async def trigger_analysis():
    """Manually trigger a full fleet analysis."""
    asyncio.create_task(run_fleet_analysis())
    return {"status": "triggered", "ts": datetime.now(timezone.utc)}
