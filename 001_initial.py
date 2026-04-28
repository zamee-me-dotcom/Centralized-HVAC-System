"""Initial schema

Revision ID: 001_initial
Revises:
Create Date: 2025-01-01 00:00:00
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── zones ──────────────────────────────────────────────────────────────
    op.create_table(
        "zones",
        sa.Column("id",          sa.String(64),  primary_key=True),
        sa.Column("name",        sa.String(128), nullable=False),
        sa.Column("building",    sa.String(128)),
        sa.Column("floor",       sa.String(32)),
        sa.Column("area_sqm",    sa.Float()),
        sa.Column("target_temp", sa.Float(), server_default="22.0"),
        sa.Column("occupancy",   sa.Boolean(), server_default="false"),
        sa.Column("metadata",    postgresql.JSONB(), server_default="{}"),
        sa.Column("created_at",  sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )

    # ── ac_units ────────────────────────────────────────────────────────────
    op.create_table(
        "ac_units",
        sa.Column("id",                  sa.String(64),  primary_key=True),
        sa.Column("zone_id",             sa.String(64),  sa.ForeignKey("zones.id"), nullable=False),
        sa.Column("name",                sa.String(128), nullable=False),
        sa.Column("model",               sa.String(128)),
        sa.Column("firmware_version",    sa.String(32)),
        sa.Column("capacity_kw",         sa.Float(),  nullable=False),
        sa.Column("status",              sa.Enum("ON","OFF","FAULT","OFFLINE","STANDBY",
                                                  name="unitstatus"), server_default="OFFLINE"),
        sa.Column("current_temp",        sa.Float()),
        sa.Column("target_temp",         sa.Float(), server_default="22.0"),
        sa.Column("fan_speed",           sa.Enum("LOW","MEDIUM","HIGH","AUTO", name="fanspeed"),
                                         server_default="AUTO"),
        sa.Column("load_percentage",     sa.Float(), server_default="0.0"),
        sa.Column("power_consumption_w", sa.Float(), server_default="0.0"),
        sa.Column("last_seen",           sa.DateTime(timezone=True)),
        sa.Column("cert_fingerprint",    sa.String(128)),
        sa.Column("tags",                postgresql.JSONB(), server_default="{}"),
        sa.Column("created_at",          sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at",          sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )
    op.create_index("idx_ac_units_zone_id", "ac_units", ["zone_id"])
    op.create_index("idx_ac_units_status",  "ac_units", ["status"])

    # ── alerts (declarative partitioning by month via pg_partman in prod) ──
    op.create_table(
        "alerts",
        sa.Column("id",          sa.String(64),  primary_key=True),
        sa.Column("unit_id",     sa.String(64),  sa.ForeignKey("ac_units.id"), nullable=False),
        sa.Column("zone_id",     sa.String(64)),
        sa.Column("alert_type",  sa.Enum("OVERHEAT","OFFLINE","HIGH_LOAD","POWER_ANOMALY",
                                          "PID_DIVERGE","SENSOR_FAULT", name="alerttype"), nullable=False),
        sa.Column("severity",    sa.Enum("INFO","WARNING","CRITICAL", name="alertseverity"), nullable=False),
        sa.Column("message",     sa.String(512)),
        sa.Column("resolved",    sa.Boolean(), server_default="false"),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        sa.Column("payload",     postgresql.JSONB(), server_default="{}"),
        sa.Column("created_at",  sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )
    op.create_index("idx_alerts_unit_id",    "alerts", ["unit_id"])
    op.create_index("idx_alerts_created_at", "alerts", ["created_at"])
    op.create_index("idx_alerts_unit_open",  "alerts", ["unit_id"],
                    postgresql_where=sa.text("resolved = false"))

    # ── command_log (audit trail of every issued command) ──────────────────
    op.create_table(
        "command_log",
        sa.Column("id",             sa.String(64), primary_key=True),
        sa.Column("unit_id",        sa.String(64), sa.ForeignKey("ac_units.id"), nullable=False),
        sa.Column("correlation_id", sa.String(64)),
        sa.Column("issued_by",      sa.String(128)),
        sa.Column("command",        postgresql.JSONB(), nullable=False),
        sa.Column("ack_received",   sa.Boolean(), server_default="false"),
        sa.Column("ack_at",         sa.DateTime(timezone=True)),
        sa.Column("created_at",     sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )
    op.create_index("idx_cmd_log_unit_id",   "command_log", ["unit_id"])
    op.create_index("idx_cmd_log_created",   "command_log", ["created_at"])

    # ── api_keys ────────────────────────────────────────────────────────────
    op.create_table(
        "api_keys",
        sa.Column("id",          sa.String(64),   primary_key=True),
        sa.Column("key_hash",    sa.String(128),  nullable=False, unique=True),
        sa.Column("name",        sa.String(128),  nullable=False),
        sa.Column("roles",       postgresql.ARRAY(sa.Text()), server_default="{}"),
        sa.Column("last_used",   sa.DateTime(timezone=True)),
        sa.Column("expires_at",  sa.DateTime(timezone=True)),
        sa.Column("active",      sa.Boolean(), server_default="true"),
        sa.Column("created_at",  sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )

    # ── predictive_maintenance_features ─────────────────────────────────────
    op.create_table(
        "maintenance_features",
        sa.Column("unit_id",          sa.String(64), sa.ForeignKey("ac_units.id"),
                                      primary_key=True),
        sa.Column("total_runtime_h",  sa.Float(), server_default="0.0"),
        sa.Column("compressor_cycles",sa.Integer(), server_default="0"),
        sa.Column("avg_load_30d",     sa.Float()),
        sa.Column("avg_power_30d",    sa.Float()),
        sa.Column("anomaly_count_7d", sa.Integer(), server_default="0"),
        sa.Column("rul_days",         sa.Float()),   # remaining useful life prediction
        sa.Column("risk_score",       sa.Float()),   # 0.0 – 1.0
        sa.Column("last_computed",    sa.DateTime(timezone=True)),
    )

    # ── Trigger: auto-update ac_units.updated_at ────────────────────────────
    op.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ language 'plpgsql';
    """)
    op.execute("""
        CREATE TRIGGER trg_ac_units_updated_at
        BEFORE UPDATE ON ac_units
        FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_ac_units_updated_at ON ac_units")
    op.execute("DROP FUNCTION IF EXISTS update_updated_at_column()")
    op.drop_table("maintenance_features")
    op.drop_table("api_keys")
    op.drop_table("command_log")
    op.drop_table("alerts")
    op.drop_table("ac_units")
    op.drop_table("zones")
    op.execute("DROP TYPE IF EXISTS unitstatus")
    op.execute("DROP TYPE IF EXISTS fanspeed")
    op.execute("DROP TYPE IF EXISTS alerttype")
    op.execute("DROP TYPE IF EXISTS alertseverity")
