"""Create the initial Forge persistence schema.

Revision ID: 0001_initial_schema
Revises:
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create all Phase 1 tables, constraints, indexes, and enum types."""

    bind = op.get_bind()
    enum_values = {
        "worker_status": ("REGISTERING", "READY", "BUSY", "DRAINING", "OFFLINE"),
        "job_status": ("QUEUED", "ASSIGNED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"),
        "attempt_status": (
            "CREATED",
            "ASSIGNED",
            "STARTING",
            "RUNNING",
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
        ),
        "assignment_status": (
            "CREATED",
            "DELIVERED",
            "ACKNOWLEDGED",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
        ),
        "reservation_status": ("ACTIVE", "RELEASED"),
    }
    for name, values in enum_values.items():
        postgresql.ENUM(*values, name=name).create(bind, checkfirst=True)

    def enum(name: str) -> postgresql.ENUM:
        return postgresql.ENUM(*enum_values[name], name=name, create_type=False)

    uuid_type = postgresql.UUID(as_uuid=True)
    json_type = postgresql.JSONB(astext_type=sa.Text())

    op.create_table(
        "workers",
        sa.Column("id", uuid_type, primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("status", enum("worker_status"), nullable=False),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("cpu_capacity", sa.Integer, nullable=False),
        sa.Column("memory_capacity_mb", sa.Integer, nullable=False),
        sa.Column("gpu_capacity", sa.Integer, nullable=False),
        sa.Column("capabilities", json_type, nullable=False),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("cpu_capacity >= 0", name="ck_workers_cpu_capacity_nonnegative"),
        sa.CheckConstraint("memory_capacity_mb >= 0", name="ck_workers_memory_capacity_nonnegative"),
        sa.CheckConstraint("gpu_capacity >= 0", name="ck_workers_gpu_capacity_nonnegative"),
        sa.UniqueConstraint("name", name="uq_workers_name"),
    )
    op.create_index("ix_workers_status", "workers", ["status"])
    op.create_index("ix_workers_last_heartbeat_at", "workers", ["last_heartbeat_at"])

    op.create_table(
        "jobs",
        sa.Column("id", uuid_type, primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("image", sa.String(255), nullable=False),
        sa.Column("command", postgresql.ARRAY(sa.Text), nullable=False),
        sa.Column("priority", sa.Integer, nullable=False),
        sa.Column("cpu_required", sa.Integer, nullable=False),
        sa.Column("memory_required_mb", sa.Integer, nullable=False),
        sa.Column("gpu_required", sa.Integer, nullable=False),
        sa.Column("required_capabilities", json_type, nullable=False),
        sa.Column("status", enum("job_status"), nullable=False),
        sa.Column("max_retries", sa.Integer, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("priority >= 0 AND priority <= 100", name="ck_jobs_priority_range"),
        sa.CheckConstraint("cpu_required >= 0", name="ck_jobs_cpu_required_nonnegative"),
        sa.CheckConstraint("memory_required_mb >= 0", name="ck_jobs_memory_required_nonnegative"),
        sa.CheckConstraint("gpu_required >= 0", name="ck_jobs_gpu_required_nonnegative"),
        sa.CheckConstraint("max_retries >= 0", name="ck_jobs_max_retries_nonnegative"),
    )
    op.create_index(
        "ix_jobs_scheduling_order",
        "jobs",
        ["status", sa.text("priority DESC"), sa.text("created_at ASC")],
    )

    op.create_table(
        "attempts",
        sa.Column("id", uuid_type, primary_key=True),
        sa.Column("job_id", uuid_type, nullable=False),
        sa.Column("attempt_number", sa.Integer, nullable=False),
        sa.Column("worker_id", uuid_type),
        sa.Column("status", enum("attempt_status"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("exit_code", sa.Integer),
        sa.Column("error_message", sa.Text),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("attempt_number > 0", name="ck_attempts_attempt_number_positive"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["worker_id"], ["workers.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("job_id", "attempt_number", name="uq_attempts_job_attempt_number"),
    )
    op.create_index("ix_attempts_job_id", "attempts", ["job_id"])
    op.create_index("ix_attempts_worker_id", "attempts", ["worker_id"])

    op.create_table(
        "assignments",
        sa.Column("id", uuid_type, primary_key=True),
        sa.Column("attempt_id", uuid_type, nullable=False),
        sa.Column("worker_id", uuid_type, nullable=False),
        sa.Column("status", enum("assignment_status"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True)),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["attempt_id"], ["attempts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["worker_id"], ["workers.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("attempt_id", name="uq_assignments_attempt_id"),
    )

    op.create_table(
        "reservations",
        sa.Column("id", uuid_type, primary_key=True),
        sa.Column("worker_id", uuid_type, nullable=False),
        sa.Column("attempt_id", uuid_type, nullable=False),
        sa.Column("cpu_reserved", sa.Integer, nullable=False),
        sa.Column("memory_reserved_mb", sa.Integer, nullable=False),
        sa.Column("gpu_reserved", sa.Integer, nullable=False),
        sa.Column("status", enum("reservation_status"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("released_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("cpu_reserved >= 0", name="ck_reservations_cpu_nonnegative"),
        sa.CheckConstraint("memory_reserved_mb >= 0", name="ck_reservations_memory_nonnegative"),
        sa.CheckConstraint("gpu_reserved >= 0", name="ck_reservations_gpu_nonnegative"),
        sa.ForeignKeyConstraint(["worker_id"], ["workers.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["attempt_id"], ["attempts.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_reservations_active_worker",
        "reservations",
        ["worker_id"],
        postgresql_where=sa.text("status = 'ACTIVE'"),
    )
    op.create_index(
        "uq_reservations_active_attempt",
        "reservations",
        ["attempt_id"],
        unique=True,
        postgresql_where=sa.text("status = 'ACTIVE'"),
    )

    op.create_table(
        "worker_heartbeats",
        sa.Column("id", uuid_type, primary_key=True),
        sa.Column("worker_id", uuid_type, nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("cpu_usage", sa.Integer, nullable=False),
        sa.Column("memory_usage_mb", sa.Integer, nullable=False),
        sa.Column("gpu_usage", sa.Integer, nullable=False),
        sa.Column("running_attempts", json_type, nullable=False),
        sa.CheckConstraint("cpu_usage >= 0", name="ck_heartbeats_cpu_usage_nonnegative"),
        sa.CheckConstraint("memory_usage_mb >= 0", name="ck_heartbeats_memory_usage_nonnegative"),
        sa.CheckConstraint("gpu_usage >= 0", name="ck_heartbeats_gpu_usage_nonnegative"),
        sa.ForeignKeyConstraint(["worker_id"], ["workers.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_worker_heartbeats_worker_timestamp",
        "worker_heartbeats",
        ["worker_id", sa.text("timestamp DESC")],
    )

    op.create_table(
        "job_events",
        sa.Column("id", uuid_type, primary_key=True),
        sa.Column("job_id", uuid_type, nullable=False),
        sa.Column("attempt_id", uuid_type),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("metadata", json_type, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["attempt_id"], ["attempts.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_job_events_job_created",
        "job_events",
        ["job_id", sa.text("created_at DESC")],
    )

    op.create_table(
        "outbox_events",
        sa.Column("id", uuid_type, primary_key=True),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("payload", json_type, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("retry_count", sa.Integer, nullable=False),
        sa.Column("last_error", sa.Text),
        sa.CheckConstraint("retry_count >= 0", name="ck_outbox_retry_count_nonnegative"),
    )
    op.create_index(
        "ix_outbox_unpublished_created",
        "outbox_events",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    """Drop all Phase 1 tables and their PostgreSQL enum types."""

    for table in (
        "outbox_events",
        "job_events",
        "worker_heartbeats",
        "reservations",
        "assignments",
        "attempts",
        "jobs",
        "workers",
    ):
        op.drop_table(table)

    bind = op.get_bind()
    for name in (
        "reservation_status",
        "assignment_status",
        "attempt_status",
        "job_status",
        "worker_status",
    ):
        postgresql.ENUM(name=name).drop(bind, checkfirst=True)
