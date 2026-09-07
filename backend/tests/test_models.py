from forge.db import models  # noqa: F401 - registers all model tables
from forge.db.base import Base


def test_phase_one_tables_are_registered() -> None:
    assert set(Base.metadata.tables) == {
        "workers",
        "jobs",
        "attempts",
        "assignments",
        "reservations",
        "worker_heartbeats",
        "job_events",
        "outbox_events",
    }


def test_scheduler_indexes_are_registered() -> None:
    def names(table_name: str) -> set[str | None]:
        return {index.name for index in Base.metadata.tables[table_name].indexes}

    assert "ix_jobs_scheduling_order" in names("jobs")
    assert "ix_reservations_active_worker" in names("reservations")
    assert "uq_reservations_active_attempt" in names("reservations")
    assert "ix_outbox_unpublished_created" in names("outbox_events")
