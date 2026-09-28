"""
Health endpoint tests — Phase 9 Requirement 2.

GET /health/live  — pure liveness, no dependencies.
GET /health/ready — requires both PostgreSQL and Redis.

Test matrix:
  1. PostgreSQL available + Redis available  → 200
  2. PostgreSQL unavailable + Redis available → 503 (PostgreSQL detail)
  3. PostgreSQL available + Redis unavailable → 503 (Redis detail)
  4. Both unavailable                         → 503 (PostgreSQL detail, checked first)
  5. /health/live is always 200 regardless of dependencies

Dependencies are replaced via app.dependency_overrides — no real servers needed.
"""
from collections.abc import AsyncGenerator

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy.exc import OperationalError

from forge.api.dependencies import get_redis
from forge.api.main import app
from forge.db.session import get_session


# ---------------------------------------------------------------------------
# Fake PostgreSQL sessions
# ---------------------------------------------------------------------------


class _HealthySession:
    async def execute(self, statement: object) -> None:
        return None


class _UnhealthySession:
    async def execute(self, statement: object) -> None:
        raise OperationalError("SELECT 1", {}, Exception("database unavailable"))


def _session_override(session: object):
    async def override() -> AsyncGenerator[object, None]:
        yield session

    return override


# ---------------------------------------------------------------------------
# Fake Redis clients
# ---------------------------------------------------------------------------


class _HealthyRedis:
    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        pass


class _UnhealthyRedis:
    async def ping(self) -> None:
        raise RedisConnectionError("Redis unavailable")

    async def aclose(self) -> None:
        pass


def _redis_override(client: object):
    async def override() -> AsyncGenerator[object, None]:
        yield client

    return override


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _call_ready(pg_session: object, redis_client: object) -> object:
    app.dependency_overrides[get_session] = _session_override(pg_session)
    app.dependency_overrides[get_redis] = _redis_override(redis_client)
    try:
        with TestClient(app) as client:
            return client.get("/health/ready")
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# /health/live — always independent
# ---------------------------------------------------------------------------


def test_live_health_check() -> None:
    """GET /health/live is always 200 regardless of dependency availability."""
    with TestClient(app) as client:
        response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_live_health_check_ignores_postgres_failure() -> None:
    """/health/live must not touch PostgreSQL — override must not affect it."""
    app.dependency_overrides[get_session] = _session_override(_UnhealthySession())
    app.dependency_overrides[get_redis] = _redis_override(_UnhealthyRedis())
    try:
        with TestClient(app) as client:
            response = client.get("/health/live")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# /health/ready — test matrix
# ---------------------------------------------------------------------------


def test_ready_health_check_when_both_available() -> None:
    """Matrix 1: PostgreSQL available + Redis available → 200."""
    response = _call_ready(_HealthySession(), _HealthyRedis())

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_health_check_when_postgres_unavailable() -> None:
    """Matrix 2: PostgreSQL unavailable + Redis available → 503."""
    response = _call_ready(_UnhealthySession(), _HealthyRedis())

    assert response.status_code == 503
    assert response.json() == {"detail": "PostgreSQL is unavailable"}


def test_ready_health_check_when_redis_unavailable() -> None:
    """Matrix 3: PostgreSQL available + Redis unavailable → 503."""
    response = _call_ready(_HealthySession(), _UnhealthyRedis())

    assert response.status_code == 503
    assert response.json() == {"detail": "Redis is unavailable"}


def test_ready_health_check_when_both_unavailable() -> None:
    """Matrix 4: Both unavailable → 503 (PostgreSQL is checked first)."""
    response = _call_ready(_UnhealthySession(), _UnhealthyRedis())

    assert response.status_code == 503
    # PostgreSQL is checked first; its error is surfaced.
    assert response.json() == {"detail": "PostgreSQL is unavailable"}


# ---------------------------------------------------------------------------
# Backward-compatible alias: legacy test names still pass
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("pg", "redis_client", "expected_status", "expected_detail"),
    [
        (_HealthySession(), _HealthyRedis(), 200, None),
        (_UnhealthySession(), _HealthyRedis(), 503, "PostgreSQL is unavailable"),
        (_HealthySession(), _UnhealthyRedis(), 503, "Redis is unavailable"),
    ],
)
def test_ready_health_check_parametrized(
    pg: object,
    redis_client: object,
    expected_status: int,
    expected_detail: str | None,
) -> None:
    response = _call_ready(pg, redis_client)

    assert response.status_code == expected_status
    if expected_detail is not None:
        assert response.json() == {"detail": expected_detail}
    else:
        assert response.json() == {"status": "ok"}
