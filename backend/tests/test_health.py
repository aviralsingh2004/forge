from collections.abc import AsyncGenerator

from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from forge.api.main import app
from forge.db.session import get_session


class HealthySession:
    async def execute(self, statement: object) -> None:
        return None


class UnhealthySession:
    async def execute(self, statement: object) -> None:
        raise OperationalError("SELECT 1", {}, Exception("database unavailable"))


def session_override(session: object):
    async def override() -> AsyncGenerator[object, None]:
        yield session

    return override


def test_live_health_check() -> None:
    with TestClient(app) as client:
        response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_health_check_when_postgres_is_available() -> None:
    app.dependency_overrides[get_session] = session_override(HealthySession())
    try:
        with TestClient(app) as client:
            response = client.get("/health/ready")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_health_check_when_postgres_is_unavailable() -> None:
    app.dependency_overrides[get_session] = session_override(UnhealthySession())
    try:
        with TestClient(app) as client:
            response = client.get("/health/ready")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json() == {"detail": "PostgreSQL is unavailable"}
