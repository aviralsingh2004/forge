from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from forge.worker.registration import WorkerRegistration
from forge.worker.worker import Worker


@pytest.fixture
def registration() -> WorkerRegistration:
    return WorkerRegistration(
        name="worker-1",
        version="0.1.0",
        cpu_capacity=8,
        memory_capacity_mb=16384,
        gpu_capacity=1,
        capabilities=["docker", "python"],
    )


@pytest.mark.asyncio
async def test_register_sets_worker_id_from_registrar_response(
    registration: WorkerRegistration,
) -> None:
    registrar = AsyncMock()
    registrar.register.return_value = {
        "id": "00000000-0000-0000-0000-000000000001",
    }
    worker = Worker(registration, registrar)

    assert worker.id is None
    await worker.register()

    registrar.register.assert_awaited_once_with(registration)
    assert worker.id == UUID("00000000-0000-0000-0000-000000000001")


@pytest.mark.asyncio
async def test_register_raises_value_error_for_invalid_worker_id(
    registration: WorkerRegistration,
) -> None:
    registrar = AsyncMock()
    registrar.register.return_value = {"id": "not-a-uuid"}
    worker = Worker(registration, registrar)

    with pytest.raises(ValueError):
        await worker.register()
