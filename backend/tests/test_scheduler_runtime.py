from unittest.mock import AsyncMock

import pytest
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from forge.scheduler.config import SchedulerConfig
from forge.scheduler.consumer import SchedulerEventConsumer
from forge.scheduler.processor import SchedulerEventProcessor
from forge.scheduler.runner import SchedulerRunner
from forge.scheduler.runtime import create_scheduler
from forge.scheduler.service import Scheduler


def test_scheduler_config_defaults() -> None:
    config = SchedulerConfig()

    assert config.stream_name == "forge:events"
    assert config.consumer_group == "scheduler"
    assert config.consumer_name == "scheduler-1"
    assert config.pending_min_idle_ms == 60_000
    assert config.worker_heartbeat_timeout_seconds == 30


def test_scheduler_config_from_env_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FORGE_REDIS_STREAM", raising=False)
    monkeypatch.delenv("FORGE_SCHEDULER_CONSUMER_GROUP", raising=False)
    monkeypatch.delenv("FORGE_SCHEDULER_CONSUMER_NAME", raising=False)
    monkeypatch.delenv("FORGE_SCHEDULER_PENDING_MIN_IDLE_MS", raising=False)
    monkeypatch.delenv("FORGE_SCHEDULER_WORKER_HEARTBEAT_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("FORGE_WORKER_HEARTBEAT_TIMEOUT_SECONDS", raising=False)

    config = SchedulerConfig.from_env()

    assert config.stream_name == "forge:events"
    assert config.consumer_group == "scheduler"
    assert config.consumer_name == "scheduler-1"
    assert config.pending_min_idle_ms == 60_000
    assert config.worker_heartbeat_timeout_seconds == 30


def test_scheduler_config_from_env_custom(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FORGE_REDIS_STREAM", "custom:stream")
    monkeypatch.setenv("FORGE_SCHEDULER_CONSUMER_GROUP", "custom-group")
    monkeypatch.setenv("FORGE_SCHEDULER_CONSUMER_NAME", "custom-consumer-42")
    monkeypatch.setenv("FORGE_SCHEDULER_PENDING_MIN_IDLE_MS", "45000")
    monkeypatch.setenv("FORGE_SCHEDULER_WORKER_HEARTBEAT_TIMEOUT_SECONDS", "45")

    config = SchedulerConfig.from_env()

    assert config.stream_name == "custom:stream"
    assert config.consumer_group == "custom-group"
    assert config.consumer_name == "custom-consumer-42"
    assert config.pending_min_idle_ms == 45_000
    assert isinstance(config.pending_min_idle_ms, int)
    assert config.worker_heartbeat_timeout_seconds == 45
    assert isinstance(config.worker_heartbeat_timeout_seconds, int)


def test_scheduler_config_from_env_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FORGE_SCHEDULER_WORKER_HEARTBEAT_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv("FORGE_WORKER_HEARTBEAT_TIMEOUT_SECONDS", "20")

    config = SchedulerConfig.from_env()

    assert config.worker_heartbeat_timeout_seconds == 20
    assert isinstance(config.worker_heartbeat_timeout_seconds, int)


def test_create_scheduler_wiring() -> None:
    session = AsyncMock(spec=AsyncSession)
    redis = AsyncMock(spec=Redis)
    config = SchedulerConfig(
        stream_name="test:events",
        consumer_group="test-group",
        consumer_name="test-worker",
        pending_min_idle_ms=12_345,
        worker_heartbeat_timeout_seconds=15,
    )

    runner = create_scheduler(
        session=session,
        redis=redis,
        config=config,
    )

    assert isinstance(runner, SchedulerRunner)

    # Verify Consumer wiring
    consumer = runner.consumer
    assert isinstance(consumer, SchedulerEventConsumer)
    assert consumer.redis is redis
    assert consumer.stream_name == "test:events"
    assert consumer.consumer_group == "test-group"
    assert consumer.consumer_name == "test-worker"
    assert consumer.pending_min_idle_ms == 12_345

    # Verify Processor wiring
    processor = runner.processor
    assert isinstance(processor, SchedulerEventProcessor)
    assert isinstance(processor.scheduler, Scheduler)
    assert processor.scheduler.session is session
    assert processor.scheduler.heartbeat_timeout_seconds == 15
