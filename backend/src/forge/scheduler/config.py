import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SchedulerConfig:
    stream_name: str = "forge:events"
    consumer_group: str = "scheduler"
    consumer_name: str = "scheduler-1"
    pending_min_idle_ms: int = 60_000
    worker_heartbeat_timeout_seconds: int = 30

    @classmethod
    def from_env(cls) -> "SchedulerConfig":
        return cls(
            stream_name=os.getenv("FORGE_REDIS_STREAM", cls.stream_name),
            consumer_group=os.getenv(
                "FORGE_SCHEDULER_CONSUMER_GROUP",
                cls.consumer_group,
            ),
            consumer_name=os.getenv(
                "FORGE_SCHEDULER_CONSUMER_NAME",
                cls.consumer_name,
            ),
            pending_min_idle_ms=int(
                os.getenv(
                    "FORGE_SCHEDULER_PENDING_MIN_IDLE_MS",
                    cls.pending_min_idle_ms,
                )
            ),
            worker_heartbeat_timeout_seconds=int(
                os.getenv(
                    "FORGE_SCHEDULER_WORKER_HEARTBEAT_TIMEOUT_SECONDS",
                    os.getenv(
                        "FORGE_WORKER_HEARTBEAT_TIMEOUT_SECONDS",
                        cls.worker_heartbeat_timeout_seconds,
                    ),
                )
            ),
        )
