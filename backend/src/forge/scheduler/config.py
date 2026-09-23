from dataclasses import dataclass


@dataclass(frozen=True)
class SchedulerConfig:
    stream_name: str = "forge:events"
    consumer_group: str = "scheduler"
    consumer_name: str = "scheduler-1"
    
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
        )