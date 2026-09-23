from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from forge.scheduler.consumer import SchedulerEventConsumer
from forge.scheduler.processor import SchedulerEventProcessor
from forge.scheduler.runner import SchedulerRunner
from forge.scheduler.service import Scheduler
from forge.scheduler.config import SchedulerConfig

def create_scheduler(
    session: AsyncSession,
    redis: Redis,
    config: SchedulerConfig,
) -> SchedulerRunner:
    scheduler = Scheduler(session=session)

    consumer = SchedulerEventConsumer(
        redis=redis,
        stream_name=config.stream_name,
        consumer_group=config.consumer_group,
        consumer_name=config.consumer_name,
    )

    processor = SchedulerEventProcessor(scheduler=scheduler)

    return SchedulerRunner(
        consumer=consumer,
        processor=processor,
    )