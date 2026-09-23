import asyncio

from forge.scheduler.consumer import SchedulerEventConsumer
from forge.scheduler.processor import SchedulerEventProcessor


class SchedulerRunner:
    def __init__(
        self,
        consumer: SchedulerEventConsumer,
        processor: SchedulerEventProcessor,
    ) -> None:
        self.consumer = consumer
        self.processor = processor
        self._stop_event = asyncio.Event()

    async def run_once(self) -> None:
        pending_events = await self.consumer.consume_pending()

        for message_id, data in pending_events:
            await self.processor.process(message_id, data)
            await self.consumer.acknowledge(message_id)

        if pending_events:
            return

        events = await self.consumer.consume()

        for message_id, data in events:
            await self.processor.process(message_id, data)
            await self.consumer.acknowledge(message_id)

    async def run(self) -> None:
        while not self._stop_event.is_set():
            await self.run_once()

    def stop(self) -> None:
        self._stop_event.set()
