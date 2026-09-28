from redis.asyncio import Redis


class SchedulerEventConsumer:
    def __init__(
        self,
        redis: Redis,
        stream_name: str,
        consumer_group: str,
        consumer_name: str,
        pending_min_idle_ms: int,
    ) -> None:
        self.redis = redis
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.pending_min_idle_ms = pending_min_idle_ms

    async def consume(
        self, count: int = 1, block_ms: int = 1000
    ) -> list[tuple[str, dict[str, str]]]:
        messages = await self.redis.xreadgroup(
            groupname=self.consumer_group,
            consumername=self.consumer_name,
            streams={self.stream_name: ">"},
            count=count,
            block=block_ms,
        )

        events: list[tuple[str, dict[str, str]]] = []

        for _, stream_messages in messages:
            for message_id, data in stream_messages:
                events.append((message_id, data))

        return events

    async def acknowledge(self, message_id: str) -> None:
        await self.redis.xack(
            self.stream_name,
            self.consumer_group,
            message_id,
        )

    async def consume_pending(
        self,
        count: int = 1,
    ) -> list[tuple[str, dict[str, str]]]:
        messages = await self.redis.xreadgroup(
            groupname=self.consumer_group,
            consumername=self.consumer_name,
            streams={self.stream_name: "0"},
            count=count,
            block=0,
        )

        events: list[tuple[str, dict[str, str]]] = []

        for _, stream_messages in messages:
            for message_id, data in stream_messages:
                events.append((message_id, data))

        return events
    
    async def consume_stale_pending(
        self,
        count: int = 1,
    ) -> list[tuple[str, dict[str,str]]]:
        next_id, stream_messages, _ = await self.redis.xautoclaim(
            name=self.stream_name,
            groupname=self.consumer_group,
            consumername=self.consumer_name,
            min_idle_time=self.pending_min_idle_ms,
            start_id="0-0",
            count=count,
        )
        
        events: list[tuple[str,dict[str, str]]] = []
        
        for message_id, data in stream_messages:
            events.append((message_id, data))
            
        return events
            