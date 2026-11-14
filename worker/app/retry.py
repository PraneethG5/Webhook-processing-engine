import asyncio
import json
import logging
import random
import time
from typing import Optional
import redis.asyncio as aioredis
from .models import QueuedEvent

logger = logging.getLogger(__name__)

DELAYED_RETRY_ZSET = "webhooks:delayed_retry"
STREAM_KEY = "webhooks:stream"

POP_READY_RETRIES_LUA = """
local zset_key = KEYS[1]
local max_score = ARGV[1]
local limit = tonumber(ARGV[2])

local members = redis.call('ZRANGEBYSCORE', zset_key, 0, max_score, 'LIMIT', 0, limit)
if #members > 0 then
    redis.call('ZREM', zset_key, unpack(members))
end
return members
"""

class RetryManager:
    def __init__(self, redis_client: aioredis.Redis, base_backoff_sec: float = 2.0, max_backoff_sec: float = 300.0):
        self.redis = redis_client
        self.base_backoff_sec = base_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self.pop_script = self.redis.register_script(POP_READY_RETRIES_LUA)

    def calculate_backoff_seconds(self, attempt_number: int) -> float:
        raw_backoff = min(self.max_backoff_sec, self.base_backoff_sec * (2 ** max(0, attempt_number - 1)))
        return round(random.uniform(raw_backoff * 0.5, raw_backoff), 2)

    async def schedule_retry(self, event: QueuedEvent) -> float:
        delay = self.calculate_backoff_seconds(event.attempt_count)
        retry_at = time.time() + delay

        event_dict = event.model_dump()
        payload_str = json.dumps(event_dict)

        await self.redis.zadd(DELAYED_RETRY_ZSET, {payload_str: retry_at})
        return delay

    async def start_retry_scheduler(self, poll_interval_sec: float = 1.0):
        while True:
            try:
                now = time.time()
                raw_events = await self.pop_script(keys=[DELAYED_RETRY_ZSET], args=[now, 50])

                if raw_events:
                    for raw in raw_events:
                        event_data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                        await self.redis.xadd(STREAM_KEY, {
                            "event_id": event_data["event_id"],
                            "tenant_id": event_data["tenant_id"],
                            "idempotency_key": event_data.get("idempotency_key") or "",
                            "event_type": event_data["event_type"],
                            "payload": json.dumps(event_data["payload"]),
                            "received_at": event_data["received_at"],
                            "max_retries": event_data["max_retries"],
                            "attempt_count": event_data.get("attempt_count", 1)
                        })

                await asyncio.sleep(poll_interval_sec)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"retry loop error: {e}")
                await asyncio.sleep(2.0)
