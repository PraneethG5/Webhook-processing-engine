import logging
from typing import Optional
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

class IdempotencyManager:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    async def acquire_processing_lock(self, event_id: str, ttl_seconds: int = 60) -> bool:
        """
        Ensures only one worker process handles a given event_id concurrently.
        """
        lock_key = f"lock:event:{event_id}"
        acquired = await self.redis.set(lock_key, "locked", nx=True, ex=ttl_seconds)
        return bool(acquired)

    async def release_processing_lock(self, event_id: str):
        lock_key = f"lock:event:{event_id}"
        await self.redis.delete(lock_key)

    async def is_already_delivered(self, tenant_id: str, idempotency_key: Optional[str]) -> bool:
        """
        Checks if a client-supplied idempotency key has already been successfully delivered.
        """
        if not idempotency_key:
            return False
        
        delivered_key = f"idempotency:delivered:{tenant_id}:{idempotency_key}"
        exists = await self.redis.exists(delivered_key)
        return exists > 0

    async def mark_delivered(self, tenant_id: str, idempotency_key: Optional[str], event_id: str, ttl_seconds: int = 86400 * 7):
        """
        Marks an idempotency key as successfully delivered for 7 days.
        """
        if not idempotency_key:
            return
        
        delivered_key = f"idempotency:delivered:{tenant_id}:{idempotency_key}"
        await self.redis.set(delivered_key, event_id, ex=ttl_seconds)
