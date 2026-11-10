import json
import logging
from typing import Optional
import redis.asyncio as aioredis
from .models import QueuedEvent
from .db.database import Database

logger = logging.getLogger(__name__)

DLQ_STREAM_KEY = "webhooks:dlq"
ACTIVE_STREAM_KEY = "webhooks:stream"

class DeadLetterQueueManager:
    def __init__(self, redis_client: aioredis.Redis, db: Database):
        self.redis = redis_client
        self.db = db

    async def route_to_dlq(self, event: QueuedEvent, reason: str, last_error: Optional[str] = None):
        """
        Routes an exhausted event to the Dead-Letter Queue stream and marks it in Postgres.
        """
        logger.warning(
            f"[DLQ] Event {event.event_id} for tenant {event.tenant_id} permanently failed after "
            f"{event.attempt_count} attempts. Moving to DLQ. Reason: {reason}"
        )

        # 1. Push to Redis DLQ Stream
        await self.redis.xadd(DLQ_STREAM_KEY, {
            "event_id": event.event_id,
            "tenant_id": event.tenant_id,
            "idempotency_key": event.idempotency_key or "",
            "event_type": event.event_type,
            "payload": json.dumps(event.payload),
            "received_at": event.received_at,
            "max_retries": event.max_retries,
            "attempts": event.attempt_count,
            "reason": reason,
            "last_error": last_error or ""
        })

        # 2. Update Postgres
        await self.db.update_event_status(
            event_id=event.event_id,
            status="FAILED_DLQ",
            retry_count=event.attempt_count,
            failed=True
        )
        await self.db.record_dead_letter(
            event_id=event.event_id,
            tenant_id=event.tenant_id,
            failure_reason=reason,
            total_attempts=event.attempt_count,
            last_error=last_error
        )

    async def replay_dlq_event(self, event_id: str) -> bool:
        """
        Re-injects a DLQ event back into the primary active stream with a reset retry counter.
        """
        assert self.db.pool is not None
        query = """
            SELECT e.id, e.tenant_id, e.idempotency_key, e.event_type, e.payload, e.max_retries
            FROM webhook_events e
            JOIN dead_letter_events d ON e.id = d.event_id
            WHERE e.id = $1
        """
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(query, event_id)
            if not row:
                return False

            # Reset status in DB
            await conn.execute("""
                UPDATE webhook_events
                SET status = 'REPLAYING', retry_count = 0, failed_at = NULL
                WHERE id = $1
            """, event_id)

            await conn.execute("""
                UPDATE dead_letter_events
                SET replayed_at = NOW()
                WHERE event_id = $1
            """, event_id)

            # Re-enqueue in Redis active stream
            await self.redis.xadd(ACTIVE_STREAM_KEY, {
                "event_id": str(row["id"]),
                "tenant_id": str(row["tenant_id"]),
                "idempotency_key": row["idempotency_key"] or "",
                "event_type": row["event_type"],
                "payload": json.dumps(row["payload"]),
                "received_at": int(row.get("received_at", 0)),
                "max_retries": row["max_retries"],
                "attempt_count": 0
            })

            logger.info(f"[DLQ] Successfully replayed event {event_id} back to {ACTIVE_STREAM_KEY}")
            return True
