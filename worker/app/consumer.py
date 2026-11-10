import asyncio
import json
import logging
import os
import socket
from typing import Dict, Any, Optional
import redis.asyncio as aioredis
from prometheus_client import Counter, Histogram

from .models import QueuedEvent
from .db.database import Database
from .dispatcher import WebhookDispatcher
from .retry import RetryManager
from .dlq import DeadLetterQueueManager
from .idempotency import IdempotencyManager

logger = logging.getLogger(__name__)

STREAM_KEY = "webhooks:stream"
GROUP_NAME = "webhook_workers"

# Prometheus metrics
DELIVERY_TOTAL = Counter("worker_webhook_deliveries_total", "Webhook delivery results", ["tenant_id", "status"])
DELIVERY_LATENCY = Histogram("worker_webhook_delivery_latency_seconds", "Outbound HTTP delivery latency", ["tenant_id"])
RETRIES_SCHEDULED_TOTAL = Counter("worker_retries_scheduled_total", "Scheduled retries", ["tenant_id"])
DLQ_ROUTED_TOTAL = Counter("worker_dlq_routed_total", "Events moved to DLQ", ["tenant_id"])

class StreamConsumer:
    def __init__(
        self,
        redis_client: aioredis.Redis,
        db: Database,
        dispatcher: WebhookDispatcher,
        retry_mgr: RetryManager,
        dlq_mgr: DeadLetterQueueManager,
        idempotency_mgr: IdempotencyManager,
        consumer_name: Optional[str] = None
    ):
        self.redis = redis_client
        self.db = db
        self.dispatcher = dispatcher
        self.retry_mgr = retry_mgr
        self.dlq_mgr = dlq_mgr
        self.idempotency_mgr = idempotency_mgr
        self.consumer_name = consumer_name or f"worker-{socket.gethostname()}-{os.getpid()}"
        self.is_running = False

    async def setup_consumer_group(self):
        try:
            await self.redis.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
            logger.info(f"Consumer group '{GROUP_NAME}' created on stream '{STREAM_KEY}'")
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{GROUP_NAME}' already exists.")
            else:
                raise

    async def start(self):
        self.is_running = True
        await self.setup_consumer_group()
        logger.info(f"Consumer [{self.consumer_name}] listening for stream events...")

        # Stalled message recovery interval tracker
        last_autoclaim_time = 0.0

        while self.is_running:
            try:
                now = asyncio.get_event_loop().time()
                # 1. Stalled message reclamation every 30s (detect and salvage dead worker tasks)
                if now - last_autoclaim_time > 30.0:
                    last_autoclaim_time = now
                    try:
                        # Auto-claim messages pending for more than 60,000 ms (1 minute)
                        claim_res = await self.redis.xautoclaim(
                            STREAM_KEY, GROUP_NAME, self.consumer_name, 60000, "0-0", count=5
                        )
                        if claim_res and len(claim_res) > 1 and claim_res[1]:
                            logger.warning(f"Reclaimed {len(claim_res[1])} pending messages from crashed workers.")
                            for msg_id, raw_fields in claim_res[1]:
                                await self.process_message(msg_id, raw_fields)
                    except Exception as e:
                        logger.warning(f"Error in xautoclaim: {e}")

                # 2. Read new messages from the stream
                entries = await self.redis.xreadgroup(
                    GROUP_NAME,
                    self.consumer_name,
                    {STREAM_KEY: ">"},
                    count=10,
                    block=2000
                )

                if not entries:
                    continue

                for stream, messages in entries:
                    for msg_id, raw_fields in messages:
                        await self.process_message(msg_id, raw_fields)

            except asyncio.CancelledError:
                logger.info(f"Consumer [{self.consumer_name}] received stop signal.")
                self.is_running = False
                break
            except Exception as e:
                logger.error(f"Error in consumer loop: {e}", exc_info=True)
                await asyncio.sleep(2.0)

    async def process_message(self, msg_id: str | bytes, raw_fields: Dict[bytes | str, bytes | str]):
        # Normalize fields to string keys/values
        fields = {
            (k.decode("utf-8") if isinstance(k, bytes) else k): (v.decode("utf-8") if isinstance(v, bytes) else v)
            for k, v in raw_fields.items()
        }

        stream_msg_id = msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)

        try:
            event_id = fields["event_id"]
            tenant_id = fields["tenant_id"]
            idempotency_key = fields.get("idempotency_key") or None
            event_type = fields.get("event_type", "webhook.event")
            payload = json.loads(fields["payload"])
            received_at = int(fields.get("received_at", 0))
            max_retries = int(fields.get("max_retries", 5))
            attempt_count = int(fields.get("attempt_count", 0))

            event = QueuedEvent(
                event_id=event_id,
                tenant_id=tenant_id,
                idempotency_key=idempotency_key,
                event_type=event_type,
                payload=payload,
                received_at=received_at,
                max_retries=max_retries,
                attempt_count=attempt_count,
                stream_message_id=stream_msg_id
            )
        except Exception as e:
            logger.error(f"Malformed stream entry {stream_msg_id}: {e}. Acknowledging to discard.")
            await self.redis.xack(STREAM_KEY, GROUP_NAME, stream_msg_id)
            return

        # 3. Idempotency Check: if already completed, acknowledge and return immediately
        if await self.idempotency_mgr.is_already_delivered(event.tenant_id, event.idempotency_key):
            logger.info(f"Event {event.event_id} already marked delivered via key '{event.idempotency_key}'. Skipping.")
            await self.redis.xack(STREAM_KEY, GROUP_NAME, stream_msg_id)
            return

        # 4. Acquire single-flight lock
        locked = await self.idempotency_mgr.acquire_processing_lock(event.event_id)
        if not locked:
            logger.debug(f"Event {event.event_id} currently locked by another worker thread. Skipping.")
            return

        try:
            tenant = await self.db.get_tenant(event.tenant_id)
            if not tenant:
                logger.error(f"Tenant {event.tenant_id} not found in database. Moving event to DLQ.")
                await self.dlq_mgr.route_to_dlq(event, reason="TENANT_NOT_FOUND")
                await self.redis.xack(STREAM_KEY, GROUP_NAME, stream_msg_id)
                return

            # 5. Outbound HTTP Dispatch with Outbound HMAC-SHA256 Signing
            delivery_result, headers = await self.dispatcher.dispatch(tenant, event)

            # Record Prometheus latency metric
            DELIVERY_LATENCY.labels(tenant_id=event.tenant_id).observe(delivery_result.latency_ms / 1000.0)

            # 6. Audit attempt in PostgreSQL
            await self.db.record_delivery_attempt(
                event_id=event.event_id,
                attempt_number=event.attempt_count + 1,
                destination_url=tenant.destination_url,
                request_headers=headers,
                result=delivery_result
            )

            if delivery_result.success:
                DELIVERY_TOTAL.labels(tenant_id=event.tenant_id, status="success").inc()
                logger.info(
                    f"Event {event.event_id} DELIVERED to {tenant.destination_url} "
                    f"in {delivery_result.latency_ms}ms (HTTP {delivery_result.status_code})"
                )

                # Mark as delivered in DB & Idempotency store
                await self.db.update_event_status(
                    event_id=event.event_id,
                    status="DELIVERED",
                    retry_count=event.attempt_count,
                    delivered=True
                )
                await self.idempotency_mgr.mark_delivered(event.tenant_id, event.idempotency_key, event.event_id)

                # Acknowledge in Redis Stream
                await self.redis.xack(STREAM_KEY, GROUP_NAME, stream_msg_id)

            else:
                DELIVERY_TOTAL.labels(tenant_id=event.tenant_id, status="failed").inc()
                event.attempt_count += 1

                if event.attempt_count < event.max_retries:
                    RETRIES_SCHEDULED_TOTAL.labels(tenant_id=event.tenant_id).inc()
                    # Schedule delayed retry via ZSET backoff
                    await self.retry_mgr.schedule_retry(event)
                    await self.db.update_event_status(
                        event_id=event.event_id,
                        status="RETRYING",
                        retry_count=event.attempt_count
                    )
                    # Acknowledge from primary stream since it is safely in the ZSET retry buffer
                    await self.redis.xack(STREAM_KEY, GROUP_NAME, stream_msg_id)
                else:
                    DLQ_ROUTED_TOTAL.labels(tenant_id=event.tenant_id).inc()
                    # Retries exhausted -> DLQ
                    await self.dlq_mgr.route_to_dlq(
                        event,
                        reason="EXHAUSTED_RETRIES",
                        last_error=delivery_result.error_message or f"HTTP {delivery_result.status_code}"
                    )
                    await self.redis.xack(STREAM_KEY, GROUP_NAME, stream_msg_id)

        finally:
            await self.idempotency_mgr.release_processing_lock(event.event_id)
