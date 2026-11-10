import asyncio
import logging
import os
import signal
import sys
import redis.asyncio as aioredis
from prometheus_client import start_http_server

from .db.database import Database
from .circuit_breaker import CircuitBreakerRegistry
from .dispatcher import WebhookDispatcher
from .retry import RetryManager
from .dlq import DeadLetterQueueManager
from .idempotency import IdempotencyManager
from .consumer import StreamConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("worker-main")

async def main():
    logger.info("Initializing Polyglot Python Worker Engine...")

    redis_addr = os.getenv("REDIS_ADDR", "redis:6379")
    redis_password = os.getenv("REDIS_PASSWORD", None)
    db_url = os.getenv("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/webhooks?sslmode=disable")
    metrics_port = int(os.getenv("METRICS_PORT", "9100"))

    # 1. Start Prometheus HTTP Exporter
    try:
        start_http_server(metrics_port)
        logger.info(f"Prometheus metrics server exposed on port {metrics_port}")
    except Exception as e:
        logger.warning(f"Failed to bind metrics server on {metrics_port}: {e}")

    # 2. Redis Connection
    redis_client = aioredis.from_url(
        f"redis://{redis_addr}",
        password=redis_password,
        decode_responses=False
    )
    await redis_client.ping()
    logger.info("Connected to Redis successfully.")

    # 3. PostgreSQL Database Pool
    db = Database(db_url)
    await db.connect()

    # 4. Core Subsystems
    circuit_breakers = CircuitBreakerRegistry()
    dispatcher = WebhookDispatcher(circuit_breakers)
    await dispatcher.init_session()

    retry_mgr = RetryManager(redis_client)
    dlq_mgr = DeadLetterQueueManager(redis_client, db)
    idempotency_mgr = IdempotencyManager(redis_client)

    consumer = StreamConsumer(
        redis_client=redis_client,
        db=db,
        dispatcher=dispatcher,
        retry_mgr=retry_mgr,
        dlq_mgr=dlq_mgr,
        idempotency_mgr=idempotency_mgr
    )

    # 5. Background Tasks
    tasks = [
        asyncio.create_task(retry_mgr.start_retry_scheduler(), name="retry_scheduler"),
        asyncio.create_task(consumer.start(), name="stream_consumer")
    ]

    stop_event = asyncio.Event()

    def handle_signal():
        logger.info("Received termination signal. Shutting down worker...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass # Windows compatibility

    await stop_event.wait()

    logger.info("Cancelling worker tasks...")
    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    await dispatcher.close()
    await db.close()
    await redis_client.close()
    logger.info("Worker gracefully terminated.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
