import json
import logging
from typing import Optional, Dict
import asyncpg
from .models import TenantConfig, DeliveryResult

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=2,
            max_size=15,
            command_timeout=10
        )
        logger.info("Connected to PostgreSQL pool successfully.")

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def get_tenant(self, tenant_id: str) -> Optional[TenantConfig]:
        assert self.pool is not None
        query = """
            SELECT id, name, inbound_secret, outbound_secret, destination_url, rate_limit_per_second, burst_limit
            FROM tenants
            WHERE id = $1
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, tenant_id)
            if not row:
                return None
            return TenantConfig(
                id=str(row["id"]),
                name=row["name"],
                inbound_secret=row["inbound_secret"],
                outbound_secret=row["outbound_secret"],
                destination_url=row["destination_url"],
                rate_limit_per_second=row["rate_limit_per_second"],
                burst_limit=row["burst_limit"]
            )

    async def record_delivery_attempt(
        self,
        event_id: str,
        attempt_number: int,
        destination_url: str,
        request_headers: Dict[str, str],
        result: DeliveryResult
    ):
        assert self.pool is not None
        query = """
            INSERT INTO delivery_attempts (
                event_id, attempt_number, destination_url, request_headers,
                status_code, response_body, error_message, latency_ms, attempted_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                event_id,
                attempt_number,
                destination_url,
                json.dumps(request_headers),
                result.status_code,
                result.response_body,
                result.error_message,
                result.latency_ms
            )

    async def update_event_status(
        self,
        event_id: str,
        status: str,
        retry_count: int,
        delivered: bool = False,
        failed: bool = False
    ):
        assert self.pool is not None
        query = """
            UPDATE webhook_events
            SET 
                status = $2,
                retry_count = $3,
                delivered_at = CASE WHEN $4 THEN NOW() ELSE delivered_at END,
                failed_at = CASE WHEN $5 THEN NOW() ELSE failed_at END
            WHERE id = $1
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, event_id, status, retry_count, delivered, failed)

    async def record_dead_letter(
        self,
        event_id: str,
        tenant_id: str,
        failure_reason: str,
        total_attempts: int,
        last_error: Optional[str]
    ):
        assert self.pool is not None
        query = """
            INSERT INTO dead_letter_events (
                event_id, tenant_id, failure_reason, total_attempts, last_error, created_at
            ) VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (event_id) DO UPDATE SET
                total_attempts = EXCLUDED.total_attempts,
                last_error = EXCLUDED.last_error
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, event_id, tenant_id, failure_reason, total_attempts, last_error)
