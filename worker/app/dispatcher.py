import hmac
import hashlib
import time
import json
import logging
from typing import Dict, Any, Tuple
import aiohttp
from .models import TenantConfig, QueuedEvent, DeliveryResult
from .circuit_breaker import CircuitBreakerRegistry, CircuitBreakerOpenException

logger = logging.getLogger(__name__)

class WebhookDispatcher:
    def __init__(self, circuit_breakers: CircuitBreakerRegistry):
        self.circuit_breakers = circuit_breakers
        self.session: aiohttp.ClientSession | None = None

    async def init_session(self):
        timeout = aiohttp.ClientTimeout(total=8, connect=3)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def transform_payload(self, event: QueuedEvent) -> Dict[str, Any]:
        """
        Extensible hook for schema transformation, metadata enrichment, or filtering.
        """
        return {
            "id": event.event_id,
            "type": event.event_type,
            "created_at": event.received_at,
            "data": event.payload,
            "_metadata": {
                "engine": "polyglot-webhook-v1",
                "attempt": event.attempt_count + 1
            }
        }

    def sign_outbound_payload(self, body_bytes: bytes, secret: str) -> Tuple[str, str]:
        """
        Signs the outbound webhook using HMAC-SHA256 with timestamp to prevent replay attacks.
        Format: timestamp, hex_signature
        """
        timestamp = str(int(time.time()))
        message_to_sign = f"{timestamp}.".encode("utf-8") + body_bytes
        signature = hmac.new(
            secret.encode("utf-8"),
            message_to_sign,
            hashlib.sha256
        ).hexdigest()
        return timestamp, signature

    async def dispatch(self, tenant: TenantConfig, event: QueuedEvent) -> Tuple[DeliveryResult, Dict[str, str]]:
        assert self.session is not None
        breaker = self.circuit_breakers.get_breaker(tenant.destination_url)

        if not breaker.can_attempt():
            msg = f"Circuit breaker is OPEN for {tenant.destination_url}. Fast-failing delivery."
            logger.warning(msg)
            return DeliveryResult(
                success=False,
                status_code=503,
                error_message=msg,
                latency_ms=0
            ), {}

        transformed = self.transform_payload(event)
        body_str = json.dumps(transformed)
        body_bytes = body_str.encode("utf-8")

        timestamp, signature = self.sign_outbound_payload(body_bytes, tenant.outbound_secret)

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Polyglot-Webhook-Engine/1.0",
            "X-Event-ID": event.event_id,
            "X-Tenant-ID": tenant.id,
            "X-Timestamp": timestamp,
            "X-Signature-SHA256": f"t={timestamp},v1={signature}",
            "X-Delivery-Attempt": str(event.attempt_count + 1)
        }
        if event.idempotency_key:
            headers["X-Idempotency-Key"] = event.idempotency_key

        start_time = time.time()
        try:
            async with self.session.post(tenant.destination_url, data=body_bytes, headers=headers) as resp:
                latency_ms = int((time.time() - start_time) * 1000)
                resp_text = await resp.text()
                # Consider 2xx as success
                success = 200 <= resp.status < 300

                if success:
                    breaker.record_success()
                    return DeliveryResult(
                        success=True,
                        status_code=resp.status,
                        response_body=resp_text[:500],
                        latency_ms=latency_ms
                    ), headers
                else:
                    # 4xx (except 429) usually indicates client-side bad payload; 5xx indicates server failure
                    if resp.status >= 500 or resp.status == 429:
                        breaker.record_failure()
                    return DeliveryResult(
                        success=False,
                        status_code=resp.status,
                        response_body=resp_text[:500],
                        error_message=f"HTTP {resp.status} received from destination",
                        latency_ms=latency_ms
                    ), headers

        except Exception as e:
            latency_ms = int((time.time() - start_time) * 1000)
            breaker.record_failure()
            logger.error(f"Network error delivering event {event.event_id} to {tenant.destination_url}: {e}")
            return DeliveryResult(
                success=False,
                error_message=str(e),
                latency_ms=latency_ms
            ), headers
