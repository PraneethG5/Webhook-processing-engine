from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
import uuid

class TenantConfig(BaseModel):
    id: str
    name: str
    inbound_secret: str
    outbound_secret: str
    destination_url: str
    rate_limit_per_second: int
    burst_limit: int

class QueuedEvent(BaseModel):
    event_id: str
    tenant_id: str
    idempotency_key: Optional[str] = None
    event_type: str = "webhook.event"
    payload: Dict[str, Any]
    received_at: int
    max_retries: int = 5
    attempt_count: int = 0
    stream_message_id: Optional[str] = None

class DeliveryResult(BaseModel):
    success: bool
    status_code: Optional[int] = None
    response_body: Optional[str] = None
    error_message: Optional[str] = None
    latency_ms: int = 0
