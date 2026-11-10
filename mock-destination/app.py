import hmac
import hashlib
import time
import logging
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s [MOCK-RECEIVER] %(message)s")
logger = logging.getLogger("mock-receiver")

routes = web.RouteTableDef()
received_deliveries = []

# Known secret for Acme Corp (from 001_init.sql)
KNOWN_SECRETS = {
    "11111111-1111-1111-1111-111111111111": "outbound_secret_acme_xyz",
    "22222222-2222-2222-2222-222222222222": "outbound_secret_globex_abc"
}

@routes.post("/webhook")
async def handle_webhook(request: web.Request):
    body_bytes = await request.read()
    headers = dict(request.headers)
    
    event_id = headers.get("X-Event-ID", "unknown")
    tenant_id = headers.get("X-Tenant-ID", "unknown")
    timestamp_header = headers.get("X-Timestamp")
    sig_header = headers.get("X-Signature-SHA256")
    attempt = headers.get("X-Delivery-Attempt", "1")

    # 1. Outbound Signature Verification
    secret = KNOWN_SECRETS.get(tenant_id)
    sig_valid = False
    if secret and timestamp_header and sig_header:
        try:
            # Format: t=timestamp,v1=hex_signature
            parts = dict(item.split("=", 1) for item in sig_header.split(","))
            expected_msg = f"{parts.get('t')}.".encode("utf-8") + body_bytes
            expected_sig = hmac.new(secret.encode("utf-8"), expected_msg, hashlib.sha256).hexdigest()
            sig_valid = hmac.compare_digest(parts.get("v1", ""), expected_sig)
        except Exception as e:
            logger.warning(f"Signature check exception: {e}")

    logger.info(
        f"Received Webhook -> Event: {event_id} | Tenant: {tenant_id} | Attempt: #{attempt} | Signature Valid: {sig_valid}"
    )

    # 2. Check for simulation controls
    # A) Query param or payload fail trigger
    query = request.query
    if query.get("fail") == "true" or b'"trigger_fail": true' in body_bytes or b'"trigger_fail":true' in body_bytes:
        logger.warning(f"Simulating destination failure (HTTP 500) for event {event_id}")
        return web.json_response({"error": "Simulated upstream failure"}, status=500)

    # B) Simulated latency
    if "delay" in query:
        delay_sec = float(query["delay"])
        time.sleep(delay_sec)

    record = {
        "event_id": event_id,
        "tenant_id": tenant_id,
        "attempt": attempt,
        "signature_valid": sig_valid,
        "received_at": time.time(),
        "payload": (await request.text())[:300]
    }
    received_deliveries.append(record)
    if len(received_deliveries) > 100:
        received_deliveries.pop(0)

    return web.json_response({
        "status": "received",
        "event_id": event_id,
        "signature_valid": sig_valid,
        "acknowledged": True
    }, status=200)

@routes.get("/deliveries")
async def get_deliveries(request: web.Request):
    return web.json_response({
        "total_received": len(received_deliveries),
        "deliveries": list(reversed(received_deliveries))
    })

@routes.get("/healthz")
async def health(request: web.Request):
    return web.json_response({"status": "ok"})

app = web.Application()
app.add_routes(routes)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=9000)
