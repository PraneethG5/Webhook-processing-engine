import hmac
import hashlib
import time
import logging
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("mock-receiver")

routes = web.RouteTableDef()
received_deliveries = []

TENANT_SECRETS = {
    "11111111-1111-1111-1111-111111111111": "sec_outbound_alpha_1a2b3c",
    "22222222-2222-2222-2222-222222222222": "sec_outbound_beta_7e8f9a"
}

@routes.post("/webhook")
async def handle_webhook(request: web.Request):
    body_bytes = await request.read()
    headers = dict(request.headers)
    
    event_id = headers.get("X-Event-ID", "")
    tenant_id = headers.get("X-Tenant-ID", "")
    timestamp_header = headers.get("X-Timestamp")
    sig_header = headers.get("X-Signature-SHA256")
    attempt = headers.get("X-Delivery-Attempt", "1")

    secret = TENANT_SECRETS.get(tenant_id)
    sig_valid = False
    if secret and timestamp_header and sig_header:
        try:
            parts = dict(item.split("=", 1) for item in sig_header.split(","))
            expected_msg = f"{parts.get('t')}.".encode("utf-8") + body_bytes
            expected_sig = hmac.new(secret.encode("utf-8"), expected_msg, hashlib.sha256).hexdigest()
            sig_valid = hmac.compare_digest(parts.get("v1", ""), expected_sig)
        except Exception as e:
            logger.warning(f"signature parse error: {e}")

    logger.info(
        f"event={event_id} tenant={tenant_id} attempt={attempt} verified={sig_valid}"
    )

    query = request.query
    if query.get("fail") == "true" or b'"trigger_fail": true' in body_bytes or b'"trigger_fail":true' in body_bytes:
        return web.json_response({"error": "simulated endpoint failure"}, status=500)

    if "delay" in query:
        time.sleep(float(query["delay"]))

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
        "status": "ok",
        "event_id": event_id,
        "signature_valid": sig_valid
    }, status=200)

@routes.get("/deliveries")
async def get_deliveries(request: web.Request):
    return web.json_response({
        "count": len(received_deliveries),
        "deliveries": list(reversed(received_deliveries))
    })

@routes.get("/healthz")
async def health(request: web.Request):
    return web.json_response({"status": "ok"})

app = web.Application()
app.add_routes(routes)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=9000)
