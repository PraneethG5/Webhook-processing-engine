#!/usr/bin/env python3
import asyncio
import hmac
import hashlib
import json
import time
import uuid
import argparse
import aiohttp

TENANT_ALPHA = {
    "id": "11111111-1111-1111-1111-111111111111",
    "secret": "sec_inbound_alpha_9f8e7d"
}

TENANT_BETA = {
    "id": "22222222-2222-2222-2222-222222222222",
    "secret": "sec_inbound_beta_4b5c6d"
}

def sign_payload(payload_bytes: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload_bytes, hashlib.sha256).hexdigest()

async def send_single_webhook(
    session: aiohttp.ClientSession,
    gateway_url: str,
    tenant: dict,
    trigger_fail: bool = False,
    idempotency_key: str = None
) -> dict:
    payload = {
        "event": "order.payment_completed",
        "order_id": str(uuid.uuid4()),
        "amount": 99.50,
        "currency": "USD",
        "customer": {"email": "alex@example.com"},
        "trigger_fail": trigger_fail
    }
    payload_bytes = json.dumps(payload).encode("utf-8")
    sig = sign_payload(payload_bytes, tenant["secret"])

    headers = {
        "Content-Type": "application/json",
        "X-Tenant-ID": tenant["id"],
        "X-Signature-SHA256": sig,
        "X-Event-Type": "order.completed"
    }
    if idempotency_key:
        headers["X-Idempotency-Key"] = idempotency_key

    t0 = time.time()
    try:
        async with session.post(f"{gateway_url}/v1/webhooks", data=payload_bytes, headers=headers) as resp:
            latency_ms = (time.time() - t0) * 1000
            resp_body = await resp.text()
            return {
                "status": resp.status,
                "latency_ms": latency_ms,
                "body": resp_body
            }
    except Exception as e:
        return {
            "status": 0,
            "latency_ms": (time.time() - t0) * 1000,
            "error": str(e)
        }

async def run_benchmark(gateway_url: str, total_requests: int, concurrency: int, tenant: dict, trigger_fail: bool):
    print(f"target: {gateway_url} | tenant: {tenant['id']}")
    print(f"requests: {total_requests} | concurrency: {concurrency} | simulate_failure: {trigger_fail}")
    print("-" * 65)

    conn = aiohttp.TCPConnector(limit=concurrency, limit_per_host=concurrency)
    async with aiohttp.ClientSession(connector=conn) as session:
        queue = asyncio.Queue()
        for _ in range(total_requests):
            queue.put_nowait(None)

        results = []

        async def worker():
            while not queue.empty():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                res = await send_single_webhook(session, gateway_url, tenant, trigger_fail=trigger_fail)
                results.append(res)
                queue.task_done()

        start_time = time.time()
        workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
        await asyncio.gather(*workers)
        total_time = time.time() - start_time

    status_counts = {}
    latencies = []
    for r in results:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1
        latencies.append(r["latency_ms"])

    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)] if latencies else 0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
    rps = len(results) / total_time if total_time > 0 else 0

    print(f"duration:   {total_time:.2f}s")
    print(f"throughput: {rps:.1f} req/s")
    print(f"p50:        {p50:.2f}ms")
    print(f"p95:        {p95:.2f}ms")
    print(f"p99:        {p99:.2f}ms")
    print(f"statuses:   {status_counts}")
    print("-" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="webhook load tester")
    parser.add_argument("--url", default="http://localhost:8080", help="gateway url")
    parser.add_argument("-n", "--total", type=int, default=500, help="total requests")
    parser.add_argument("-c", "--concurrency", type=int, default=25, help="concurrent connections")
    parser.add_argument("--tenant", choices=["alpha", "beta"], default="alpha", help="target tenant")
    parser.add_argument("--fail", action="store_true", help="trigger failure to test retries & dlq")

    args = parser.parse_args()
    chosen_tenant = TENANT_ALPHA if args.tenant == "alpha" else TENANT_BETA
    asyncio.run(run_benchmark(args.url, args.total, args.concurrency, chosen_tenant, args.fail))
