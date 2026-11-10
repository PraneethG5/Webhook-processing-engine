# Polyglot Webhook Processing Engine (Part 2: Ingestion + Worker Pool)

An asynchronous webhook processing engine with a **Go Ingestion Gateway**, **Redis Streams**, and a concurrent **Python Worker Pool**.

## Progress: Milestone 2 (~50% Complete)
- [x] **Go Ingestion Gateway:** HMAC-SHA256 signature verification (`crypto/subtle.ConstantTimeCompare`), per-tenant Token Bucket rate limiting, and Redis Stream buffering (`webhooks:stream`).
- [x] **Python Worker Pool:** Async consumer group reading from Redis Streams (`XREADGROUP`), auto-reclaiming pending messages from crashed workers (`XAUTOCLAIM`), idempotency check to avoid duplicate side effects, and outbound dispatch.
- [x] **End-to-End Delivery:** Outbound webhook dispatch with signature signing (`X-Signature-SHA256`, `X-Timestamp`), audit logging of every attempt in PostgreSQL.
- [x] **Mock Destination Receiver:** Local mock server on port 9000 to verify end-to-end receipt.
- [ ] *Coming in Part 3 & 4:* Prometheus telemetry, circuit breaker fine-tuning, DLQ replay CLI, and load testing scripts.

## How to Run

```bash
docker compose up --build
```

- **Go Gateway (Ingestion):** `http://localhost:8080/v1/webhooks`
- **Mock Destination (Receipts):** `http://localhost:9000/deliveries`
