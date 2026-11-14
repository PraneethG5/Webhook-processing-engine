# Polyglot Webhook Processing Engine

An async webhook ingestion engine w tenant isolation.

## Architecture

- **Ingestion Gateway ** Receives inbound webhooks, verifies SHA256 signatures using constant-time comparison, evaluates tenant token bucket 
- **Message Broker:** Manages event queues with consumer groups, automatic crashed-worker reclamation (`XAUTOCLAIM`), and delayed retry scheduling via sorted sets (`ZSET`).
- **Worker Pool :** Asynchronously consumes queued events, enforces idempotency to prevent duplicate side effects, signs outbound webhooks with timestamped HMAC signatures, and logs delivery attempts to PostgreSQL.
- **Observability:** Prometheus metrics exported from both the go gatewayand Python worker 


```bash
docker compose up --build
```

Service endpoints:
- Ingestion Gateway: `http://localhost:8080/v1/webhooks`
- Gateway Health: `http://localhost:8080/healthz`
- Mock Destination: `http://localhost:9000/deliveries`
- Prometheus Dashboard: `http://localhost:9090`

## Verification & Toolin

### 1. Load Testing & Rate Limit Verification

```bash
python tools/load_test.py --url http://localhost:8080 -n 1000 -c 50 --tenant alpha

python tools/load_test.py --url http://localhost:8080 -n 500 -c 40 --tenant beta
```

### 2. Failure Simulation & DLQ Replay

```bash
# Push failing webhooks
python tools/load_test.py --url http://localhost:8080 -n 10 -c 2 --tenant alpha --fail

# Inspect dead-letter queue 
python tools/replay_dlq.py list

# Replay an event back into the primary processing stream
python tools/replay_dlq.py replay <event_id>
```
