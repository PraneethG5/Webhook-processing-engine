
A webhook ingestion service in GO

- HMAC-SHA256 Verification: Constant-time cryptographic signature validation (`crypto/subtle.ConstantTimeCompare`) to reject forged payloads.
- Per-Tenant Rate Limiting: Redis-backed atomic Token Bucket (Lua script) to eliminate noisy neighbors.

```bash
docker compose up --build
```

- Gateway: `http://localhost:8080/v1/webhooks`
- Health: `http://localhost:8080/healthz`
- Metrics: `http://localhost:8080/metrics`
