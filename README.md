# Polyglot Webhook Processing Engine 

An asynchronous webhook processing engine

 Go Ingestion Gateway: HMAC-SHA256 signature verification (`crypto/subtle.ConstantTimeCompare`), per-tenant Token Bucket rate limiting, and Redis Stream buffering (`webhooks:stream`).
## How to Run

```bash
docker compose up --build
```
