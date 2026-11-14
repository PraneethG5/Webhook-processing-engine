#!/usr/bin/env python3
import sys
import argparse
import asyncio
import os
import asyncpg
import redis.asyncio as aioredis
import json

DB_URL = os.getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/webhooks?sslmode=disable")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

async def list_dlq(conn):
    rows = await conn.fetch("""
        SELECT d.event_id, d.tenant_id, d.failure_reason, d.total_attempts, d.last_error, d.created_at, d.replayed_at
        FROM dead_letter_events d
        ORDER BY d.created_at DESC
        LIMIT 50
    """)
    if not rows:
        print("no dead-letter events found")
        return

    print(f"{'Event ID':<38} | {'Tenant ID':<38} | {'Attempts':<8} | {'Replayed':<10} | {'Reason'}")
    print("-" * 110)
    for r in rows:
        replayed = "yes" if r["replayed_at"] else "no"
        print(f"{str(r['event_id']):<38} | {str(r['tenant_id']):<38} | {r['total_attempts']:<8} | {replayed:<10} | {r['failure_reason']}")

async def replay_event(conn, redis_client, event_id):
    row = await conn.fetchrow("""
        SELECT e.id, e.tenant_id, e.idempotency_key, e.event_type, e.payload, e.max_retries
        FROM webhook_events e
        WHERE e.id = $1
    """, event_id)

    if not row:
        print(f"event {event_id} not found")
        return

    await conn.execute("""
        UPDATE webhook_events
        SET status = 'REPLAYING', retry_count = 0, failed_at = NULL
        WHERE id = $1
    """, event_id)

    await conn.execute("""
        UPDATE dead_letter_events
        SET replayed_at = NOW()
        WHERE event_id = $1
    """, event_id)

    await redis_client.xadd("webhooks:stream", {
        "event_id": str(row["id"]),
        "tenant_id": str(row["tenant_id"]),
        "idempotency_key": row["idempotency_key"] or "",
        "event_type": row["event_type"],
        "payload": json.dumps(row["payload"]),
        "received_at": 0,
        "max_retries": row["max_retries"],
        "attempt_count": 0
    })

    print(f"re-queued event {event_id} into webhooks:stream")

async def main():
    parser = argparse.ArgumentParser(description="DLQ management utility")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="list dead-letter events")

    replay_parser = subparsers.add_parser("replay", help="replay dead-letter event by id")
    replay_parser.add_argument("event_id", type=str, help="event UUID")

    args = parser.parse_args()

    conn = await asyncpg.connect(DB_URL)
    redis_client = aioredis.from_url(REDIS_URL)

    try:
        if args.command == "list":
            await list_dlq(conn)
        elif args.command == "replay":
            await replay_event(conn, redis_client, args.event_id)
    finally:
        await conn.close()
        await redis_client.close()

if __name__ == "__main__":
    asyncio.run(main())
