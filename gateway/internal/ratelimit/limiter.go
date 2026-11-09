package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketLuaScript performs atomic token replenishment and consumption
// KEYS[1]: ratelimit key (e.g. ratelimit:tenant:<id>)
// ARGV[1]: capacity (burst limit)
// ARGV[2]: refill rate (tokens per second)
// ARGV[3]: current timestamp in milliseconds
// ARGV[4]: requested tokens (typically 1)
//
// Returns array: [allowed (1 or 0), remaining_tokens, retry_after_ms]
const tokenBucketLuaScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

-- Get current state
local data = redis.call("HMGET", key, "tokens", "last_updated")
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if not tokens then
    tokens = capacity
    last_updated = now
else
    -- Calculate tokens replenished since last update
    local elapsed_seconds = math.max(0, (now - last_updated) / 1000.0)
    tokens = math.min(capacity, tokens + (elapsed_seconds * refill_rate))
    last_updated = now
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    -- Expire bucket if idle for 2 * (capacity / refill_rate) seconds (minimum 60s)
    local ttl = math.max(60, math.ceil(2 * (capacity / refill_rate)))
    redis.call("EXPIRE", key, ttl)
    return {1, math.floor(tokens), 0}
else
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    local missing = requested - tokens
    local retry_after_ms = math.ceil((missing / refill_rate) * 1000)
    return {0, math.floor(tokens), retry_after_ms}
end
`

// TokenBucketLimiter manages per-tenant rate limits backed by Redis
type TokenBucketLimiter struct {
	client     *redis.Client
	scriptHash string
}

// Result contains the rate limit decision
type Result struct {
	Allowed         bool
	RemainingTokens int
	RetryAfter      time.Duration
}

// NewTokenBucketLimiter compiles the Lua script into Redis script cache
func NewTokenBucketLimiter(ctx context.Context, client *redis.Client) (*TokenBucketLimiter, error) {
	script := redis.NewScript(tokenBucketLuaScript)
	// Pre-load script SHA for zero-overhead EVALSHA
	hash, err := script.Load(ctx, client).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to load token bucket lua script: %w", err)
	}
	return &TokenBucketLimiter{
		client:     client,
		scriptHash: hash,
	}, nil
}

// Allow checks if a tenant is allowed to perform a request
func (tbl *TokenBucketLimiter) Allow(ctx context.Context, tenantID string, ratePerSec, burstLimit int) (Result, error) {
	if ratePerSec <= 0 {
		ratePerSec = 50
	}
	if burstLimit <= 0 {
		burstLimit = ratePerSec * 2
	}

	key := fmt.Sprintf("ratelimit:tenant:%s", tenantID)
	nowMs := time.Now().UnixNano() / int64(time.Millisecond)

	res, err := tbl.client.EvalSha(ctx, tbl.scriptHash, []string{key}, burstLimit, ratePerSec, nowMs, 1).Result()
	if err != nil {
		// Fallback to plain Eval if script wasn't cached on replica or restart
		script := redis.NewScript(tokenBucketLuaScript)
		res, err = script.Run(ctx, tbl.client, []string{key}, burstLimit, ratePerSec, nowMs, 1).Result()
		if err != nil {
			return Result{Allowed: false}, fmt.Errorf("rate limit evaluation failed: %w", err)
		}
	}

	values, ok := res.([]interface{})
	if !ok || len(values) < 3 {
		return Result{Allowed: false}, fmt.Errorf("unexpected lua return format: %v", res)
	}

	allowed := values[0].(int64) == 1
	remaining := int(values[1].(int64))
	retryAfterMs := values[2].(int64)

	return Result{
		Allowed:         allowed,
		RemainingTokens: remaining,
		RetryAfter:      time.Duration(retryAfterMs) * time.Millisecond,
	}, nil
}
