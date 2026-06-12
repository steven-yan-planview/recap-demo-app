"""
Redis-backed token bucket rate limiter.

Replaces the in-process sliding window limiter for multi-replica deployments.
Counters are stored in Redis with a per-tenant TTL so state is shared across
all API pods. Falls back to allowing the request if Redis is unavailable
(fail-open policy — acceptable for ingestion; revisit for auth endpoints).

Algorithm: token bucket with atomic Lua script to avoid TOCTOU races.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

import redis.asyncio as aioredis

from src.config import settings

logger = logging.getLogger(__name__)

# Lua script: atomically refill + consume one token.
# Returns {allowed: 0|1, remaining: int}
_LUA_TOKEN_BUCKET = """
local key        = KEYS[1]
local capacity   = tonumber(ARGV[1])
local rate       = tonumber(ARGV[2])   -- tokens per second
local now        = tonumber(ARGV[3])   -- unix timestamp (float)
local ttl        = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens     = tonumber(data[1]) or capacity
local last_refill = tonumber(data[2]) or now

-- Refill
local elapsed = now - last_refill
local refilled = math.min(capacity, tokens + elapsed * rate)

-- Consume
local allowed = 0
local remaining = 0
if refilled >= 1 then
    refilled  = refilled - 1
    allowed   = 1
    remaining = math.floor(refilled)
end

redis.call('HMSET', key, 'tokens', refilled, 'last_refill', now)
redis.call('EXPIRE', key, ttl)

return {allowed, remaining}
"""


class RedisTokenBucketLimiter:
    """
    Distributed token bucket rate limiter backed by Redis.

    Parameters
    ----------
    redis_url:
        Connection URL, e.g. ``redis://localhost:6379/0``.
    capacity:
        Maximum burst size (tokens).
    rate_per_minute:
        Sustained throughput — converted to tokens/second internally.
    key_prefix:
        Namespace for Redis keys; defaults to ``rl:``.
    """

    def __init__(
        self,
        redis_url: str,
        capacity: int = 1000,
        rate_per_minute: int = 1000,
        key_prefix: str = "rl:",
    ) -> None:
        self._client: Optional[aioredis.Redis] = None
        self._redis_url = redis_url
        self._capacity = capacity
        self._rate = rate_per_minute / 60.0  # tokens per second
        self._ttl = 120  # seconds; 2× the refill window
        self._prefix = key_prefix
        self._script: Optional[aioredis.client.Script] = None

    async def _get_client(self) -> aioredis.Redis:
        if self._client is None:
            self._client = await aioredis.from_url(
                self._redis_url, encoding="utf-8", decode_responses=True
            )
            self._script = self._client.register_script(_LUA_TOKEN_BUCKET)
        return self._client

    async def is_allowed(self, tenant_id: str) -> tuple[bool, int]:
        """
        Returns ``(allowed, remaining_tokens)``.

        On Redis error the call is fail-open: returns ``(True, -1)``.
        """
        key = f"{self._prefix}{tenant_id}"
        try:
            client = await self._get_client()
            result = await self._script(  # type: ignore[misc]
                keys=[key],
                args=[
                    self._capacity,
                    self._rate,
                    time.time(),
                    self._ttl,
                ],
                client=client,
            )
            allowed, remaining = int(result[0]), int(result[1])
            return bool(allowed), remaining
        except Exception as exc:
            logger.error("Redis rate limiter error (fail-open): %s", exc)
            return True, -1


# Module-level singleton — shared across all FastAPI worker coroutines.
redis_limiter = RedisTokenBucketLimiter(
    redis_url=getattr(settings, "redis_url", "redis://localhost:6379/0"),
    capacity=getattr(settings, "rate_limit_burst", 1000),
    rate_per_minute=getattr(settings, "rate_limit_per_minute", 1000),
)
