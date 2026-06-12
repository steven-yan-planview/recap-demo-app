"""
Per-tenant rate limiting for the ingestion API.

Keyed on the JWT `tenant` claim extracted by AuthMiddleware.
Uses a sliding window algorithm backed by an in-process deque.
Limit and window are configurable via environment variables.
"""
from __future__ import annotations
import time
import logging
from collections import defaultdict, deque
from fastapi import Request, HTTPException

from src.config import settings

logger = logging.getLogger(__name__)


class SlidingWindowRateLimiter:
    """
    In-process sliding window rate limiter.

    Not suitable for multi-replica deployments — use Redis-backed
    limiter (tracked separately) for horizontal scaling.
    """

    def __init__(self, limit: int, window_seconds: int):
        self.limit = limit
        self.window = window_seconds
        self._buckets: dict[str, deque[float]] = defaultdict(deque)

    def is_allowed(self, key: str) -> tuple[bool, int]:
        """
        Returns (allowed, remaining).
        Side-effect: records the current request timestamp.
        """
        now = time.monotonic()
        bucket = self._buckets[key]

        # Evict timestamps outside the window
        cutoff = now - self.window
        while bucket and bucket[0] < cutoff:
            bucket.popleft()

        remaining = self.limit - len(bucket)
        if remaining <= 0:
            return False, 0

        bucket.append(now)
        return True, remaining - 1


_limiter = SlidingWindowRateLimiter(
    limit=getattr(settings, "rate_limit_per_minute", 1000),
    window_seconds=60,
)


async def enforce_rate_limit(request: Request) -> None:
    """
    FastAPI dependency — raises 429 if the tenant exceeds their rate limit.
    Expects request.state.tenant to be set by AuthMiddleware.
    """
    tenant = getattr(request.state, "tenant", "anonymous")
    allowed, remaining = _limiter.is_allowed(tenant)

    if not allowed:
        logger.warning("Rate limit exceeded for tenant %s", tenant)
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": "60"},
        )

    request.state.rate_limit_remaining = remaining
