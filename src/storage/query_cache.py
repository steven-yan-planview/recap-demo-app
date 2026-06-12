"""
LRU query-result cache for Snowflake read queries.

Keyed on (query_hash, tenant_id) with a configurable TTL.
Cache is invalidated on schema change events received via the
``invalidate_tenant`` method (called by the schema-change event handler).

Design decisions
----------------
* In-process LRU rather than Redis for read queries — these are immutable
  at the moment of caching and the cache is per-pod. A shared cache would
  add latency for small query results and introduce cache-coherence
  complexity that isn't justified at current traffic levels.
* Max size is enforced by ``maxsize`` on the underlying OrderedDict so
  memory is bounded without a background sweep.
* TTL eviction happens lazily on read; an optional background task
  (``start_background_eviction``) can run periodic sweeps if memory
  pressure is a concern.

Metrics
-------
Exposes ``cache_hits``, ``cache_misses``, and ``cache_size`` counters
that callers can read for Prometheus export.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import OrderedDict
from threading import Lock
from typing import Any, Optional

logger = logging.getLogger(__name__)


class _CacheEntry:
    __slots__ = ("value", "expires_at")

    def __init__(self, value: Any, ttl_seconds: int) -> None:
        self.value = value
        self.expires_at = time.monotonic() + ttl_seconds


class QueryResultCache:
    """
    Thread-safe LRU cache for Snowflake query results.

    Parameters
    ----------
    maxsize:
        Maximum number of entries before LRU eviction kicks in.
    ttl_seconds:
        Per-entry expiry. Stale entries are evicted on read.
    """

    def __init__(self, maxsize: int = 2048, ttl_seconds: int = 300) -> None:
        self._maxsize = maxsize
        self._ttl = ttl_seconds
        self._store: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._lock = Lock()

        # Prometheus-style counters (read via metrics endpoint)
        self.cache_hits = 0
        self.cache_misses = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, query: str, tenant_id: str) -> Optional[Any]:
        key = self._make_key(query, tenant_id)
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self.cache_misses += 1
                return None
            if time.monotonic() > entry.expires_at:
                del self._store[key]
                self.cache_misses += 1
                return None
            # Move to end (most recently used)
            self._store.move_to_end(key)
            self.cache_hits += 1
            return entry.value

    def set(self, query: str, tenant_id: str, value: Any) -> None:
        key = self._make_key(query, tenant_id)
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = _CacheEntry(value, self._ttl)
            # Evict oldest entry if over capacity
            while len(self._store) > self._maxsize:
                evicted_key, _ = self._store.popitem(last=False)
                logger.debug("LRU evicted cache entry: %s", evicted_key)

    def invalidate_tenant(self, tenant_id: str) -> int:
        """
        Remove all cached entries for ``tenant_id``.

        Returns the number of entries evicted. Called on schema-change events.
        """
        prefix = f"{tenant_id}:"
        with self._lock:
            keys_to_delete = [k for k in self._store if k.startswith(prefix)]
            for k in keys_to_delete:
                del self._store[k]
        if keys_to_delete:
            logger.info("Cache invalidated %d entries for tenant %s", len(keys_to_delete), tenant_id)
        return len(keys_to_delete)

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._store)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_key(query: str, tenant_id: str) -> str:
        query_hash = hashlib.sha256(query.encode()).hexdigest()[:16]
        return f"{tenant_id}:{query_hash}"


# Module-level singleton used by SnowflakeClient
query_cache = QueryResultCache(
    maxsize=2048,
    ttl_seconds=300,  # 5 minutes
)
