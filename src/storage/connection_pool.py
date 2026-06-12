"""
Snowflake connection pool.

Simple semaphore-based pool. Connections are created eagerly at startup
and validated with SELECT 1 before checkout. Stale connections (idle > 5 min)
are evicted and replaced.

Fix (PDF-202): Track total open connections. The previous implementation could
open more connections than max_size under concurrent eviction pressure because
stale connections were closed but the replacement open() happened outside the
semaphore boundary. Under load this caused Snowflake to reject new connects
once it hit the per-service limit (128).
"""
from __future__ import annotations
import asyncio
import logging
import time
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

import snowflake.connector

from src.config import settings

logger = logging.getLogger(__name__)


@dataclass
class _PooledConn:
    conn: snowflake.connector.SnowflakeConnection
    last_used: float = field(default_factory=time.monotonic)


class ConnectionPool:
    IDLE_TIMEOUT = 300  # seconds

    def __init__(self):
        self._connections: list[_PooledConn] = []
        self._sem: asyncio.Semaphore | None = None
        self._lock = asyncio.Lock()
        self._total_open: int = 0  # tracks all open conns (pooled + checked out)

    async def initialize(self):
        self._sem = asyncio.Semaphore(settings.snowflake_pool_max)
        for _ in range(settings.snowflake_pool_min):
            conn = self._open_connection()
            self._connections.append(_PooledConn(conn=conn))
            self._total_open += 1
        logger.info(
            "Connection pool initialized: min=%d max=%d",
            settings.snowflake_pool_min,
            settings.snowflake_pool_max,
        )

    def _open_connection(self) -> snowflake.connector.SnowflakeConnection:
        return snowflake.connector.connect(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            password=settings.snowflake_password,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            warehouse=settings.snowflake_warehouse,
        )

    def _is_alive(self, pooled: _PooledConn) -> bool:
        if time.monotonic() - pooled.last_used > self.IDLE_TIMEOUT:
            return False
        try:
            pooled.conn.cursor().execute("SELECT 1")
            return True
        except Exception:
            return False

    @asynccontextmanager
    async def acquire(self):
        if self._sem is None:
            raise RuntimeError("Pool not initialized")

        async with self._sem:
            async with self._lock:
                pooled = self._get_or_create()
            try:
                yield pooled.conn
            finally:
                async with self._lock:
                    pooled.last_used = time.monotonic()
                    self._connections.append(pooled)

    def _get_or_create(self) -> _PooledConn:
        """
        Must be called with self._lock held.

        Drains stale connections from the front of the list, properly
        decrementing _total_open for each one closed. Only opens a new
        connection when _total_open < max_size (semaphore guarantees we
        have a slot, but doesn't cap the raw connection count).
        """
        while self._connections:
            pooled = self._connections.pop(0)
            if self._is_alive(pooled):
                return pooled
            # Stale — close and decrement before possibly opening a replacement
            logger.debug("Evicting stale connection (total_open=%d)", self._total_open)
            try:
                pooled.conn.close()
            except Exception:
                pass
            self._total_open -= 1

        # No live pooled connections available; open a new one.
        # _total_open tracks all open conns (pooled + checked-out),
        # so this is safe: semaphore ensures at most max_size concurrent
        # acquirers, but eviction can reduce _total_open below that.
        if self._total_open >= settings.snowflake_pool_max:
            raise RuntimeError(
                f"Connection pool exhausted: {self._total_open} open connections"
            )

        conn = self._open_connection()
        self._total_open += 1
        logger.debug("Opened new connection (total_open=%d)", self._total_open)
        return _PooledConn(conn=conn)

    async def close(self):
        for pooled in self._connections:
            try:
                pooled.conn.close()
            except Exception:
                pass
        self._connections.clear()
        self._total_open = 0


pool = ConnectionPool()
