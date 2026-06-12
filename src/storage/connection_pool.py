"""
Snowflake connection pool.

Simple semaphore-based pool. Connections are created eagerly at startup
and validated with SELECT 1 before checkout. Stale connections (idle > 5 min)
are evicted and replaced.
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

    async def initialize(self):
        self._sem = asyncio.Semaphore(settings.snowflake_pool_max)
        for _ in range(settings.snowflake_pool_min):
            conn = self._open_connection()
            self._connections.append(_PooledConn(conn=conn))
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
        while self._connections:
            pooled = self._connections.pop(0)
            if self._is_alive(pooled):
                return pooled
            logger.debug("Evicting stale connection")
            try:
                pooled.conn.close()
            except Exception:
                pass
        conn = self._open_connection()
        return _PooledConn(conn=conn)

    async def close(self):
        for pooled in self._connections:
            try:
                pooled.conn.close()
            except Exception:
                pass
        self._connections.clear()


pool = ConnectionPool()
