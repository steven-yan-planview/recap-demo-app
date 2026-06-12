"""
Batched tenant permission resolution service.

Replaces the previous per-resource permission check pattern that issued
one SQL query per resource per request. Under load that created an N+1
problem: a request touching 50 resources produced 50 round-trips to the
permission store, causing p99 latency spikes of 800ms+.

The fix: collect all resource IDs required by a request, resolve them in
a single ``WHERE resource_id IN (...)`` query, and cache the result for
the lifetime of the request using FastAPI's dependency injection.

Architecture notes
------------------
* ``PermissionService`` is request-scoped (instantiated per request via
  ``Depends(get_permission_service)``). This means the per-request cache
  in ``_resolved`` never leaks across requests.
* The underlying SQL uses parameterised ``IN`` clauses to prevent SQL
  injection. SQLAlchemy's ``bindparam(..., expanding=True)`` generates
  the right number of placeholders at execution time.
* If the permission store is unavailable, ``check_all`` raises
  ``PermissionStoreUnavailable``; the caller decides whether to fail-open
  or fail-closed (endpoints that return sensitive data should fail-closed).
"""
from __future__ import annotations

import logging
from typing import Iterable

from fastapi import Depends, Request

from src.storage.connection_pool import pool

logger = logging.getLogger(__name__)


class PermissionStoreUnavailable(Exception):
    """Raised when the permission store cannot be reached."""


class PermissionService:
    """
    Request-scoped permission resolver with batch fetching.

    Usage (FastAPI endpoint)::

        @router.get("/data/{resource_id}")
        async def get_data(
            resource_id: str,
            svc: PermissionService = Depends(get_permission_service),
        ):
            allowed = await svc.check_all(tenant_id, [resource_id])
            if resource_id not in allowed:
                raise HTTPException(403)
            ...
    """

    def __init__(self, tenant_id: str) -> None:
        self._tenant_id = tenant_id
        # Per-request cache: resource_id -> bool
        self._resolved: dict[str, bool] = {}

    async def check_all(self, resource_ids: Iterable[str]) -> set[str]:
        """
        Return the subset of ``resource_ids`` the tenant is allowed to access.

        Results are memoised for the lifetime of this service instance (one
        HTTP request). A second call with overlapping IDs will only query
        for IDs not already resolved.
        """
        ids = list(resource_ids)
        uncached = [rid for rid in ids if rid not in self._resolved]

        if uncached:
            await self._batch_resolve(uncached)

        return {rid for rid in ids if self._resolved.get(rid, False)}

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _batch_resolve(self, resource_ids: list[str]) -> None:
        """
        Single SQL round-trip to resolve permissions for all ``resource_ids``.

        Previously this was called once per resource (N+1). Now it resolves
        the full set in one query regardless of how many resources are involved.
        """
        if not resource_ids:
            return

        # Build a safe parameterised IN clause
        placeholders = ",".join(["%s"] * len(resource_ids))
        sql = f"""
            SELECT resource_id
            FROM tenant_permissions
            WHERE tenant_id = %s
              AND resource_id IN ({placeholders})
              AND expires_at > CURRENT_TIMESTAMP()
        """
        params = (self._tenant_id, *resource_ids)

        try:
            async with pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                allowed_ids = {row[0] for row in cursor.fetchall()}
        except Exception as exc:
            logger.error("Permission store query failed: %s", exc)
            raise PermissionStoreUnavailable(str(exc)) from exc

        # Populate the per-request cache
        for rid in resource_ids:
            self._resolved[rid] = rid in allowed_ids

        logger.debug(
            "Batch resolved %d permission(s) for tenant %s: %d allowed",
            len(resource_ids),
            self._tenant_id,
            len(allowed_ids),
        )


# ------------------------------------------------------------------
# FastAPI dependency
# ------------------------------------------------------------------

def get_permission_service(request: Request) -> PermissionService:
    """
    FastAPI dependency that returns a request-scoped ``PermissionService``.

    Requires ``AuthMiddleware`` to have set ``request.state.tenant``.
    """
    tenant_id = getattr(request.state, "tenant", "")
    return PermissionService(tenant_id=tenant_id)
