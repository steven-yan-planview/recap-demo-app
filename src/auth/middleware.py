import httpx
import logging
from jose import jwt, JWTError
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from src.config import settings

logger = logging.getLogger(__name__)

_jwks_cache: dict = {}


async def _fetch_jwks() -> dict:
    global _jwks_cache
    if _jwks_cache:
        return _jwks_cache
    async with httpx.AsyncClient() as client:
        resp = await client.get(settings.jwks_uri)
        resp.raise_for_status()
        _jwks_cache = resp.json()
    return _jwks_cache


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path in ("/health", "/metrics"):
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing bearer token")

        token = auth_header.removeprefix("Bearer ")
        try:
            jwks = await _fetch_jwks()
            payload = jwt.decode(
                token,
                jwks,
                algorithms=["RS256"],
                audience=settings.jwt_audience,
            )
        except JWTError as exc:
            logger.warning("JWT validation failed: %s", exc)
            raise HTTPException(status_code=401, detail="Invalid token")

        request.state.sub = payload["sub"]
        request.state.tenant = payload["tenant"]
        request.state.scope = payload.get("scope", "")

        return await call_next(request)
