"""
OAuth 2.0 PKCE authorization flow for interactive clients.

Implements RFC 7636 (PKCE) on top of the authorization code flow.
Used by the data platform dashboard and CLI tool for human-in-the-loop
access — not for service-to-service calls (those use client_credentials
via the existing JWT middleware).

Flow:
    1. Client calls /auth/authorize to get redirect URL + code_verifier
    2. User authenticates at the IdP (Okta)
    3. IdP redirects to /auth/callback with authorization code
    4. Callback exchanges code + verifier for tokens
    5. Access token returned to client; used as Bearer in subsequent calls
"""
from __future__ import annotations
import base64
import hashlib
import os
import time
import logging
import httpx
from urllib.parse import urlencode

from src.config import settings

logger = logging.getLogger(__name__)

# In-memory state store — not suitable for multi-replica.
# Replace with Redis for production horizontal scaling.
_pending_states: dict[str, dict] = {}
STATE_TTL = 300  # seconds


def _generate_code_verifier() -> str:
    return base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode()


def _code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def build_authorization_url(redirect_uri: str) -> tuple[str, str, str]:
    """
    Returns (authorization_url, state, code_verifier).
    Caller must store code_verifier server-side keyed on state.
    """
    state = base64.urlsafe_b64encode(os.urandom(16)).rstrip(b"=").decode()
    verifier = _generate_code_verifier()
    challenge = _code_challenge(verifier)

    _pending_states[state] = {"verifier": verifier, "created_at": time.monotonic()}

    params = {
        "response_type": "code",
        "client_id": settings.oauth_client_id,
        "redirect_uri": redirect_uri,
        "scope": "openid profile email",
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    url = f"{settings.oauth_authorization_endpoint}?{urlencode(params)}"
    return url, state, verifier


async def exchange_code(code: str, state: str, redirect_uri: str) -> dict:
    """
    Exchange authorization code for tokens. Validates state and PKCE verifier.
    Returns the token response from the IdP.
    """
    pending = _pending_states.pop(state, None)
    if pending is None:
        raise ValueError("Unknown or expired state parameter")

    if time.monotonic() - pending["created_at"] > STATE_TTL:
        raise ValueError("Authorization state expired")

    verifier = pending["verifier"]
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            settings.oauth_token_endpoint,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": settings.oauth_client_id,
                "code_verifier": verifier,
            },
        )
        resp.raise_for_status()

    logger.info("OAuth2 PKCE token exchange successful for state %s", state[:8])
    return resp.json()


def _prune_stale_states() -> None:
    now = time.monotonic()
    stale = [k for k, v in _pending_states.items() if now - v["created_at"] > STATE_TTL]
    for k in stale:
        del _pending_states[k]
    if stale:
        logger.debug("Pruned %d stale OAuth states", len(stale))
