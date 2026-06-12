"""
OAuth 2.0 PKCE endpoints mounted at /auth.
These routes are excluded from JWT middleware (see main.py exemptions).
"""
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from src.auth.oauth2 import build_authorization_url, exchange_code

router = APIRouter(prefix="/auth")

# In production this would come from config / env
_REDIRECT_URI = "http://localhost:8080/auth/callback"


@router.get("/authorize")
async def authorize():
    """
    Start the PKCE flow. Returns the IdP redirect URL.
    """
    url, state, _verifier = build_authorization_url(_REDIRECT_URI)
    return {"authorization_url": url, "state": state}


@router.get("/callback")
async def callback(code: str, state: str):
    """
    IdP redirects here after user authentication.
    Exchanges code for tokens and returns them to the client.
    """
    try:
        tokens = await exchange_code(code, state, _REDIRECT_URI)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return tokens
