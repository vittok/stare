from uuid import UUID

import httpx
from fastapi import Header, HTTPException

from .config import get_settings


async def require_user_id(authorization: str | None = Header(default=None)) -> UUID:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    settings = get_settings()
    if not settings.supabase_url or not settings.supabase_publishable_key:
        raise HTTPException(status_code=503, detail="Supabase Auth is not configured")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                f"{settings.supabase_url.rstrip('/')}/auth/v1/user",
                headers={
                    "apikey": settings.supabase_publishable_key,
                    "authorization": authorization,
                },
            )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="Authentication service unavailable") from exc

    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="Invalid or expired access token")

    try:
        return UUID(response.json()["id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Invalid authenticated user") from exc
