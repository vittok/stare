from dataclasses import dataclass
from uuid import UUID

import httpx
from fastapi import Depends, Header, HTTPException

from .config import get_settings


@dataclass(frozen=True)
class AuthenticatedUser:
    id: UUID
    email: str | None


async def require_user(authorization: str | None = Header(default=None)) -> AuthenticatedUser:
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
        payload = response.json()
        return AuthenticatedUser(id=UUID(payload["id"]), email=payload.get("email"))
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Invalid authenticated user") from exc


async def require_user_id(user: AuthenticatedUser = Depends(require_user)) -> UUID:
    return user.id
