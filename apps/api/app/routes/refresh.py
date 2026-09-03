import asyncio
import time
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, status

from ..auth import AuthenticatedUser, require_user
from ..config import get_settings

router = APIRouter(prefix="/api", tags=["updates"])

_dispatch_lock = asyncio.Lock()
_last_dispatch_at = 0.0


def _workflow_url(repository: str, workflow: str) -> str:
    owner, repo = repository.split("/", 1)
    workflow_id = quote(workflow, safe="")
    return f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow_id}"


@router.post("/refresh", status_code=status.HTTP_202_ACCEPTED)
async def refresh_market_data(user: AuthenticatedUser = Depends(require_user)) -> dict[str, str]:
    global _last_dispatch_at

    settings = get_settings()
    allowed_emails = settings.refresh_allowed_email_list
    user_email = (user.email or "").lower()

    if not allowed_emails:
        raise HTTPException(status_code=503, detail="Refresh access is not configured")
    if user_email not in allowed_emails:
        raise HTTPException(status_code=403, detail="Your account cannot start market updates")
    if not settings.github_actions_token:
        raise HTTPException(status_code=503, detail="GitHub Actions access is not configured")
    if "/" not in settings.github_repository:
        raise HTTPException(status_code=503, detail="GitHub repository configuration is invalid")

    workflow_url = _workflow_url(settings.github_repository, settings.github_workflow)
    headers = {
        "accept": "application/vnd.github+json",
        "authorization": f"Bearer {settings.github_actions_token}",
        "x-github-api-version": "2022-11-28",
    }

    async with _dispatch_lock:
        if time.monotonic() - _last_dispatch_at < 60:
            return {
                "status": "already_running",
                "message": "A market update was requested recently and is starting.",
            }

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                runs_response = await client.get(
                    f"{workflow_url}/runs",
                    headers=headers,
                    params={"branch": "main", "per_page": 10},
                )
                runs_response.raise_for_status()
                active_run = next(
                    (
                        run
                        for run in runs_response.json().get("workflow_runs", [])
                        if run.get("status") in {"queued", "in_progress", "waiting", "requested", "pending"}
                    ),
                    None,
                )

                if active_run:
                    return {
                        "status": "already_running",
                        "message": "A market update is already in progress.",
                        "workflow_run_url": active_run.get("html_url", ""),
                    }

                dispatch_response = await client.post(
                    f"{workflow_url}/dispatches",
                    headers=headers,
                    json={"ref": "main"},
                )
                dispatch_response.raise_for_status()
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            raise HTTPException(status_code=502, detail="The market update could not be started") from exc

        _last_dispatch_at = time.monotonic()

    return {
        "status": "queued",
        "message": "The market update has been queued. Fresh data will appear automatically when it completes.",
        "workflow_run_url": f"https://github.com/{settings.github_repository}/actions/workflows/{settings.github_workflow}",
    }
