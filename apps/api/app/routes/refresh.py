import asyncio
import time
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status

from ..auth import AuthenticatedUser, require_user
from ..config import get_settings

router = APIRouter(prefix="/api", tags=["updates"])

_dispatch_lock = asyncio.Lock()
_last_dispatch_at = 0.0
_ACTIVE_RUN_STATUSES = {"queued", "in_progress", "waiting", "requested", "pending"}


def _workflow_url(repository: str, workflow: str) -> str:
    owner, repo = repository.split("/", 1)
    workflow_id = quote(workflow, safe="")
    return f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow_id}"


def _repository_url(repository: str) -> str:
    return f"https://api.github.com/repos/{repository}"


def _github_headers(token: str) -> dict[str, str]:
    return {
        "accept": "application/vnd.github+json",
        "authorization": f"Bearer {token}",
        "x-github-api-version": "2022-11-28",
    }


def _authorize_refresh(user: AuthenticatedUser) -> tuple[Any, dict[str, str]]:
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

    return settings, _github_headers(settings.github_actions_token)


def _run_progress(run: dict[str, Any], jobs: list[dict[str, Any]]) -> dict[str, Any]:
    run_status = run.get("status", "queued")
    conclusion = run.get("conclusion")
    steps = [step for job in jobs for step in job.get("steps", [])]
    completed_steps = sum(step.get("status") == "completed" for step in steps)
    current_step = next(
        (step for step in steps if step.get("status") == "in_progress"),
        None,
    ) or next((step for step in steps if step.get("status") == "queued"), None)

    if run_status == "completed":
        progress = 100
        if conclusion == "success":
            state = "success"
            stage = "Market update complete"
            message = "The update finished. Loading the new report data."
        else:
            state = "failed"
            stage = "Market update failed"
            message = f"The update ended with status: {conclusion or 'failed'}."
    else:
        progress = max(5, min(95, round(completed_steps / len(steps) * 100))) if steps else 5
        state = "in_progress" if run_status == "in_progress" else "queued"
        stage = current_step.get("name", "Waiting for an update worker") if current_step else "Waiting for an update worker"
        message = f"{stage} ({completed_steps} of {len(steps)} steps complete)." if steps else "The update is queued and waiting to start."

    return {
        "status": state,
        "progress": progress,
        "stage": stage,
        "message": message,
        "workflow_run_id": run.get("id"),
        "workflow_run_url": run.get("html_url", ""),
        "conclusion": conclusion,
    }


@router.post("/refresh", status_code=status.HTTP_202_ACCEPTED)
async def refresh_market_data(user: AuthenticatedUser = Depends(require_user)) -> dict[str, Any]:
    global _last_dispatch_at

    settings, headers = _authorize_refresh(user)
    workflow_url = _workflow_url(settings.github_repository, settings.github_workflow)

    async with _dispatch_lock:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                runs_response = await client.get(
                    f"{workflow_url}/runs",
                    headers=headers,
                    params={"branch": "main", "per_page": 10},
                )
                runs_response.raise_for_status()
                runs = runs_response.json().get("workflow_runs", [])
                latest_run = runs[0] if runs else None
                active_run = next((run for run in runs if run.get("status") in _ACTIVE_RUN_STATUSES), None)

                if active_run:
                    return {
                        "status": "already_running",
                        "message": "A market update is already in progress.",
                        "workflow_run_id": active_run.get("id"),
                        "workflow_run_url": active_run.get("html_url", ""),
                    }

                if time.monotonic() - _last_dispatch_at < 60:
                    return {
                        "status": "already_running",
                        "message": "A market update was requested recently and is starting.",
                        "baseline_run_id": latest_run.get("id") if latest_run else None,
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
        "baseline_run_id": latest_run.get("id") if latest_run else None,
        "workflow_run_url": f"https://github.com/{settings.github_repository}/actions/workflows/{settings.github_workflow}",
    }


@router.get("/refresh/status")
async def market_refresh_status(
    baseline_run_id: int | None = Query(default=None, ge=1),
    workflow_run_id: int | None = Query(default=None, ge=1),
    user: AuthenticatedUser = Depends(require_user),
) -> dict[str, Any]:
    settings, headers = _authorize_refresh(user)
    workflow_url = _workflow_url(settings.github_repository, settings.github_workflow)
    repository_url = _repository_url(settings.github_repository)

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            if workflow_run_id:
                run_response = await client.get(
                    f"{repository_url}/actions/runs/{workflow_run_id}",
                    headers=headers,
                )
                run_response.raise_for_status()
                run = run_response.json()
            else:
                runs_response = await client.get(
                    f"{workflow_url}/runs",
                    headers=headers,
                    params={"branch": "main", "per_page": 10},
                )
                runs_response.raise_for_status()
                runs = runs_response.json().get("workflow_runs", [])
                run = next(
                    (item for item in runs if item.get("status") in _ACTIVE_RUN_STATUSES),
                    None,
                ) or next(
                    (
                        item for item in runs
                        if baseline_run_id is None or item.get("id", 0) > baseline_run_id
                    ),
                    None,
                )

                if not run:
                    return {
                        "status": "waiting",
                        "progress": 2,
                        "stage": "Waiting for GitHub Actions",
                        "message": "The update request was accepted and is waiting to appear in the queue.",
                    }

            jobs_response = await client.get(
                f"{repository_url}/actions/runs/{run['id']}/jobs",
                headers=headers,
                params={"per_page": 100},
            )
            jobs_response.raise_for_status()
            jobs = jobs_response.json().get("jobs", [])
    except (httpx.HTTPError, ValueError, KeyError) as exc:
        raise HTTPException(status_code=502, detail="Market update progress is temporarily unavailable") from exc

    return _run_progress(run, jobs)
