from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from time import perf_counter
from uuid import uuid4

from fastapi import Request, Response


logger = logging.getLogger("stare.requests")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _request_id(request: Request) -> str:
    supplied = request.headers.get("x-request-id", "")
    return supplied if REQUEST_ID_RE.fullmatch(supplied) else uuid4().hex


def _log_payload(
    request: Request,
    request_id: str,
    status_code: int,
    duration_ms: float,
) -> str:
    route = request.scope.get("route")
    route_path = getattr(route, "path", request.url.path)
    return json.dumps(
        {
            "event": "http_request",
            "timestamp": datetime.now(UTC).isoformat(),
            "request_id": request_id,
            "method": request.method,
            "path": route_path,
            "status": status_code,
            "duration_ms": round(duration_ms, 2),
        },
        separators=(",", ":"),
    )


async def log_request(request: Request, call_next) -> Response:
    request_id = _request_id(request)
    started = perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        duration_ms = (perf_counter() - started) * 1000
        logger.exception(_log_payload(request, request_id, 500, duration_ms))
        raise

    response.headers["x-request-id"] = request_id
    duration_ms = (perf_counter() - started) * 1000
    payload = _log_payload(request, request_id, response.status_code, duration_ms)
    if request.url.path == "/health" and response.status_code < 400:
        logger.debug(payload)
    elif response.status_code >= 500:
        logger.error(payload)
    else:
        logger.info(payload)
    return response
