"""Usage metering middleware.

Every ``/v1`` request is recorded: which key, which market, status,
latency.  This is deliberately more than an access log -- it is the
telemetry that answers "what should we charge for this?" before there is
a pricing page to answer it with.  Rejected requests are recorded too
(with a null key), because sustained 401s and 403s are the first signal
of either abuse or a client hitting an entitlement wall they would like
to pay to remove.

The write happens off the event loop via the threadpool: SQLite is fast
but it is still blocking I/O, and metering must never add latency to the
signal the client actually came for.
"""

from __future__ import annotations

import time

from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from shiden.api.auth.dependencies import get_store

METERED_PREFIX = "/v1"


class UsageMeteringMiddleware(BaseHTTPMiddleware):
    """Record one usage event per metered request."""

    def __init__(self, app: ASGIApp, prefix: str = METERED_PREFIX) -> None:
        super().__init__(app)
        self.prefix = prefix

    async def dispatch(self, request: Request, call_next) -> Response:
        if not request.url.path.startswith(self.prefix):
            return await call_next(request)

        started = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - started) * 1000

        identity = getattr(request.state, "api_key", None)
        # id 0 is the auth-disabled stand-in; store NULL so it never looks
        # like a real customer in the usage summary.
        key_id = identity.id if identity is not None and identity.id else None

        limit = getattr(request.state, "rate_limit_limit", None)
        remaining = getattr(request.state, "rate_limit_remaining", None)
        if limit is not None:
            response.headers["X-RateLimit-Limit"] = str(limit)
            response.headers["X-RateLimit-Remaining"] = str(remaining)

        try:
            await run_in_threadpool(
                get_store().record_usage,
                key_id=key_id,
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                duration_ms=duration_ms,
                market_id=getattr(request.state, "market_id", None),
                response_rows=getattr(request.state, "response_rows", None),
            )
        except Exception:  # noqa: BLE001 - metering must never break a request
            pass

        return response
