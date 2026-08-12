"""Per-key sliding-window rate limiting.

Scope and its limits
--------------------
State lives in process memory.  With a single uvicorn worker -- the MVP
and demo configuration -- this is exact.  Run ``uvicorn --workers N`` and
each worker enforces the limit independently, so the effective ceiling
becomes ``N * limit``.  That is a deliberate trade: a shared counter
means Redis, and Redis is not worth provisioning before the first paying
client.  When it is, replace :class:`SlidingWindowLimiter` with a Redis
-backed implementation exposing the same ``check`` method -- nothing
outside this module needs to change.

A sliding window is used rather than a fixed window because a fixed
window lets a client fire ``2 * limit`` requests across a boundary, which
is exactly the burst that would hurt a Spark-backed read path.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass

WINDOW_SECONDS = 60.0


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    retry_after: int


class SlidingWindowLimiter:
    """Thread-safe in-memory limiter keyed by API key id."""

    def __init__(self, window_seconds: float = WINDOW_SECONDS) -> None:
        self._window = window_seconds
        self._hits: dict[int, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, key_id: int, limit: int) -> RateLimitResult:
        """Register an attempt and report whether it is permitted."""
        now = time.monotonic()
        cutoff = now - self._window

        with self._lock:
            hits = self._hits[key_id]
            while hits and hits[0] <= cutoff:
                hits.popleft()

            if len(hits) >= limit:
                retry_after = max(1, int(hits[0] + self._window - now) + 1)
                return RateLimitResult(
                    allowed=False,
                    limit=limit,
                    remaining=0,
                    retry_after=retry_after,
                )

            hits.append(now)
            return RateLimitResult(
                allowed=True,
                limit=limit,
                remaining=limit - len(hits),
                retry_after=0,
            )

    def reset(self, key_id: int | None = None) -> None:
        """Clear state -- used by tests and after key revocation."""
        with self._lock:
            if key_id is None:
                self._hits.clear()
            else:
                self._hits.pop(key_id, None)


limiter = SlidingWindowLimiter()
