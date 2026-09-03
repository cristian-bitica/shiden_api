"""FastAPI dependencies for key authentication and market entitlement.

Two dependencies, deliberately separate:

``require_api_key``
    Proves *who* is calling.  Use on routes with no market in the path.

``require_market_access``
    Proves who is calling *and* that they are entitled to the market in
    the path.  Use on every ``/{market_id}`` route.

Entitlement is per-market rather than per-row because Shiden serves
market data, not customer data: Romanian day-ahead prices are identical
for every client.  What a client buys is access to a market, so that is
what the key grants.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from fastapi import Depends, HTTPException, Request
from fastapi.security import APIKeyHeader

from shiden.api.auth.ratelimit import limiter
from shiden.api.auth.store import ApiKeyRecord, KeyStore
from shiden.config.settings import settings

API_KEY_HEADER = "X-API-Key"

# auto_error=False so we can emit our own message rather than FastAPI's
# terse "Not authenticated", and so the auth-disabled path can short-circuit.
_header_scheme = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


@dataclass(frozen=True)
class AuthenticatedKey:
    """The caller's identity, attached to the request for downstream use."""

    id: int
    name: str
    display: str
    environment: str
    markets: tuple[str, ...]
    rate_limit_per_min: int

    @classmethod
    def from_record(cls, record: ApiKeyRecord) -> "AuthenticatedKey":
        return cls(
            id=record.id,
            name=record.name,
            display=record.display,
            environment=record.environment,
            markets=record.markets,
            rate_limit_per_min=record.rate_limit_per_min,
        )

    @classmethod
    def anonymous(cls) -> "AuthenticatedKey":
        """Stand-in identity used when auth is disabled for local work."""
        return cls(
            id=0,
            name="auth-disabled",
            display="-",
            environment="local",
            markets=("*",),
            rate_limit_per_min=0,
        )


@lru_cache(maxsize=1)
def get_store() -> KeyStore:
    """Process-wide key store. Overridable in tests via dependency_overrides."""
    return KeyStore(settings.auth_db_path)


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=401,
        detail=detail,
        headers={"WWW-Authenticate": API_KEY_HEADER},
    )


def require_api_key(
    request: Request,
    presented: str | None = Depends(_header_scheme),
) -> AuthenticatedKey:
    """Authenticate the caller and apply their rate limit."""
    if not settings.api_auth_enabled:
        identity = AuthenticatedKey.anonymous()
        request.state.api_key = identity
        return identity

    if not presented:
        raise _unauthorized(
            f"Missing {API_KEY_HEADER} header. "
            "Request a key from Shiden and send it as "
            f"'{API_KEY_HEADER}: shiden_live_...'."
        )

    store = get_store()
    record = store.lookup(presented)
    if record is None:
        raise _unauthorized("Invalid API key.")
    if not record.is_active:
        raise _unauthorized("API key has been revoked.")

    identity = AuthenticatedKey.from_record(record)
    # Attach identity *before* the rate-limit check so a 429 is still
    # attributed to the key that caused it. A client sustaining 429s is the
    # clearest buy signal the metering produces -- losing that attribution
    # would make throttled clients indistinguishable from anonymous abuse.
    request.state.api_key = identity

    verdict = limiter.check(record.id, record.rate_limit_per_min)
    if not verdict.allowed:
        request.state.rate_limit_limit = verdict.limit
        request.state.rate_limit_remaining = 0
        raise HTTPException(
            status_code=429,
            detail=(
                f"Rate limit exceeded: {verdict.limit} requests/minute. "
                f"Retry in {verdict.retry_after}s."
            ),
            headers={
                "Retry-After": str(verdict.retry_after),
                "X-RateLimit-Limit": str(verdict.limit),
                "X-RateLimit-Remaining": "0",
            },
        )

    request.state.rate_limit_limit = verdict.limit
    request.state.rate_limit_remaining = verdict.remaining
    store.touch(record.id)
    return identity


def require_market_access(
    request: Request,
    market_id: str,
    key: AuthenticatedKey = Depends(require_api_key),
) -> AuthenticatedKey:
    """Authenticate, then check the key is entitled to this market."""
    request.state.market_id = market_id

    if "*" not in key.markets and market_id not in key.markets:
        raise HTTPException(
            status_code=403,
            detail=(
                f"API key is not entitled to market {market_id!r}. "
                f"Entitled markets: {sorted(key.markets)}. "
                "Contact Shiden to add this market to your plan."
            ),
        )
    return key
