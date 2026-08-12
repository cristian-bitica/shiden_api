from typing import Any

from fastapi import Depends, FastAPI

from shiden.api.auth import AuthenticatedKey, require_api_key, require_market_access
from shiden.api.middleware import UsageMeteringMiddleware
from shiden.api.routers import bess, generation, prices, weather

DESCRIPTION = """
Energy market data and BESS intelligence for operators and aggregators.

### Authentication

Every `/v1` endpoint requires an API key, sent as the `X-API-Key` header:

```bash
curl -H "X-API-Key: shiden_live_..." \\
  "https://api.shiden.io/v1/bess/RO/signals?date=2026-08-01"
```

Keys are scoped to the markets on your plan; requesting a market you are
not entitled to returns `403`.

### Rate limits

Limits are per key and reported on every response via `X-RateLimit-Limit`
and `X-RateLimit-Remaining`. Exceeding the limit returns `429` with a
`Retry-After` header — back off for that many seconds rather than retrying
immediately.
"""

app = FastAPI(
    title="Shiden Energy API",
    description=DESCRIPTION,
    version="0.1.0",
)

app.add_middleware(UsageMeteringMiddleware)

# Every /v1 route carries {market_id}, so entitlement is enforced once here
# rather than being re-declared (and eventually forgotten) on each handler.
market_scoped = [Depends(require_market_access)]

app.include_router(
    prices.router, prefix="/v1/prices", tags=["prices"], dependencies=market_scoped
)
app.include_router(
    generation.router,
    prefix="/v1/generation",
    tags=["generation"],
    dependencies=market_scoped,
)
app.include_router(
    weather.router, prefix="/v1/weather", tags=["weather"], dependencies=market_scoped
)
app.include_router(
    bess.router, prefix="/v1/bess", tags=["bess"], dependencies=market_scoped
)


@app.get("/health", tags=["meta"])
def health() -> dict[str, Any]:
    """Liveness probe. Unauthenticated by design so uptime monitors can hit it."""
    return {"status": "ok", "version": "0.1.0"}


@app.get("/v1/me", tags=["meta"])
def whoami(key: AuthenticatedKey = Depends(require_api_key)) -> dict[str, Any]:
    """Echo back the calling key's identity, entitlements and rate limit.

    Useful as a first call when integrating: it confirms the key works and
    tells you which markets you can query without guessing.
    """
    return {
        "name": key.name,
        "key": key.display,
        "environment": key.environment,
        "markets": list(key.markets),
        "rate_limit_per_min": key.rate_limit_per_min,
    }
