"""Shared router helpers.

``nullable_float`` / ``nullable_int`` previously existed as three private
copies across the prices, generation and BESS routers.
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any

from fastapi import HTTPException

from shiden.config.markets import MARKETS, MarketConfig


def require_market(market_id: str) -> MarketConfig:
    """Return the market config or raise 404 for unknown markets."""
    market = MARKETS.get(market_id)
    if market is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Unknown market: {market_id!r}. "
                f"Available: {sorted(MARKETS.keys())}"
            ),
        )
    return market


def validate_date_range(start: date, end: date) -> None:
    """Raise 422 when the requested range is inverted."""
    if end < start:
        raise HTTPException(
            status_code=422,
            detail=f"end ({end.isoformat()}) must be >= start ({start.isoformat()})",
        )


def nullable_float(row: Any, col: str) -> float | None:
    """Return float or None, converting NaN → None."""
    val = getattr(row, col, None)
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def nullable_int(row: Any, col: str) -> int | None:
    """Return int or None, converting NaN → None."""
    f = nullable_float(row, col)
    return None if f is None else int(f)
