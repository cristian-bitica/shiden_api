"""Weather router — placeholder until the Gold weather table lands."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException

from shiden.api.routers._common import require_market, validate_date_range

router = APIRouter()


@router.get("/{market_id}")
def get_weather(market_id: str, start: date, end: date) -> None:
    """Hourly weather data for all configured locations in a market
    (planned — reads a Gold weather table once it exists)."""
    require_market(market_id)
    validate_date_range(start, end)
    raise HTTPException(
        status_code=501,
        detail="Weather endpoint is not implemented yet (planned).",
    )
