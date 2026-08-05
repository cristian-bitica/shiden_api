"""Generation router — serves gold/generation_hourly via delta-rs (no Spark).

Handlers are sync (`def`) — delta-rs/pandas block, and FastAPI threadpools
sync handlers instead of blocking the event loop.
"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException

from shiden.api.routers._common import (
    nullable_float,
    require_market,
    validate_date_range,
)
from shiden.api.schemas import GenerationInterval, GenerationResponse

router = APIRouter()

# Columns that are returned in the top-level struct (not under 'sources').
_META_COLS = {
    "date_id", "time_id", "market_id",
    "total_mw", "renewable_mw", "thermal_mw",
    "nuclear_mw_total", "renewable_share_pct",
}


@router.get("/{market_id}", response_model=GenerationResponse)
def get_generation(
    market_id: str,
    start: date,
    end: date,
) -> GenerationResponse:
    """
    Hourly generation mix by source for a market and date range.

    Each interval includes top-level aggregate columns (total_mw, renewable_mw,
    renewable_share_pct, etc.) plus a ``sources`` dict with per-technology MW
    values keyed by snake_case source name (e.g. ``wind_onshore_mw``).

    Query parameters
    ----------------
    start : YYYY-MM-DD
    end   : YYYY-MM-DD
    """
    from deltalake.exceptions import TableNotFoundError

    from shiden.processing.gold.reader import read_gold

    require_market(market_id)
    validate_date_range(start, end)

    start_id = int(start.strftime("%Y%m%d"))
    end_id = int(end.strftime("%Y%m%d"))

    try:
        df = read_gold(
            "generation_hourly",
            filters=[
                ("market_id", "=", market_id),
                ("date_id", ">=", start_id),
                ("date_id", "<=", end_id),
            ],
        )
    except TableNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    df = df.sort_values(["date_id", "time_id"])

    # Source columns = everything ending in _mw that is not a meta column
    source_cols = [c for c in df.columns if c.endswith("_mw") and c not in _META_COLS]

    intervals = []
    for row in df.itertuples(index=False):
        sources = {col: nullable_float(row, col) for col in source_cols}
        intervals.append(
            GenerationInterval(
                date_id=int(row.date_id),
                time_id=int(row.time_id),
                total_mw=float(row.total_mw),
                renewable_mw=float(row.renewable_mw),
                thermal_mw=float(row.thermal_mw),
                nuclear_mw_total=float(row.nuclear_mw_total),
                renewable_share_pct=nullable_float(row, "renewable_share_pct"),
                sources=sources,
            )
        )

    return GenerationResponse(
        market_id=market_id,
        start=start.isoformat(),
        end=end.isoformat(),
        intervals=intervals,
    )
