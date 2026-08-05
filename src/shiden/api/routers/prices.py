"""Prices router — serves gold/price_hourly via delta-rs (no Spark).

Handlers are deliberately sync (`def`, not `async def`): delta-rs and pandas
calls block, and FastAPI runs sync handlers in its threadpool instead of
blocking the event loop.
"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException

from shiden.api.routers._common import (
    nullable_float,
    require_market,
    validate_date_range,
)
from shiden.api.schemas import PriceInterval, PriceResponse

router = APIRouter()


@router.get("/{market_id}", response_model=PriceResponse)
def get_day_ahead_prices(
    market_id: str,
    start: date,
    end: date,
) -> PriceResponse:
    """
    Hourly day-ahead prices (local currency + EUR/MWh) for a market and range.

    EUR prices use the reconciled, forward-filled rate from
    silver/exchange_rates.  If no data has been ingested yet for the
    requested range, an empty ``intervals`` list is returned.

    Query parameters
    ----------------
    start : YYYY-MM-DD  — first delivery date (inclusive)
    end   : YYYY-MM-DD  — last  delivery date (inclusive)
    """
    from deltalake.exceptions import TableNotFoundError

    from shiden.processing.gold.reader import read_gold

    market = require_market(market_id)
    validate_date_range(start, end)

    start_id = int(start.strftime("%Y%m%d"))
    end_id = int(end.strftime("%Y%m%d"))

    try:
        df = read_gold(
            "price_hourly",
            filters=[
                ("market_id", "=", market_id),
                ("date_id", ">=", start_id),
                ("date_id", "<=", end_id),
            ],
        )
    except TableNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    df = df.sort_values(["date_id", "time_id"])

    intervals = [
        PriceInterval(
            date_id=int(row.date_id),
            time_id=int(row.time_id),
            time_label=str(row.time_label),
            price_local_mwh=nullable_float(row, "price_local_mwh"),
            fx_rate=nullable_float(row, "fx_rate"),
            price_eur_mwh=nullable_float(row, "price_eur_mwh"),
            is_peak=bool(row.is_peak),
        )
        for row in df.itertuples(index=False)
    ]

    return PriceResponse(
        market_id=market_id,
        currency=market.currency,
        start=start.isoformat(),
        end=end.isoformat(),
        intervals=intervals,
    )
