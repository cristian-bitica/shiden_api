"""BESS router — serves gold/bess_signals via delta-rs (no Spark).

Two implemented endpoints:

  GET /bess/{market_id}/signals?date=YYYY-MM-DD
      Full 96-slot signal list for one day.

  GET /bess/{market_id}/arbitrage-windows?date=YYYY-MM-DD
      Charge / discharge windows (contiguous consecutive-slot groups).

Handlers are sync (`def`) — delta-rs/pandas block, and FastAPI threadpools
sync handlers instead of blocking the event loop.
"""

from __future__ import annotations

from datetime import date
from itertools import groupby

import pandas as pd
from fastapi import APIRouter, HTTPException

from shiden.api.routers._common import nullable_float, nullable_int, require_market
from shiden.api.schemas import (
    ArbitrageResponse,
    ArbitrageWindow,
    BessInterval,
    SignalsResponse,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/{market_id}/signals", response_model=SignalsResponse)
def get_signals(market_id: str, date: date) -> SignalsResponse:
    """
    Full 96-slot (15-min) BESS charge/discharge/idle signal list for one day.

    signal values:
      -1 = charge  (lowest N price intervals of the day)
       0 = idle
      +1 = discharge (highest N price intervals of the day)
    """
    require_market(market_id)
    df = _load_day(market_id, date)
    df = df.sort_values("time_id")

    intervals = [
        BessInterval(
            date_id=int(row.date_id),
            time_id=int(row.time_id),
            time_label=str(row.time_label),
            price_local_mwh=nullable_float(row, "price_local_mwh"),
            price_eur_mwh=nullable_float(row, "price_eur_mwh"),
            signal=int(row.signal),
            price_rank_asc=nullable_int(row, "price_rank_asc"),
            price_rank_desc=nullable_int(row, "price_rank_desc"),
            daily_min_price_eur_mwh=nullable_float(row, "daily_min_price_eur_mwh"),
            daily_max_price_eur_mwh=nullable_float(row, "daily_max_price_eur_mwh"),
            arbitrage_spread_eur_mwh=nullable_float(row, "arbitrage_spread_eur_mwh"),
        )
        for row in df.itertuples(index=False)
    ]

    return SignalsResponse(
        market_id=market_id,
        date=date.isoformat(),
        intervals=intervals,
    )


@router.get("/{market_id}/arbitrage-windows", response_model=ArbitrageResponse)
def get_arbitrage_windows(market_id: str, date: date) -> ArbitrageResponse:
    """
    Charge and discharge windows for one day, grouped by contiguous signal runs.

    Each ``ArbitrageWindow`` covers a sequence of consecutive 15-min slots that
    share the same signal (-1 charge, +1 discharge).  A single "optimal window"
    may be split across multiple non-contiguous windows if the cheapest charge
    slots are not adjacent.

    ``arbitrage_spread_eur_mwh`` is the day-level max − min EUR price.
    """
    require_market(market_id)
    df = _load_day(market_id, date)
    df = df.sort_values("time_id")

    # Day-level spread (constant across all rows — take from first row)
    spread = nullable_float(df.iloc[0], "arbitrage_spread_eur_mwh") if len(df) else None

    charge_windows: list[ArbitrageWindow] = []
    discharge_windows: list[ArbitrageWindow] = []

    # Group consecutive rows by signal value
    rows = list(df.itertuples(index=False))
    for signal_val, group_iter in groupby(rows, key=lambda r: int(r.signal)):
        if signal_val == 0:
            continue  # idle — skip

        group = list(group_iter)
        prices = [nullable_float(r, "price_eur_mwh") for r in group]
        valid_prices = [p for p in prices if p is not None]
        avg_price = sum(valid_prices) / len(valid_prices) if valid_prices else None

        window = ArbitrageWindow(
            type="charge" if signal_val == -1 else "discharge",
            start_time_id=int(group[0].time_id),
            end_time_id=int(group[-1].time_id),
            start_label=str(group[0].time_label),
            end_label=str(group[-1].time_label),
            avg_price_eur_mwh=avg_price,
            n_slots=len(group),
        )

        if signal_val == -1:
            charge_windows.append(window)
        else:
            discharge_windows.append(window)

    return ArbitrageResponse(
        market_id=market_id,
        date=date.isoformat(),
        arbitrage_spread_eur_mwh=spread,
        charge_windows=charge_windows,
        discharge_windows=discharge_windows,
    )


@router.get("/{market_id}/forecast")
def get_price_forecast(market_id: str, target_date: date) -> None:
    """
    Weather-driven next-day price forecast (planned — ML phase).
    """
    require_market(market_id)
    raise HTTPException(
        status_code=501,
        detail="Price forecast is not implemented yet (planned: ML phase).",
    )


# ---------------------------------------------------------------------------
# Shared loader
# ---------------------------------------------------------------------------


def _load_day(market_id: str, target_date: date) -> pd.DataFrame:
    """Read one day's BESS signals from Gold via delta-rs."""
    from deltalake.exceptions import TableNotFoundError

    from shiden.processing.gold.reader import read_gold

    date_id = int(target_date.strftime("%Y%m%d"))

    try:
        df = read_gold(
            "bess_signals",
            filters=[
                ("market_id", "=", market_id),
                ("date_id", "=", date_id),
            ],
        )
    except TableNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No BESS signals found for market={market_id!r} "
                f"date={target_date.isoformat()!r}. "
                "Run the Gold pipeline for this day first."
            ),
        )

    return df
