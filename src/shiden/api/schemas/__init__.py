"""Pydantic response schemas for the Shiden Energy API."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------


class PriceInterval(BaseModel):
    """One hourly price record."""

    date_id:          int            = Field(description="Delivery date as YYYYMMDD integer")
    time_id:          int            = Field(description="Hour-start slot (0, 4, 8 … 92)")
    time_label:       str            = Field(description="Human-readable label e.g. '08:00'")
    price_local_mwh:  Optional[float] = Field(None, description="Day-ahead price in the market's settlement currency per MWh (see PriceResponse.currency)")
    fx_rate:          Optional[float] = Field(None, description="EUR→local FX rate applied (reconciled + forward-filled in silver/exchange_rates)")
    price_eur_mwh:    Optional[float] = Field(None, description="Day-ahead price in EUR/MWh")
    is_peak:          bool           = Field(description="True for OPCOM peak hours (08:00–19:45 local)")


class PriceResponse(BaseModel):
    market_id:  str
    currency:   str = Field(description="Settlement currency of price_local_mwh (e.g. RON)")
    start:      str
    end:        str
    intervals:  list[PriceInterval]


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------


class GenerationInterval(BaseModel):
    """One hourly generation record (wide format)."""

    date_id:             int           = Field(description="Delivery date as YYYYMMDD integer")
    time_id:             int           = Field(description="Hour-start slot (0, 4, 8 … 92)")
    total_mw:            float         = Field(description="Total generation across all sources")
    renewable_mw:        float         = Field(description="Sum of renewable generation sources")
    thermal_mw:          float         = Field(description="Sum of thermal generation sources")
    nuclear_mw_total:    float         = Field(description="Nuclear generation")
    renewable_share_pct: Optional[float] = Field(None, description="Renewable % of total")
    # Per-source columns are returned as a catch-all dict to avoid schema churn
    # as new ENTSO-E production types are discovered.
    sources:             dict[str, Optional[float]] = Field(
        default_factory=dict,
        description="Generation per source in MW, keyed by snake_case source name",
    )


class GenerationResponse(BaseModel):
    market_id:  str
    start:      str
    end:        str
    intervals:  list[GenerationInterval]


# ---------------------------------------------------------------------------
# BESS
# ---------------------------------------------------------------------------


class BessInterval(BaseModel):
    """One 15-min BESS signal record."""

    date_id:                  int           = Field(description="Delivery date as YYYYMMDD integer")
    time_id:                  int           = Field(description="15-min slot (0–95)")
    time_label:               str           = Field(description="Human-readable label e.g. '08:00'")
    price_local_mwh:          Optional[float] = Field(None, description="Price in the market's settlement currency per MWh")
    price_eur_mwh:            Optional[float] = Field(None)
    signal:                   int           = Field(description="-1=charge, 0=idle, +1=discharge")
    price_rank_asc:           Optional[int]  = Field(None, description="1=cheapest interval of the day")
    price_rank_desc:          Optional[int]  = Field(None, description="1=most expensive interval of the day")
    daily_min_price_eur_mwh:  Optional[float] = Field(None)
    daily_max_price_eur_mwh:  Optional[float] = Field(None)
    arbitrage_spread_eur_mwh: Optional[float] = Field(None)


class ArbitrageWindow(BaseModel):
    """A ranked charge or discharge window for a given day."""

    type:          str           = Field(description="'charge' or 'discharge'")
    start_time_id: int           = Field(description="First time_id of the window")
    end_time_id:   int           = Field(description="Last time_id of the window")
    start_label:   str
    end_label:     str
    avg_price_eur_mwh: Optional[float] = Field(None)
    n_slots:       int           = Field(description="Number of 15-min slots in window")


class ArbitrageResponse(BaseModel):
    market_id:        str
    date:             str
    arbitrage_spread_eur_mwh: Optional[float] = None
    charge_windows:   list[ArbitrageWindow]
    discharge_windows: list[ArbitrageWindow]


class SignalsResponse(BaseModel):
    market_id:  str
    date:       str
    intervals:  list[BessInterval]
