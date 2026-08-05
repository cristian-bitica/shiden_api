"""Shared landing-zone path conventions.

An ingester (writer) and its corresponding Bronze processor (reader) must
agree on exactly where a source's raw files live in the landing zone. That
layout previously lived as a duplicated ``_LANDING_PATH_TMPL`` string
constant in both ``shiden.ingestion.<source>`` and
``shiden.processing.bronze.<source>`` — two independent copies of the same
contract that could silently drift apart.

This module centralizes those templates in one place so both layers import
the same path-building function. It intentionally holds no logic beyond
path construction — it must not become a dependency edge between the
ingestion and processing layers themselves (they still don't import from
each other; they each import this small path-only module).

Covers all five sources: OPCOM PZU, ENTSO-E (generation + load), ECB,
BNR (XML + JSON sidecar), and weather.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

_OPCOM_PZU_TMPL = "{base}/landing/opcom_pzu/{market_id}/{date}.csv"
_ENTSOE_GENERATION_TMPL = "{base}/landing/entsoe/{market_id}/generation/{date}.json"
_ENTSOE_LOAD_TMPL = "{base}/landing/entsoe/{market_id}/load/{date}.json"
_ECB_FX_TMPL = "{base}/landing/ecb_fx_rates/{date}.json"
_BNR_FX_XML_TMPL = "{base}/landing/bnr_fx_rates/{year}/{date}.xml"
_BNR_FX_SIDECAR_TMPL = "{base}/landing/bnr_fx_rates/{year}/{date}.json"
_WEATHER_HOURLY_TMPL = "{base}/landing/weather/{market_id}/{location}/{date}.json"


def opcom_pzu_csv_path(base: str, market_id: str, delivery_date: date) -> Path:
    """Landing path for one day of OPCOM PZU CSV data.

    ``base`` is ``settings.landing_base_path``, passed in explicitly rather
    than read from settings here, so this module has no config dependency
    and stays trivially testable.
    """
    return Path(
        _OPCOM_PZU_TMPL.format(
            base=base, market_id=market_id, date=delivery_date.isoformat()
        )
    )


def entsoe_generation_json_path(base: str, market_id: str, delivery_date: date) -> Path:
    """Landing path for one day of ENTSO-E actual generation (A75) data."""
    return Path(
        _ENTSOE_GENERATION_TMPL.format(
            base=base, market_id=market_id, date=delivery_date.isoformat()
        )
    )


def entsoe_load_json_path(base: str, market_id: str, delivery_date: date) -> Path:
    """Landing path for one day of ENTSO-E actual load (A65) data."""
    return Path(
        _ENTSOE_LOAD_TMPL.format(
            base=base, market_id=market_id, date=delivery_date.isoformat()
        )
    )


def ecb_fx_json_path(base: str, delivery_date: date) -> Path:
    """Landing path for one day of ECB EUR reference rates."""
    return Path(_ECB_FX_TMPL.format(base=base, date=delivery_date.isoformat()))


def bnr_fx_xml_path(base: str, delivery_date: date) -> Path:
    """Landing path for one day of BNR FX rates (raw XML Cube)."""
    return Path(
        _BNR_FX_XML_TMPL.format(
            base=base, year=delivery_date.year, date=delivery_date.isoformat()
        )
    )


def bnr_fx_sidecar_path(base: str, delivery_date: date) -> Path:
    """Landing path for the JSON sidecar (source_url, fetched_at) accompanying
    one day of BNR FX rates."""
    return Path(
        _BNR_FX_SIDECAR_TMPL.format(
            base=base, year=delivery_date.year, date=delivery_date.isoformat()
        )
    )


def weather_hourly_json_path(
    base: str, market_id: str, location: str, delivery_date: date
) -> Path:
    """Landing path for one (location, day) of hourly weather data."""
    return Path(
        _WEATHER_HOURLY_TMPL.format(
            base=base,
            market_id=market_id,
            location=location,
            date=delivery_date.isoformat(),
        )
    )
