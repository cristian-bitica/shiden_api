"""ENTSO-E actual generation and load ingester — landing zone only.

Responsibility: fetch actual generation per production type (document type A75)
and actual total load (A65) for a market's bidding zone via the entsoe-py client,
and write per-date JSON files to the landing zone.

ENTSO-E data for Romania is hourly (1-hour settlement period).

Landing zone layout::

    {landing_base_path}/landing/entsoe/{market_id}/generation/{YYYY-MM-DD}.json
    {landing_base_path}/landing/entsoe/{market_id}/load/{YYYY-MM-DD}.json

Note on entsoe-py column format:
    query_generation() returns a wide DataFrame. Most production types get a
    flat string column name (e.g. "Wind Onshore"). Types that report both
    dispatch and consumption (e.g. Hydro Pumped Storage) get a two-level
    MultiIndex column: ("Hydro Pumped Storage", "Actual Aggregated") and
    ("Hydro Pumped Storage", "Actual Consumption"). Both are serialised as
    "Type|SubType" in the landing JSON and stored as the production_type key in
    Bronze so Silver can split on "|" if needed.

Note on ENTSO-E end parameter:
    The ENTSO-E API and entsoe-py both treat end as exclusive, so we pass
    end + 1 day to include the full end date.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from shiden.config.markets import get_market
from shiden.config.settings import settings
from shiden.dates import date_range as _date_range
from shiden.landing_paths import entsoe_generation_json_path, entsoe_load_json_path

logger = logging.getLogger(__name__)

_A75 = "A75"  # Actual Generation per Production Type
_A65 = "A65"  # Actual Total Load


class EntsoeIngester:
    """Fetches ENTSO-E actual generation and load data to the landing zone."""

    def ingest(self, market_id: str, start: date, end: date) -> None:
        """
        Fetch actual generation (A75) and load (A65) for [start, end].

        Writes per-date JSON files to the landing zone.  Landing files are
        re-written on every run (not skipped when present): ENTSO-E revises
        published actuals, and Bronze's append-with-revision-capture relies
        on landing holding the latest observation.  The fetch is a single
        API call per series either way.

        Raises RuntimeError if entsoe_api_key is not configured.
        """
        if not settings.entsoe_api_key:
            raise RuntimeError(
                "EntsoeIngester: entsoe_api_key not set — add ENTSOE_API_KEY to .env"
            )

        market = get_market(market_id)
        bidding_zone = market.bidding_zone

        logger.info(
            "ENTSO-E ingest: market=%s zone=%s %s–%s",
            market_id,
            bidding_zone,
            start.isoformat(),
            end.isoformat(),
        )

        self._ingest_series(
            label="gen",
            document_type=_A75,
            df=self._fetch_generation(bidding_zone, start, end),
            to_records=_generation_to_records,
            landing_path=lambda d: _gen_landing_path(market_id, d),
            market_id=market_id,
            bidding_zone=bidding_zone,
            start=start,
            end=end,
        )
        self._ingest_series(
            label="load",
            document_type=_A65,
            df=self._fetch_load(bidding_zone, start, end),
            to_records=_load_to_records,
            landing_path=lambda d: _load_landing_path(market_id, d),
            market_id=market_id,
            bidding_zone=bidding_zone,
            start=start,
            end=end,
        )

    def _ingest_series(
        self,
        *,
        label: str,
        document_type: str,
        df: pd.DataFrame,
        to_records: Callable[[pd.DataFrame], list[dict[str, Any]]],
        landing_path: Callable[[date], Path],
        market_id: str,
        bidding_zone: str,
        start: date,
        end: date,
    ) -> None:
        """Write one fetched series (generation or load) to per-date landing
        JSON files.  The two series differ only in document type, record
        serialisation and landing path — everything else is identical."""
        if df is None or df.empty:
            logger.warning(
                "ENTSO-E: no %s data returned for %s %s–%s",
                label,
                market_id,
                start,
                end,
            )
            return

        fetched_at = datetime.now(timezone.utc).isoformat()
        saved = 0

        for d in _date_range(start, end):
            date_str = d.isoformat()
            day_df = _filter_day(df, d)
            if day_df.empty:
                logger.debug("ENTSO-E %s: no data for %s", label, date_str)
                continue

            records = to_records(day_df)
            path = landing_path(d)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "market_id": market_id,
                        "date": date_str,
                        "bidding_zone": bidding_zone,
                        "document_type": document_type,
                        "fetched_at": fetched_at,
                        "records": records,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            saved += 1
            logger.info(
                "ENTSO-E %s landing: saved %s (%d records)", label, path, len(records)
            )

        logger.info(
            "ENTSO-E %s ingest complete: %s–%s written=%d",
            label,
            start.isoformat(),
            end.isoformat(),
            saved,
        )

    # ------------------------------------------------------------------
    # entsoe-py API calls
    # ------------------------------------------------------------------

    def _fetch_generation(
        self, bidding_zone: str, start: date, end: date
    ) -> pd.DataFrame:
        """Call entsoe-py query_generation; end is made exclusive (+1 day)."""
        from entsoe import EntsoePandasClient

        client = EntsoePandasClient(api_key=settings.entsoe_api_key)
        ts_start = pd.Timestamp(start.isoformat(), tz="UTC")
        ts_end = pd.Timestamp((end + timedelta(days=1)).isoformat(), tz="UTC")
        return self._call_with_retry(
            lambda: client.query_generation(bidding_zone, start=ts_start, end=ts_end)
        )

    def _fetch_load(self, bidding_zone: str, start: date, end: date) -> pd.DataFrame:
        """Call entsoe-py query_load; normalises Series return to DataFrame."""
        from entsoe import EntsoePandasClient

        client = EntsoePandasClient(api_key=settings.entsoe_api_key)
        ts_start = pd.Timestamp(start.isoformat(), tz="UTC")
        ts_end = pd.Timestamp((end + timedelta(days=1)).isoformat(), tz="UTC")
        result = self._call_with_retry(
            lambda: client.query_load(bidding_zone, start=ts_start, end=ts_end)
        )
        if isinstance(result, pd.Series):
            result = result.to_frame(name="Actual Load")
        return result

    def _call_with_retry(
        self, fn: Callable[[], pd.DataFrame], max_retries: int = 3
    ) -> pd.DataFrame:
        """Call fn(), retrying on transient failures with exponential backoff.

        NoMatchingDataError (ENTSO-E has no data for this period) is not an
        error — return an empty DataFrame without retrying.
        """
        from entsoe.exceptions import NoMatchingDataError

        delay = 5.0
        last_exc: Exception | None = None
        for attempt in range(max_retries):
            try:
                return fn()
            except NoMatchingDataError as exc:
                logger.info(
                    "ENTSO-E NoMatchingDataError — no data for period: %s", exc
                )
                return pd.DataFrame()
            except Exception as exc:
                last_exc = exc
                if attempt < max_retries - 1:
                    logger.warning(
                        "ENTSO-E API attempt %d/%d failed (%s), retrying in %.0fs",
                        attempt + 1,
                        max_retries,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                    delay *= 2
        raise RuntimeError(
            f"ENTSO-E API failed after {max_retries} attempts: {last_exc}"
        ) from last_exc


# ---------------------------------------------------------------------------
# Serialisation helpers (module-level so they can be unit-tested directly)
# ---------------------------------------------------------------------------


def _generation_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Convert wide generation DataFrame to long-format dicts for JSON landing.

    Output::

        [
          {"timestamp_utc": "2024-01-15T00:00:00",
           "production_type": "Wind Onshore",
           "actual_mw": 1823.0},
          ...
        ]

    MultiIndex columns (e.g. Hydro Pumped Storage with Actual Aggregated +
    Actual Consumption) are flattened to "Type|SubType" strings.
    NaN values are preserved as null — Bronze schema is NULLABLE for actual_mw.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [
            f"{c[0]}|{c[1]}" if len(c) > 1 and c[1] else str(c[0]) for c in df.columns
        ]

    records: list[dict[str, Any]] = []
    for ts, row in df.iterrows():
        ts_str = _ts_to_utc_str(ts)
        for col, val in row.items():
            records.append(
                {
                    "timestamp_utc": ts_str,
                    "production_type": str(col),
                    "actual_mw": None if pd.isna(val) else float(val),
                }
            )
    return records


def _load_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Convert load DataFrame to dicts for JSON landing.

    Output::

        [{"timestamp_utc": "2024-01-15T00:00:00", "actual_load_mw": 7234.0}, ...]

    NaN is preserved as null.
    """
    load_col = df.columns[0]
    records: list[dict[str, Any]] = []
    for ts, val in df[load_col].items():
        records.append(
            {
                "timestamp_utc": _ts_to_utc_str(ts),
                "actual_load_mw": None if pd.isna(val) else float(val),
            }
        )
    return records


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def _gen_landing_path(market_id: str, d: date) -> Path:
    return entsoe_generation_json_path(settings.landing_base_path, market_id, d)


def _load_landing_path(market_id: str, d: date) -> Path:
    return entsoe_load_json_path(settings.landing_base_path, market_id, d)


# ---------------------------------------------------------------------------
# Private utilities
# ---------------------------------------------------------------------------


def _filter_day(df: pd.DataFrame, d: date) -> pd.DataFrame:
    """Return rows whose UTC timestamp falls on calendar date d.

    Handles both tz-aware (entsoe-py standard) and naive DatetimeIndex.
    """
    idx = df.index
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC")
        target = pd.Timestamp(d.isoformat(), tz="UTC")
    else:
        target = pd.Timestamp(d.isoformat())  # naive — treat as UTC
    return df[idx.normalize() == target]


def _ts_to_utc_str(ts: Any) -> str:
    """Convert a pandas Timestamp (tz-aware or naive) to naive-UTC ISO-8601."""
    if getattr(ts, "tz", None) is not None:
        ts = ts.tz_convert("UTC")
    return str(ts.strftime("%Y-%m-%dT%H:%M:%S"))
