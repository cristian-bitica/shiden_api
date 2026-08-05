"""Bronze processor for ECB (European Central Bank) FX rate data.

Responsibility: read per-date JSON files from the landing zone and append to
the bronze/ecb_fx_rates Delta table.

Bronze is pure append-only with revision capture: re-ingesting unchanged
rates writes nothing (growth guard); a revised rate appends a new row.
Silver reads the latest row per (date, quote_currency) by ingested_at.
Base currency is always EUR; rates express 1 EUR = rate quote_currency.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from shiden.config.settings import settings
from shiden.landing_paths import ecb_fx_json_path
from shiden.processing.delta_io import append_new_observations

logger = logging.getLogger(__name__)

# Alias avoids the field name 'date' inside EcbRow shadowing datetime.date
_Date = date

_TABLE_PATH = "{base}/bronze/ecb_fx_rates"

_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=False),
        StructField("quote_currency", StringType(), nullable=False),
        StructField("rate", DoubleType(), nullable=False),
        StructField("source_file_date", DateType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
    ]
)


class EcbRow(NamedTuple):
    """One parsed EUR/X rate row for the Bronze table."""

    date: _Date
    quote_currency: str
    rate: float
    source_file_date: _Date  # calendar date when the ECB XML was fetched
    ingested_at: datetime  # naive UTC


class EcbBronzeWriter:
    """Parses ECB landing JSON files, appends new observations to Bronze."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)

    def process(self, start: date, end: date, spark: SparkSession) -> None:
        """
        Parse landing JSON files for [start, end] and append to Bronze.

        Reads:  {landing_base_path}/landing/ecb_fx_rates/{YYYY-MM-DD}.json
        Writes: {delta_base_path}/bronze/ecb_fx_rates
                (Delta, partitioned by quote_currency)

        Append-only with revision capture on (date, quote_currency) keyed
        value (rate) — safe to re-run.
        ECB only publishes on business days; missing landing files are skipped.
        """
        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        rows: list[EcbRow] = []

        current = start
        while current <= end:
            landing_p = _landing_path(current)

            if not landing_p.exists():
                logger.debug(
                    "EcbBronzeWriter: no landing file for %s"
                    " (weekend/holiday?) — skipping",
                    current.isoformat(),
                )
                current += timedelta(days=1)
                continue

            day_rows = _parse_landing_json(landing_p, current, ingested_at)
            rows.extend(day_rows)
            logger.info(
                "EcbBronzeWriter: parsed %d rates for %s",
                len(day_rows),
                current.isoformat(),
            )
            current += timedelta(days=1)

        if not rows:
            logger.info("EcbBronzeWriter: nothing to write for %s–%s", start, end)
            return

        incoming_df = spark.createDataFrame(rows, schema=_SCHEMA)
        append_new_observations(
            spark,
            incoming_df,
            self._table_path,
            key_cols=("date", "quote_currency"),
            value_cols=("rate",),
            partition_cols=("quote_currency",),
        )
        logger.info(
            "EcbBronzeWriter: appended new observations from %d parsed rows to %s",
            len(rows),
            self._table_path,
        )


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_landing_json(
    json_path: Path,
    target_date: date,
    ingested_at: datetime,
) -> list[EcbRow]:
    """
    Parse a per-date ECB landing JSON and return one EcbRow per currency pair.

    Landing JSON structure::

        {
            "date": "YYYY-MM-DD",
            "rates": {"USD": 1.0921, "GBP": 0.8567, ...},
            "source_url": "https://...",
            "fetched_at": "..."
        }

    ``source_file_date`` is derived from ``fetched_at`` when available,
    otherwise defaults to today.
    """
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("EcbBronzeWriter: failed to read %s: %s", json_path, exc)
        return []

    rate_date_str = data.get("date", target_date.isoformat())
    try:
        rate_date = date.fromisoformat(rate_date_str)
    except ValueError:
        rate_date = target_date

    # source_file_date: when was the ECB XML downloaded?
    fetched_at_str = data.get("fetched_at", "")
    try:
        source_file_date = datetime.fromisoformat(fetched_at_str).date()
    except (ValueError, AttributeError):
        source_file_date = date.today()

    rates = data.get("rates", {})
    rows: list[EcbRow] = []
    for currency, rate in rates.items():
        try:
            rows.append(
                EcbRow(
                    date=rate_date,
                    quote_currency=str(currency),
                    rate=float(rate),
                    source_file_date=source_file_date,
                    ingested_at=ingested_at,
                )
            )
        except (ValueError, TypeError) as exc:
            logger.warning(
                "EcbBronzeWriter: skipping bad rate %r for %s on %s: %s",
                rate,
                currency,
                rate_date,
                exc,
            )

    return rows


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def _landing_path(d: date) -> Path:
    return ecb_fx_json_path(settings.landing_base_path, d)
