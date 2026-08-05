"""Bronze processor for OPCOM PZU (Day-Ahead) data.

Responsibility: read raw CSV files from the landing zone, apply minimal
parsing (type casting, column naming), and append to the Bronze Delta table.
Bronze is append-only — no deduplication, no upsert.  The Silver layer is
responsible for deduplication and cleaning.
"""

from __future__ import annotations

import csv
import io
import logging
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from shiden.config.settings import settings
from shiden.landing_paths import opcom_pzu_csv_path
from shiden.processing.delta_io import append_new_observations

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/bronze/opcom_pzu_prices"

# Column indices in the PT15 CSV data rows
_COL_INTERVAL = 1
_COL_PRICE = 2
_COL_VOL_TOTAL = 3
_COL_VOL_BUY = 4
_COL_VOL_SELL = 5

_SCHEMA = StructType(
    [
        StructField("market_id", StringType(), nullable=False),
        StructField("delivery_date", DateType(), nullable=False),
        StructField("interval_15min", IntegerType(), nullable=False),
        StructField("price_lei_mwh", DoubleType(), nullable=True),
        StructField("volume_mw", DoubleType(), nullable=True),
        StructField("volume_buy_mw", DoubleType(), nullable=True),
        StructField("volume_sell_mw", DoubleType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=False),
        StructField("source_url", StringType(), nullable=True),
    ]
)


class OpcomInterval(NamedTuple):
    """One parsed 15-minute interval row from an OPCOM PT15 CSV.

    Holds only what's present in a single CSV data row — the fields that
    are constant for the whole file (market_id, delivery_date, ingested_at,
    source_url) are attached later, once per batch, in ``_to_rows``.
    """

    interval_15min: int
    price_lei_mwh: float | None
    volume_mw: float | None
    volume_buy_mw: float | None
    volume_sell_mw: float | None


class OpcomRow(NamedTuple):
    """One row written to bronze/opcom_pzu_prices — mirrors _SCHEMA field-for-field."""

    market_id: str
    delivery_date: date
    interval_15min: int
    price_lei_mwh: float | None
    volume_mw: float | None
    volume_buy_mw: float | None
    volume_sell_mw: float | None
    ingested_at: datetime
    source_url: str


class OpcomBronzeWriter:
    """Reads OPCOM landing CSVs, parses them minimally, appends to Bronze Delta."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)

    def process(
        self, market_id: str, start: date, end: date, spark: SparkSession
    ) -> None:
        """
        Append landing CSV files for [start, end] into the Bronze Delta table.

        Reads:  {landing_base_path}/landing/opcom_pzu/{market_id}/{YYYY-MM-DD}.csv
        Writes: {delta_base_path}/bronze/opcom_pzu_prices
                (Delta, partitioned by market_id, delivery_date)

        Append-only with revision capture on
        (market_id, delivery_date, interval_15min): unchanged re-runs write
        nothing; corrected prices/volumes append new rows.  Silver reads the
        latest row per key by ingested_at.
        """
        rows: list[OpcomRow] = []
        ingested_at = datetime.now(timezone.utc)

        current = start
        while current <= end:
            intervals = _load_day(market_id, current)
            if intervals is not None:
                rows.extend(
                    _to_rows(
                        market_id,
                        current,
                        intervals,
                        ingested_at,
                        _build_source_url(current),
                    )
                )
            current += timedelta(days=1)

        if not rows:
            logger.warning(
                "OpcomBronzeWriter: no records to write for %s–%s", start, end
            )
            return

        incoming_df = spark.createDataFrame(rows, schema=_SCHEMA)
        append_new_observations(
            spark,
            incoming_df,
            self._table_path,
            key_cols=("market_id", "delivery_date", "interval_15min"),
            value_cols=(
                "price_lei_mwh",
                "volume_mw",
                "volume_buy_mw",
                "volume_sell_mw",
            ),
            partition_cols=("market_id", "delivery_date"),
        )

        logger.info(
            "OpcomBronzeWriter: appended new observations from %d rows to %s",
            len(rows),
            self._table_path,
        )


# ---------------------------------------------------------------------------
# Per-day loading
# ---------------------------------------------------------------------------


def _load_day(market_id: str, delivery_date: date) -> list[OpcomInterval] | None:
    """
    Read and parse one day's landing CSV.

    Returns None (already logged) if the landing file is missing or no
    interval rows could be parsed from it — the caller treats both as
    "nothing to contribute for this day" and moves on.
    """
    landing_path = opcom_pzu_csv_path(
        settings.landing_base_path, market_id, delivery_date
    )
    if not landing_path.exists():
        logger.warning(
            "OpcomBronzeWriter: landing file not found, skipping: %s", landing_path
        )
        return None

    raw_csv = landing_path.read_text(encoding="utf-8")
    intervals = _parse_csv(raw_csv, delivery_date)

    if not intervals:
        logger.warning(
            "OpcomBronzeWriter: no interval rows parsed from %s", landing_path
        )
        return None

    logger.info(
        "OpcomBronzeWriter: parsed %d intervals for %s",
        len(intervals),
        delivery_date.isoformat(),
    )
    return intervals


def _to_rows(
    market_id: str,
    delivery_date: date,
    intervals: list[OpcomInterval],
    ingested_at: datetime,
    source_url: str,
) -> list[OpcomRow]:
    """Attach the per-batch fields (market_id, date, ingested_at, source_url)
    to each parsed interval, producing rows ready for the Bronze schema."""
    return [
        OpcomRow(
            market_id=market_id,
            delivery_date=delivery_date,
            interval_15min=iv.interval_15min,
            price_lei_mwh=iv.price_lei_mwh,
            volume_mw=iv.volume_mw,
            volume_buy_mw=iv.volume_buy_mw,
            volume_sell_mw=iv.volume_sell_mw,
            ingested_at=ingested_at,
            source_url=source_url,
        )
        for iv in intervals
    ]


# ---------------------------------------------------------------------------
# CSV parsing (Bronze-layer concern)
# ---------------------------------------------------------------------------


def _parse_csv(raw: str, delivery_date: date) -> list[OpcomInterval]:
    """
    Parse an OPCOM PT15 CSV export into a list of interval rows.

    The file has a summary block (Base/Peak/Off-Peak rows) followed by
    96 data rows.  Only the data rows are returned.

    Deliberately driver-side, row-by-row parsing rather than a distributed
    Spark CSV reader: the preamble before the data block is not a fixed
    number of rows (OPCOM's summary section can vary), so this scans for
    the "Zona de tranzactionare" marker instead of skipping a hardcoded N
    rows. A fixed-row skip would also depend on Databricks' `skipRows`
    CSV option, which doesn't exist in the OSS Spark engine this project
    runs locally (pyspark==3.5.1) — that would work in prod but silently
    break local dev and the unit test suite. Landing files are also small
    (one day, ~100 lines), so distributed parsing buys nothing here; every
    other Bronze writer in this codebase (entsoe, ecb, bnr, weather)
    follows the same driver-side-parse-then-createDataFrame pattern.
    """
    reader = csv.reader(io.StringIO(raw))
    intervals: list[OpcomInterval] = []
    for row in _data_rows(reader):
        interval = _row_to_interval(row, delivery_date)
        if interval is not None:
            intervals.append(interval)
    return intervals


def _data_rows(reader: Iterator[list[str]]) -> Iterator[list[str]]:
    """
    Yield stripped data rows, skipping the title/summary preamble and blanks.

    Scans for the "Zona de tranzactionare" column-header row that marks the
    start of the 96-interval data block; everything before it (title line,
    blank lines, the Base/Peak/Off-Peak summary rows) is discarded.
    """
    in_data_block = False
    for row in reader:
        if not row or all(cell.strip() == "" for cell in row):
            continue

        stripped = [c.strip().strip('"') for c in row]

        if not in_data_block:
            if len(stripped) >= 6 and "Zona de tranzactionare" in stripped[0]:
                in_data_block = True
            continue

        if len(stripped) < 6:
            continue

        yield stripped


def _row_to_interval(stripped: list[str], delivery_date: date) -> OpcomInterval | None:
    """Parse one stripped data row into an OpcomInterval, or None if malformed."""
    interval_str = stripped[_COL_INTERVAL]
    if not interval_str.isdigit():
        return None

    try:
        return OpcomInterval(
            interval_15min=int(interval_str),
            price_lei_mwh=_parse_float(stripped[_COL_PRICE]),
            volume_mw=_parse_float(stripped[_COL_VOL_TOTAL]),
            volume_buy_mw=_parse_float(stripped[_COL_VOL_BUY]),
            volume_sell_mw=_parse_float(stripped[_COL_VOL_SELL]),
        )
    except (ValueError, IndexError) as exc:
        logger.warning(
            "Skipping malformed row for %s: %s — %s",
            delivery_date.isoformat(),
            stripped,
            exc,
        )
        return None


def _parse_float(value: str) -> float:
    """
    Convert an OPCOM number string to float.

    Handles both:
    - CSV export format (standard):  "636.23"   → 636.23
    - HTML table format (Romanian):  "1.638,5"  → 1638.5
    """
    cleaned = value.strip()
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    return float(cleaned)


def _build_source_url(delivery_date: date) -> str:
    """Reconstruct the OPCOM export URL for a delivery date.

    Best-effort: the landing zone doesn't currently persist the exact URL
    a file was fetched from (unlike bnr's JSON sidecar), so this rebuilds
    it from the date using the same template the ingester used to fetch.
    """
    return (
        f"https://www.opcom.ro/rapoarte-pzu-raportPIP-export-csv"
        f"/{delivery_date.day:02d}/{delivery_date.month:02d}/{delivery_date.year}/ro?resolution=15"
    )
