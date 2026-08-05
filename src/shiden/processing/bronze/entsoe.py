"""Bronze processor for ENTSO-E actual generation and load data.

Responsibility: read per-date JSON landing files, parse them into typed rows,
and append to two Bronze Delta tables:

    bronze/entsoe_generation  — keyed (market_id, timestamp_utc, production_type)
    bronze/entsoe_load        — keyed (market_id, timestamp_utc)

Bronze is pure append-only with revision capture: ENTSO-E republishes
corrected actuals, and each revised value is appended as a new row (the
original observation is preserved); unchanged re-ingests write nothing.
Silver reads the latest row per key by ingested_at.
NaN values from the ingester are stored as NULL (nullable columns).
No business logic; Silver handles cross-production aggregations, unit
conversion, and alignment with the 15-min OPCOM price grid.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from shiden.config.settings import settings
from shiden.dates import date_range
from shiden.landing_paths import entsoe_generation_json_path, entsoe_load_json_path
from shiden.processing.delta_io import append_new_observations

logger = logging.getLogger(__name__)

_GEN_TABLE_PATH = "{base}/bronze/entsoe_generation"
_LOAD_TABLE_PATH = "{base}/bronze/entsoe_load"

_SCHEMA_GENERATION = StructType(
    [
        StructField("market_id", StringType(), nullable=False),
        StructField("timestamp_utc", TimestampType(), nullable=False),
        StructField("production_type", StringType(), nullable=False),
        StructField("actual_mw", DoubleType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=False),
    ]
)

_SCHEMA_LOAD = StructType(
    [
        StructField("market_id", StringType(), nullable=False),
        StructField("timestamp_utc", TimestampType(), nullable=False),
        StructField("actual_load_mw", DoubleType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=False),
    ]
)


class GenerationRow(NamedTuple):
    """One parsed row for bronze/entsoe_generation."""

    market_id: str
    timestamp_utc: datetime  # naive UTC
    production_type: str
    actual_mw: float | None
    ingested_at: datetime  # naive UTC


class LoadRow(NamedTuple):
    """One parsed row for bronze/entsoe_load."""

    market_id: str
    timestamp_utc: datetime  # naive UTC
    actual_load_mw: float | None
    ingested_at: datetime  # naive UTC


class EntsoeBronzeWriter:
    """Parses ENTSO-E landing JSONs and MERGEs into bronze Delta tables."""

    def __init__(self) -> None:
        self._gen_table_path = _GEN_TABLE_PATH.format(base=settings.delta_base_path)
        self._load_table_path = _LOAD_TABLE_PATH.format(base=settings.delta_base_path)

    def process(
        self, market_id: str, start: date, end: date, spark: SparkSession
    ) -> None:
        """
        Parse landing JSONs for [start, end] and append to Bronze tables.

        Reads:
            {landing_base_path}/landing/entsoe/{market_id}/generation/{YYYY-MM-DD}.json
            {landing_base_path}/landing/entsoe/{market_id}/load/{YYYY-MM-DD}.json

        Writes:
            {delta_base_path}/bronze/entsoe_generation
                (Delta, partitioned by market_id)
            {delta_base_path}/bronze/entsoe_load
                (Delta, partitioned by market_id)

        Append-only with revision capture; observation keys:
            generation: (market_id, timestamp_utc, production_type)
            load:       (market_id, timestamp_utc)
        """
        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

        self._process_series(
            label="gen",
            market_id=market_id,
            start=start,
            end=end,
            spark=spark,
            ingested_at=ingested_at,
            landing_path=lambda d: _gen_landing_path(market_id, d),
            parse=_parse_landing_generation_json,
            schema=_SCHEMA_GENERATION,
            table_path=self._gen_table_path,
            key_cols=("market_id", "timestamp_utc", "production_type"),
            value_cols=("actual_mw",),
        )
        self._process_series(
            label="load",
            market_id=market_id,
            start=start,
            end=end,
            spark=spark,
            ingested_at=ingested_at,
            landing_path=lambda d: _load_landing_path(market_id, d),
            parse=_parse_landing_load_json,
            schema=_SCHEMA_LOAD,
            table_path=self._load_table_path,
            key_cols=("market_id", "timestamp_utc"),
            value_cols=("actual_load_mw",),
        )

    def _process_series(
        self,
        *,
        label: str,
        market_id: str,
        start: date,
        end: date,
        spark: SparkSession,
        ingested_at: datetime,
        landing_path: Callable[[date], Path],
        parse: Callable[[Path, str, datetime], Sequence[tuple[Any, ...]]],
        schema: StructType,
        table_path: str,
        key_cols: Sequence[str],
        value_cols: Sequence[str],
    ) -> None:
        """Parse one series' landing files and append new observations.

        Generation and load differ only in landing path, parser, schema and
        observation keys — the iterate/parse/append shape is identical.
        """
        rows: list[tuple[Any, ...]] = []
        for current in date_range(start, end):
            path = landing_path(current)
            if path.exists():
                day_rows = parse(path, market_id, ingested_at)
                rows.extend(day_rows)
                logger.info(
                    "EntsoeBronzeWriter %s: parsed %d rows for %s",
                    label,
                    len(day_rows),
                    current.isoformat(),
                )
            else:
                logger.debug(
                    "EntsoeBronzeWriter %s: no landing file for %s — skipping",
                    label,
                    current.isoformat(),
                )

        if not rows:
            logger.info(
                "EntsoeBronzeWriter %s: nothing to write for %s %s–%s",
                label,
                market_id,
                start,
                end,
            )
            return

        incoming_df = spark.createDataFrame(rows, schema=schema)
        append_new_observations(
            spark,
            incoming_df,
            table_path,
            key_cols=key_cols,
            value_cols=value_cols,
            partition_cols=("market_id",),
        )
        logger.info(
            "EntsoeBronzeWriter %s: appended new observations from %d rows to %s",
            label,
            len(rows),
            table_path,
        )


# ---------------------------------------------------------------------------
# Parsing helpers (module-level — tested directly in unit tests)
# ---------------------------------------------------------------------------


def _parse_landing_generation_json(
    path: Path,
    market_id: str,
    ingested_at: datetime,
) -> list[GenerationRow]:
    """
    Parse a per-date ENTSO-E generation landing JSON.

    Expected structure::

        {
          "market_id": "RO",
          "date": "2024-01-15",
          "bidding_zone": "10YRO-TEL------P",
          "document_type": "A75",
          "fetched_at": "...",
          "records": [
            {"timestamp_utc": "2024-01-15T00:00:00",
             "production_type": "Wind Onshore",
             "actual_mw": 1823.0},
            ...
          ]
        }

    Returns an empty list if the file is missing or malformed.
    """
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("EntsoeBronzeWriter: cannot read %s: %s", path, exc)
        return []

    rows: list[GenerationRow] = []
    for record in data.get("records", []):
        try:
            ts = datetime.fromisoformat(record["timestamp_utc"])
            production_type = str(record["production_type"])
            raw_mw = record.get("actual_mw")
            actual_mw = float(raw_mw) if raw_mw is not None else None
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning(
                "EntsoeBronzeWriter gen: skipping bad record in %s: %s", path, exc
            )
            continue
        rows.append(
            GenerationRow(
                market_id=market_id,
                timestamp_utc=ts,
                production_type=production_type,
                actual_mw=actual_mw,
                ingested_at=ingested_at,
            )
        )
    return rows


def _parse_landing_load_json(
    path: Path,
    market_id: str,
    ingested_at: datetime,
) -> list[LoadRow]:
    """
    Parse a per-date ENTSO-E load landing JSON.

    Expected structure::

        {
          "market_id": "RO",
          "date": "2024-01-15",
          "bidding_zone": "10YRO-TEL------P",
          "document_type": "A65",
          "fetched_at": "...",
          "records": [
            {"timestamp_utc": "2024-01-15T00:00:00", "actual_load_mw": 7234.0},
            ...
          ]
        }

    Returns an empty list if the file is missing or malformed.
    """
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("EntsoeBronzeWriter: cannot read %s: %s", path, exc)
        return []

    rows: list[LoadRow] = []
    for record in data.get("records", []):
        try:
            ts = datetime.fromisoformat(record["timestamp_utc"])
            raw_mw = record.get("actual_load_mw")
            actual_load_mw = float(raw_mw) if raw_mw is not None else None
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning(
                "EntsoeBronzeWriter load: skipping bad record in %s: %s", path, exc
            )
            continue
        rows.append(
            LoadRow(
                market_id=market_id,
                timestamp_utc=ts,
                actual_load_mw=actual_load_mw,
                ingested_at=ingested_at,
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def _gen_landing_path(market_id: str, d: date) -> Path:
    return entsoe_generation_json_path(settings.landing_base_path, market_id, d)


def _load_landing_path(market_id: str, d: date) -> Path:
    return entsoe_load_json_path(settings.landing_base_path, market_id, d)
