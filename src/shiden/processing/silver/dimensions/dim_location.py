"""Silver dimension: dim_location.

One row per weather measurement point.  Populated from the MARKETS config
registry (weather_locations tuple on each MarketConfig).

location_id is a deterministic surrogate: hash of (market_id, location_name)
truncated to a positive 6-digit integer.  Stable across runs without needing
to read the existing table state.

MERGE key: (market_id, location_name).
Partition:  none (O(10) rows per market at full scale).
"""

from __future__ import annotations

import hashlib
import logging
from typing import NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from shiden.config.markets import MARKETS
from shiden.config.settings import settings
from shiden.processing.delta_io import merge_upsert

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/silver/dim_location"

_SCHEMA = StructType(
    [
        StructField("location_id", IntegerType(), nullable=False),
        StructField("location_name", StringType(), nullable=False),
        StructField("market_id", StringType(), nullable=False),
        StructField("latitude", DoubleType(), nullable=False),
        StructField("longitude", DoubleType(), nullable=False),
        StructField("wind_weight", DoubleType(), nullable=False),
        StructField("solar_weight", DoubleType(), nullable=False),
        StructField("demand_weight", DoubleType(), nullable=False),
    ]
)


class LocationRow(NamedTuple):
    location_id: int
    location_name: str
    market_id: str
    latitude: float
    longitude: float
    wind_weight: float
    solar_weight: float
    demand_weight: float


class DimLocationProcessor:
    """Writes dim_location from the MARKETS config registry."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)

    def process(self, spark: SparkSession) -> None:
        rows = _build_rows()
        incoming_df = spark.createDataFrame(rows, schema=_SCHEMA)
        merge_upsert(
            spark,
            incoming_df,
            self._table_path,
            key_cols=("market_id", "location_name"),
        )
        logger.info(
            "DimLocationProcessor: merged %d rows into %s", len(rows), self._table_path
        )


# ---------------------------------------------------------------------------
# Row builder (module-level, unit-testable)
# ---------------------------------------------------------------------------


def _location_id(market_id: str, location_name: str) -> int:
    """Stable surrogate key: positive 6-digit integer derived from MD5.

    Uses hashlib.md5 (not Python's built-in hash()) to guarantee the same
    value across all processes and interpreter restarts regardless of
    PYTHONHASHSEED.  Built-in hash() is randomised per process and would
    break FK joins between fact_weather and dim_location on every restart.
    """
    digest = hashlib.md5(f"{market_id}::{location_name}".encode()).hexdigest()
    return int(digest[:8], 16) % 900_000 + 100_000


def _build_rows() -> list[LocationRow]:
    rows = []
    for market_id, cfg in MARKETS.items():
        for loc in cfg.weather_locations:
            rows.append(
                LocationRow(
                    location_id=_location_id(market_id, loc.name),
                    location_name=loc.name,
                    market_id=market_id,
                    latitude=loc.lat,
                    longitude=loc.lon,
                    wind_weight=loc.wind_weight,
                    solar_weight=loc.solar_weight,
                    demand_weight=loc.demand_weight,
                )
            )
    return rows
