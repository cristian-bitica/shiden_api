"""Silver fact table: fact_load.

Grain: one row per 1-hour interval × market (hour-start time_id).
~8,760 rows per market-year.

Source: bronze/entsoe_load.

ENTSO-E timestamps are UTC — converted to local (Europe/Bucharest) to align
with the delivery-day grain used by OPCOM prices.

time_id = local_hour × 4  (always an hour-start: 0, 4, 8, … 92).
date_id = YYYYMMDD integer from the local delivery date.

MERGE key: (date_id, time_id, market_id).
Partition:  (market_id, date_id).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from shiden.config.markets import get_market
from shiden.config.settings import settings
from shiden.processing.delta_io import (
    deduplicate_latest,
    merge_upsert,
    with_local_keys,
)

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/silver/fact_load"

_SCHEMA = StructType(
    [
        StructField("date_id", IntegerType(), nullable=False),
        StructField("time_id", IntegerType(), nullable=False),
        StructField("market_id", StringType(), nullable=False),
        StructField("actual_load_mw", DoubleType(), nullable=True),
    ]
)


class FactLoadProcessor:
    """Reads ENTSO-E load Bronze and MERGEs into silver/fact_load."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)
        self._bronze_path = f"{settings.delta_base_path}/bronze/entsoe_load"

    def process(
        self, market_id: str, start: date, end: date, spark: SparkSession
    ) -> None:
        timezone = get_market(market_id).timezone
        start_str = start.isoformat()
        end_str = end.isoformat()

        utc_start = (start - timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%S")
        utc_end = f"{end_str}T23:59:59"

        bronze = (
            spark.read.format("delta")
            .load(self._bronze_path)
            .filter(
                (F.col("market_id") == market_id)
                & (F.col("timestamp_utc") >= utc_start)
                & (F.col("timestamp_utc") <= utc_end)
            )
        )

        # Deduplicate on Bronze natural key before UTC→local conversion —
        # Bronze is append-only with revision capture, so corrected values
        # (same timestamp, different MW) appear as extra rows; keep only the
        # latest ingested one.
        bronze = deduplicate_latest(bronze, ["market_id", "timestamp_utc"])

        bronze = with_local_keys(bronze, timezone)

        start_id = int(start.strftime("%Y%m%d"))
        end_id = int(end.strftime("%Y%m%d"))
        bronze = bronze.filter(
            (F.col("date_id") >= start_id) & (F.col("date_id") <= end_id)
        )

        # ENTSO-E load is published at 15-min resolution; Silver grain is
        # hourly.  Average the four 15-min MW values within each local hour so
        # that the MERGE key (date_id, time_id, market_id) stays unique and the
        # stored value is the mean load for that hour.
        incoming_df = (
            bronze
            .groupBy("date_id", "time_id", "market_id")
            .agg(F.avg("actual_load_mw").alias("actual_load_mw"))
        )

        count = incoming_df.count()
        merge_upsert(
            spark,
            incoming_df,
            self._table_path,
            key_cols=("date_id", "time_id", "market_id"),
            partition_cols=("market_id", "date_id"),
        )
        logger.info(
            "FactLoadProcessor: merged %d rows for %s %s–%s",
            count, market_id, start_str, end_str,
        )
