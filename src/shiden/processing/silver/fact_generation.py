"""Silver fact table: fact_generation.

Grain: one row per 1-hour interval × production_type × market.
~131,000 rows per market-year (24 h × 365 days × ~15 types for Romania).

Source: bronze/entsoe_generation (long format: one row per timestamp × type).
Long format is preserved in Silver; wide pivoting is deferred to Gold.

ENTSO-E timestamps are UTC — converted to Europe/Bucharest local time to
compute date_id and time_id, matching OPCOM's delivery-day grain.

time_id = local_hour × 4  (always an hour-start: 0, 4, 8, … 92).
date_id = YYYYMMDD integer from the local delivery date.

Requires silver/dim_production_type to exist (for the FK lookup).

MERGE key: (date_id, time_id, market_id, production_type_id).
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

_TABLE_PATH = "{base}/silver/fact_generation"

_SCHEMA = StructType(
    [
        StructField("date_id", IntegerType(), nullable=False),
        StructField("time_id", IntegerType(), nullable=False),
        StructField("market_id", StringType(), nullable=False),
        StructField("production_type_id", IntegerType(), nullable=False),
        StructField("actual_mw", DoubleType(), nullable=True),
    ]
)


class FactGenerationProcessor:
    """Reads ENTSO-E generation Bronze, resolves type IDs, MERGEs into
    silver/fact_generation."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)
        self._bronze_path = f"{settings.delta_base_path}/bronze/entsoe_generation"
        self._dim_pt_path = f"{settings.delta_base_path}/silver/dim_production_type"

    def process(
        self, market_id: str, start: date, end: date, spark: SparkSession
    ) -> None:
        """
        Transform ENTSO-E generation Bronze → fact_generation for [start, end].

        Requires silver/dim_production_type to already exist.
        """
        timezone = get_market(market_id).timezone
        start_str = start.isoformat()
        end_str = end.isoformat()

        # Widen UTC window by 4 h on the left to capture local midnight–03:00
        # (EET = UTC+2, EEST = UTC+3; 4 h covers the worst-case offset).
        # After UTC→local conversion, rows from the widened window that map to
        # dates before `start` or after `end` are trimmed by the date_id filter
        # below — they would otherwise be inserted outside the requested range.
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

        # Bronze is append-only with revision capture — keep only the latest
        # observation per natural key before aggregating.
        bronze = deduplicate_latest(
            bronze, ["market_id", "timestamp_utc", "production_type"]
        )

        # Convert UTC → local time for date/time keys
        bronze = with_local_keys(bronze, timezone)

        # Only keep local dates within the requested window
        start_id = int(start.strftime("%Y%m%d"))
        end_id = int(end.strftime("%Y%m%d"))
        bronze = bronze.filter(
            (F.col("date_id") >= start_id) & (F.col("date_id") <= end_id)
        )

        # Resolve production_type_id via dim lookup
        dim_pt = spark.read.format("delta").load(self._dim_pt_path).select(
            "production_type_id", "production_type_name"
        )
        joined = bronze.join(
            dim_pt,
            bronze["production_type"] == dim_pt["production_type_name"],
            how="left",
        ).cache()  # cache before multiple actions (unknown count + merge)

        # Drop rows where type lookup failed — log so it's not silent data loss
        unknown = joined.filter(F.col("production_type_id").isNull())
        unknown_count = unknown.count()
        if unknown_count > 0:
            unknown_types = [
                r["production_type"]
                for r in unknown.select("production_type").distinct().collect()
            ]
            logger.warning(
                "FactGenerationProcessor: dropping %d rows for unknown types %s "
                "— run DimProductionTypeProcessor to register them",
                unknown_count,
                unknown_types,
            )

        joined = joined.filter(F.col("production_type_id").isNotNull())

        # ENTSO-E generation is published at 15-min resolution; Silver grain is
        # hourly.  Average the four 15-min MW values within each local hour so
        # the MERGE key (date_id, time_id, market_id, production_type_id) stays
        # unique and the stored value is the mean generation for that hour.
        incoming_df = (
            joined
            .groupBy("date_id", "time_id", "market_id", "production_type_id")
            .agg(F.avg("actual_mw").alias("actual_mw"))
        )

        count = incoming_df.count()
        merge_upsert(
            spark,
            incoming_df,
            self._table_path,
            key_cols=("date_id", "time_id", "market_id", "production_type_id"),
            partition_cols=("market_id", "date_id"),
        )
        logger.info(
            "FactGenerationProcessor: merged %d rows for %s %s–%s",
            count, market_id, start_str, end_str,
        )
