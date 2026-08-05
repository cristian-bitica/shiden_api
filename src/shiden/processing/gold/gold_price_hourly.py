"""Gold table: gold_price_hourly.

Grain: one row per 1-hour interval × market.

Transforms silver/fact_price (15-min OPCOM) into an hourly Gold table:
  - Averages the four 15-min price slots within each local hour
    (both local-currency and EUR prices).
  - Carries the fx_rate applied (constant within a day).
  - Joins dim_time to add is_peak and time_label.

FX gap-filling is NOT done here: silver/exchange_rates is already
reconciled and forward-filled (with a lookback seed across window edges),
and fact_price carries the applied rate.  An earlier design forward-filled
within this job's processing window, which permanently nulled weekend EUR
prices whenever a weekend fell at the start of the rolling window.

Write strategy: replaceWhere on (market_id, date_id) partition range so the
job is idempotent and safe to re-run for a rolling window.

Partition: (market_id, date_id).
"""

from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from shiden.config.settings import settings
from shiden.dates import date_to_id
from shiden.processing.delta_io import replace_date_range

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/gold/price_hourly"


class GoldPriceHourlyProcessor:
    """Build gold/price_hourly from silver fact_price + dim_time."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)
        self._price_path = f"{settings.delta_base_path}/silver/fact_price"
        self._time_path = f"{settings.delta_base_path}/silver/dim_time"

    def process(
        self, market_id: str, start: date, end: date, spark: SparkSession
    ) -> None:
        """
        Compute gold/price_hourly for market_id over [start, end] and write.

        Requires silver/fact_price and silver/dim_time.
        """
        start_id = date_to_id(start)
        end_id = date_to_id(end)

        # ── 1. Read Silver fact_price (15-min) for the range ─────────────
        price = (
            spark.read.format("delta").load(self._price_path)
            .filter(
                (F.col("market_id") == market_id)
                & (F.col("date_id") >= start_id)
                & (F.col("date_id") <= end_id)
            )
        )

        # ── 2. Aggregate 15-min → hourly (time_id = 4 × local_hour) ──────
        # fact_price uses time_id = interval_15min - 1 (0-95); the four
        # slots of one hour bucket to the hour-start id: floor(t/4)*4.
        # floor() is required — Spark `/` is true division, so a bare
        # (t / 4) * 4 would round-trip every value unchanged.
        price = price.withColumn(
            "hour_time_id",
            (F.floor(F.col("time_id") / 4) * 4).cast(IntegerType()),
        )
        hourly = (
            price.groupBy("date_id", "hour_time_id", "market_id")
            .agg(
                F.avg("price_local_mwh").alias("price_local_mwh"),
                F.avg("price_eur_mwh").alias("price_eur_mwh"),
                F.first("fx_rate", ignorenulls=True).alias("fx_rate"),
            )
            .withColumnRenamed("hour_time_id", "time_id")
        )

        # ── 3. Join dim_time for is_peak and time_label ───────────────────
        dim_time = (
            spark.read.format("delta").load(self._time_path)
            .select("time_id", "is_peak", "time_label")
        )
        result = hourly.join(dim_time, on="time_id", how="left")

        incoming_df = result.select(
            "date_id", "time_id", "market_id",
            "price_local_mwh", "fx_rate", "price_eur_mwh",
            "is_peak", "time_label",
        )

        count = incoming_df.count()
        replace_date_range(
            spark, incoming_df, self._table_path, market_id, start_id, end_id
        )
        logger.info(
            "GoldPriceHourlyProcessor: wrote %d rows for %s %s–%s",
            count, market_id, start, end,
        )
