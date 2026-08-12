"""Gold table: gold_price_hourly.

Grain: one row per 1-hour interval × market.

Transforms silver/fact_price (15-min OPCOM) into an hourly Gold table:
  - Averages the four 15-min price slots within each local hour
    (both local-currency and EUR prices).
  - Carries the fx_rate applied (constant within a day).
  - Joins dim_datetime on the UTC hour to add is_peak and time_label,
    so DST days yield 23 or 25 delivery hours rather than always 24.

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

from shiden.config.settings import settings
from shiden.dates import date_to_id
from shiden.processing.delta_io import replace_date_range

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/gold/price_hourly"


class GoldPriceHourlyProcessor:
    """Build gold/price_hourly from silver fact_price + dim_datetime."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)
        self._price_path = f"{settings.delta_base_path}/silver/fact_price"
        self._time_path = f"{settings.delta_base_path}/silver/dim_datetime"

    def process(
        self, market_id: str, start: date, end: date, spark: SparkSession
    ) -> None:
        """
        Compute gold/price_hourly for market_id over [start, end] and write.

        Requires silver/fact_price and silver/dim_datetime.
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

        # ── 2. Aggregate 15-min → hourly, bucketed on the UTC hour ────────
        # Bucketing on the local clock position would merge the two 03:00
        # hours of the autumn changeover into one and emit 24 rows for a
        # 25-hour day. The UTC hour is unambiguous, so a DST day correctly
        # yields 23 or 25 delivery hours.
        price = price.withColumn(
            "hour_start_utc", F.date_trunc("hour", F.col("timestamp_utc"))
        )
        hourly = price.groupBy("hour_start_utc", "market_id").agg(
            F.avg("price_local_mwh").alias("price_local_mwh"),
            F.avg("price_eur_mwh").alias("price_eur_mwh"),
            F.first("fx_rate", ignorenulls=True).alias("fx_rate"),
        )

        # ── 3. Resolve labels from dim_datetime at the hour boundary ──────
        dim_dt = (
            spark.read.format("delta").load(self._time_path)
            .filter(F.col("is_hour_start"))
            .select(
                "market_id",
                F.col("timestamp_utc").alias("hour_start_utc"),
                "date_id",
                F.col("local_time_id").alias("time_id"),
                "is_peak",
                "time_label",
                "is_repeated_hour",
            )
        )
        result = hourly.join(dim_dt, on=["market_id", "hour_start_utc"], how="left")

        incoming_df = result.select(
            "date_id", "time_id", "hour_start_utc", "market_id",
            "price_local_mwh", "fx_rate", "price_eur_mwh",
            "is_peak", "time_label", "is_repeated_hour",
        )

        count = incoming_df.count()
        replace_date_range(
            spark, incoming_df, self._table_path, market_id, start_id, end_id
        )
        logger.info(
            "GoldPriceHourlyProcessor: wrote %d rows for %s %s–%s",
            count, market_id, start, end,
        )
