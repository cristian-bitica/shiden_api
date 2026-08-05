"""Silver fact table: fact_price.

Grain: one row per 15-minute delivery interval × market.
~35,000 rows per market-year.

Source: bronze/opcom_pzu_prices (append-only, may contain duplicates and
revisions).  Deduplication keeps the latest ingested_at per natural key.

Market-agnostic price columns:

    price_local_mwh  price in the market's settlement currency
                     (RON for RO — the currency itself lives in dim_market)
    fx_rate          EUR→local rate applied, from silver/exchange_rates
                     (already reconciled and forward-filled there)
    price_eur_mwh    price_local_mwh / fx_rate; NULL only when no rate
                     exists at all (before FX history begins)

time_id = interval_15min - 1  (OPCOM intervals are 1-indexed, local time).
date_id = YYYYMMDD integer from OPCOM delivery_date (already local).

Requires silver/exchange_rates for the market's currency.

MERGE key: (date_id, time_id, market_id).
Partition:  (market_id, date_id).
"""

from __future__ import annotations

import logging
from datetime import date

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
from shiden.processing.delta_io import deduplicate_latest, merge_upsert

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/silver/fact_price"

_SCHEMA = StructType(
    [
        StructField("date_id", IntegerType(), nullable=False),
        StructField("time_id", IntegerType(), nullable=False),
        StructField("market_id", StringType(), nullable=False),
        StructField("price_local_mwh", DoubleType(), nullable=True),
        StructField("fx_rate", DoubleType(), nullable=True),
        StructField("price_eur_mwh", DoubleType(), nullable=True),
    ]
)


class FactPriceProcessor:
    """Reads OPCOM PZU Bronze, converts to EUR via silver/exchange_rates,
    MERGEs into silver/fact_price."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)
        self._bronze_path = f"{settings.delta_base_path}/bronze/opcom_pzu_prices"
        self._fx_path = f"{settings.delta_base_path}/silver/exchange_rates"

    def process(
        self, market_id: str, start: date, end: date, spark: SparkSession
    ) -> None:
        """
        Transform OPCOM Bronze → fact_price for market_id and [start, end].

        Requires silver/exchange_rates to already exist for the given range.
        """
        market = get_market(market_id)
        start_str = start.isoformat()
        end_str = end.isoformat()

        bronze = (
            spark.read.format("delta")
            .load(self._bronze_path)
            .filter(
                (F.col("market_id") == market_id)
                & (F.col("delivery_date") >= start_str)
                & (F.col("delivery_date") <= end_str)
            )
        )

        # Keep latest observation per interval (Bronze holds revisions)
        bronze = deduplicate_latest(
            bronze, ["market_id", "delivery_date", "interval_15min"]
        )

        bronze = bronze.withColumn(
            "date_id", F.date_format("delivery_date", "yyyyMMdd").cast("int")
        ).withColumn("time_id", F.col("interval_15min") - 1)

        # EUR→local rate for this market's settlement currency.
        # silver/exchange_rates is already reconciled + forward-filled.
        fx = (
            spark.read.format("delta")
            .load(self._fx_path)
            .filter(
                (F.col("base_currency") == market.reporting_currency)
                & (F.col("quote_currency") == market.currency)
            )
            .select(F.col("date").alias("delivery_date"), F.col("rate"))
        )
        joined = bronze.join(fx, on="delivery_date", how="left").withColumnRenamed(
            "rate", "fx_rate"
        )

        # NULL fx_rate (no FX history yet) or zero (corrupt row) → NULL EUR.
        joined = joined.withColumn(
            "price_eur_mwh",
            F.when(
                F.col("fx_rate").isNotNull() & (F.col("fx_rate") != 0),
                F.col("price_lei_mwh") / F.col("fx_rate"),
            ).otherwise(F.lit(None).cast(DoubleType())),
        ).withColumnRenamed("price_lei_mwh", "price_local_mwh")

        incoming_df = joined.select(
            "date_id", "time_id", "market_id",
            "price_local_mwh", "fx_rate", "price_eur_mwh",
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
            "FactPriceProcessor: merged %d rows for %s %s–%s",
            count, market_id, start_str, end_str,
        )
