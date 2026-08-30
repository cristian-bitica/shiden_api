"""Gold table: gold_bess_signals.

Grain: one row per 15-minute delivery interval × market.

For each calendar day, ranks all 96 OPCOM 15-min intervals by EUR price and
assigns charge / discharge / idle signals for a Battery Energy Storage System
(BESS).  The signals are rule-based (no ML) and deterministic:

    CHARGE    : lowest ``charge_slots`` prices of the day  → signal = -1
    DISCHARGE : highest ``discharge_slots`` prices of the day → signal = +1
    IDLE      : everything else                             → signal =  0

Default slots: 8 charge (2h equivalent) and 8 discharge (2h equivalent).
Configurable via the processor constructor so the scheduler can override for
different market assumptions or customer tiers.

Additional columns per row:
    price_rank_asc  : 1 = cheapest interval of the day (used for CHARGE)
    price_rank_desc : 1 = most expensive interval of the day (used for DISCHARGE)
    arbitrage_spread_eur_mwh : daily max_price − daily min_price (constant per day)
    daily_min_price_eur_mwh  : cheapest interval price for the day
    daily_max_price_eur_mwh  : most expensive interval price for the day

Only rows where price_eur_mwh is non-NULL are ranked.  If the day has no EUR
prices (Silver fact_price missing, or the date precedes all FX history in
silver/exchange_rates), all rows for that day get signal=0 and NULL ranks.

Source: gold/price_hourly (hourly) is NOT used here — we go back to
silver/fact_price (15-min) so the BESS signals retain 15-min granularity,
which is the natural granularity of OPCOM day-ahead settlement.

Write strategy: replaceWhere on (market_id, date_id).
Partition: (market_id, date_id).
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
from pyspark.sql.window import Window

from shiden.config.markets import get_market
from shiden.config.settings import settings
from shiden.dates import date_to_id
from shiden.processing.delta_io import replace_date_range

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/gold/bess_signals"

_SCHEMA = StructType(
    [
        StructField("date_id",                   IntegerType(), nullable=False),
        StructField("time_id",                   IntegerType(), nullable=False),
        StructField("market_id",                 StringType(),  nullable=False),
        StructField("time_label",                StringType(),  nullable=False),
        StructField("price_local_mwh",           DoubleType(),  nullable=True),
        StructField("price_eur_mwh",             DoubleType(),  nullable=True),
        StructField("price_rank_asc",            IntegerType(), nullable=True),
        StructField("price_rank_desc",           IntegerType(), nullable=True),
        StructField("signal",                    IntegerType(), nullable=False),
        StructField("daily_min_price_eur_mwh",   DoubleType(),  nullable=True),
        StructField("daily_max_price_eur_mwh",   DoubleType(),  nullable=True),
        StructField("arbitrage_spread_eur_mwh",  DoubleType(),  nullable=True),
    ]
)

# Default BESS sizing assumption: 2-hour charge + 2-hour discharge window.
# 8 slots × 15 min = 2 hours.
_DEFAULT_CHARGE_SLOTS    = 8
_DEFAULT_DISCHARGE_SLOTS = 8


class GoldBessSignalsProcessor:
    """Build gold/bess_signals from silver/fact_price + silver/dim_datetime."""

    def __init__(
        self,
        charge_slots: int    = _DEFAULT_CHARGE_SLOTS,
        discharge_slots: int = _DEFAULT_DISCHARGE_SLOTS,
    ) -> None:
        self._table_path     = _TABLE_PATH.format(base=settings.delta_base_path)
        self._price_path     = f"{settings.delta_base_path}/silver/fact_price"
        self._time_path      = f"{settings.delta_base_path}/silver/dim_datetime"
        self.charge_slots    = charge_slots
        self.discharge_slots = discharge_slots

    def process(
        self, market_id: str, start: date, end: date, spark: SparkSession
    ) -> None:
        """
        Compute gold/bess_signals for market_id over [start, end] and write.

        Requires silver/fact_price and silver/dim_datetime.  fact_price carries
        price_eur_mwh converted via silver/exchange_rates (reconciled and
        forward-filled), so EUR prices are NULL only before FX history begins.
        """
        start_id = date_to_id(start)
        end_id = date_to_id(end)

        # ── 1. Read Silver fact_price (15-min) ────────────────────────────────
        price = (
            spark.read.format("delta").load(self._price_path)
            .filter(
                (F.col("market_id") == market_id)
                & (F.col("date_id") >= start_id)
                & (F.col("date_id") <= end_id)
            )
            # fact_price keys on the instant; the clock position it carries is
            # named local_time_id. Gold's published column stays time_id.
            .withColumnRenamed("local_time_id", "time_id")
        )

        # ── 2. Join dim_datetime for the clock label ──────────────────────────
        # Joined on the UTC instant, not on a clock position: on the autumn
        # changeover local 03:00 occurs twice, so a clock-position join would
        # duplicate every row of that hour and mislabel the rest of the day.
        dim_dt = (
            spark.read.format("delta").load(self._time_path)
            .filter(F.col("timezone") == get_market(market_id).timezone)
            .select("timestamp_utc", "time_label", "is_repeated_hour")
        )
        price = price.join(dim_dt, on="timestamp_utc", how="left")

        # ── 3. Day-level windows for ranking and daily stats ──────────────────
        # timestamp_utc is the secondary sort key so equal prices rank
        # deterministically (earlier slot wins) — matching the stable sort in
        # the pure-Python assign_signals() below. It replaces time_id, which
        # is not monotonic across a DST changeover.
        day_window = Window.partitionBy("market_id", "date_id")
        rank_asc_window = day_window.orderBy(
            F.col("price_eur_mwh").asc_nulls_last(), F.col("timestamp_utc").asc()
        )
        rank_desc_window = day_window.orderBy(
            F.col("price_eur_mwh").desc_nulls_last(), F.col("timestamp_utc").desc()
        )

        price = (
            price
            .withColumn("price_rank_asc",  F.row_number().over(rank_asc_window))
            .withColumn("price_rank_desc", F.row_number().over(rank_desc_window))
            .withColumn(
                "daily_min_price_eur_mwh",
                F.min("price_eur_mwh").over(day_window),
            )
            .withColumn(
                "daily_max_price_eur_mwh",
                F.max("price_eur_mwh").over(day_window),
            )
        )

        # ── 4. Assign signals ─────────────────────────────────────────────────
        # Rows without EUR price get signal=0 and NULL ranks (nullify ranks
        # set above for those rows so the API consumer can filter on them).
        charge_slots    = self.charge_slots
        discharge_slots = self.discharge_slots

        price = price.withColumn(
            "signal",
            F.when(
                F.col("price_eur_mwh").isNull(),
                F.lit(0),
            ).when(
                F.col("price_rank_asc") <= charge_slots,
                F.lit(-1),
            ).when(
                F.col("price_rank_desc") <= discharge_slots,
                F.lit(1),
            ).otherwise(F.lit(0)),
        ).withColumn(
            "price_rank_asc",
            F.when(
                F.col("price_eur_mwh").isNotNull(), F.col("price_rank_asc")
            ).otherwise(F.lit(None).cast(IntegerType())),
        ).withColumn(
            "price_rank_desc",
            F.when(
                F.col("price_eur_mwh").isNotNull(), F.col("price_rank_desc")
            ).otherwise(F.lit(None).cast(IntegerType())),
        )

        # ── 5. Arbitrage spread ───────────────────────────────────────────────
        price = price.withColumn(
            "arbitrage_spread_eur_mwh",
            F.col("daily_max_price_eur_mwh") - F.col("daily_min_price_eur_mwh"),
        )

        result = price.select(
            "date_id", "time_id", "market_id", "time_label",
            "price_local_mwh", "price_eur_mwh",
            "price_rank_asc", "price_rank_desc",
            "signal",
            "daily_min_price_eur_mwh", "daily_max_price_eur_mwh",
            "arbitrage_spread_eur_mwh",
        )

        count = result.count()
        replace_date_range(
            spark, result, self._table_path, market_id, start_id, end_id
        )
        logger.info(
            "GoldBessSignalsProcessor: wrote %d rows for %s %s–%s "
            "(charge_slots=%d, discharge_slots=%d)",
            count, market_id, start, end,
            self.charge_slots, self.discharge_slots,
        )


# ---------------------------------------------------------------------------
# Pure-Python helper (unit-testable without Spark)
# ---------------------------------------------------------------------------


def assign_signals(
    prices_asc: list[float | None],
    charge_slots: int = _DEFAULT_CHARGE_SLOTS,
    discharge_slots: int = _DEFAULT_DISCHARGE_SLOTS,
) -> list[int]:
    """Return a list of signals (-1/0/1) for a day's sorted price sequence.

    Parameters
    ----------
    prices_asc:
        96-element list of EUR prices (None for missing), in time_id order
        (00:00 first, 23:45 last).
    charge_slots, discharge_slots:
        Number of intervals to mark as charge / discharge.

    Returns
    -------
    list[int]
        96-element signal list aligned with prices_asc.

    This function is the pure-Python equivalent of the Spark window logic above
    and is exposed here for unit testing and for the ML feature pipeline.
    """
    n = len(prices_asc)
    signals = [0] * n

    # Index into sorted order, skipping NULLs
    valid_indexed = [
        (p, i) for i, p in enumerate(prices_asc) if p is not None
    ]
    valid_indexed.sort(key=lambda x: x[0])  # ascending

    for rank, (_, idx) in enumerate(valid_indexed):
        if rank < charge_slots:
            signals[idx] = -1
        elif rank >= len(valid_indexed) - discharge_slots:
            signals[idx] = 1

    return signals
