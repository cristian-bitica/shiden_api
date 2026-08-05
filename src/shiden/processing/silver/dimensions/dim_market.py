"""Silver dimension: dim_market.

One row per market.  Populated directly from the MARKETS config registry —
no Bronze dependency.  Adding a new market to markets.py (including its
``name``) is the only change required to extend coverage here.

MERGE key: market_id.
Partition:  none (O(10) rows at full scale).
"""

from __future__ import annotations

import logging
from typing import NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from shiden.config.markets import MARKETS
from shiden.config.settings import settings
from shiden.processing.delta_io import merge_upsert

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/silver/dim_market"

_SCHEMA = StructType(
    [
        StructField("market_id", StringType(), nullable=False),
        StructField("market_name", StringType(), nullable=False),
        StructField("bidding_zone", StringType(), nullable=False),
        StructField("timezone", StringType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("reporting_currency", StringType(), nullable=False),
    ]
)


class MarketRow(NamedTuple):
    market_id: str
    market_name: str
    bidding_zone: str
    timezone: str
    currency: str
    reporting_currency: str


class DimMarketProcessor:
    """Writes dim_market from the MARKETS config registry."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)

    def process(self, spark: SparkSession) -> None:
        rows = [
            MarketRow(
                market_id=mid,
                market_name=cfg.name or mid,
                bidding_zone=cfg.bidding_zone,
                timezone=cfg.timezone,
                currency=cfg.currency,
                reporting_currency=cfg.reporting_currency,
            )
            for mid, cfg in MARKETS.items()
        ]
        incoming_df = spark.createDataFrame(rows, schema=_SCHEMA)
        merge_upsert(spark, incoming_df, self._table_path, key_cols=("market_id",))
        logger.info(
            "DimMarketProcessor: merged %d rows into %s", len(rows), self._table_path
        )
