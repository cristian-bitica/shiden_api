"""Silver fact table: fact_weather.

Grain: one row per 1-hour interval × location × data_type.
~52,560 rows per Romania-year (24 h × 365 days × 6 locations; doubled for
REALIZED/FORECAST overlap window where both types coexist).

Source: bronze/weather (append-only, deduplication required).

Column renames from Bronze → Silver:
    temperature_2m       → temperature_c
    wind_speed_100m      → wind_speed_ms       (turbine hub height)
    shortwave_radiation  → solar_irradiance_wm2
    cloud_cover          → cloud_cover_pct

wind_speed_10m is dropped — Silver uses hub-height speed for generation
modelling.  Kept in Bronze for completeness.

Open-Meteo timestamps are UTC.
Converted to local time for date_id / time_id to match delivery-day grain.

Requires silver/dim_location to exist (for location_id FK lookup).

MERGE key: (date_id, time_id, location_id, data_type).
Partition:  (location_id, date_id).
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
    TimestampType,
)

from shiden.config.markets import get_market
from shiden.config.settings import settings
from shiden.processing.delta_io import (
    deduplicate_latest,
    merge_upsert,
    with_local_keys,
)

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/silver/fact_weather"

_SCHEMA = StructType(
    [
        StructField("date_id", IntegerType(), nullable=False),
        StructField("time_id", IntegerType(), nullable=False),
        # Grain key: (date_id, time_id) is ambiguous on the autumn DST
        # changeover, where local 03:00 occurs twice.
        StructField("hour_start_utc", TimestampType(), nullable=False),
        StructField("location_id", IntegerType(), nullable=False),
        StructField("data_type", StringType(), nullable=False),
        StructField("temperature_c", DoubleType(), nullable=True),
        StructField("wind_speed_ms", DoubleType(), nullable=True),
        StructField("solar_irradiance_wm2", DoubleType(), nullable=True),
        StructField("cloud_cover_pct", DoubleType(), nullable=True),
    ]
)


class FactWeatherProcessor:
    """Reads weather Bronze, resolves location IDs, MERGEs into silver/fact_weather."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)
        self._bronze_path = f"{settings.delta_base_path}/bronze/weather"
        self._dim_loc_path = f"{settings.delta_base_path}/silver/dim_location"

    def process(
        self, market_id: str, start: date, end: date, spark: SparkSession
    ) -> None:
        """
        Transform weather Bronze → fact_weather for market_id and [start, end].

        Processes both REALIZED and FORECAST data_type rows.
        Requires silver/dim_location to already exist.
        """
        timezone = get_market(market_id).timezone
        start_str = start.isoformat()
        end_str = end.isoformat()

        # Widen UTC window by 4 h on the left (EET=UTC+2, EEST=UTC+3) so that
        # local midnight–03:00 rows are not missed.  date_id filter after
        # UTC→local conversion trims any rows outside [start, end].
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

        # Deduplicate: keep latest ingested row per
        # (timestamp_utc, location_name, data_type)
        bronze = deduplicate_latest(
            bronze, ["market_id", "location_name", "timestamp_utc", "data_type"]
        )

        # UTC → local keys
        bronze = with_local_keys(bronze, timezone)

        # Trim to requested local date range
        start_id = int(start.strftime("%Y%m%d"))
        end_id = int(end.strftime("%Y%m%d"))
        bronze = bronze.filter(
            (F.col("date_id") >= start_id) & (F.col("date_id") <= end_id)
        )

        # Rename measurement columns
        bronze = (
            bronze.withColumnRenamed("temperature_2m", "temperature_c")
            .withColumnRenamed("wind_speed_100m", "wind_speed_ms")
            .withColumnRenamed("shortwave_radiation", "solar_irradiance_wm2")
            .withColumnRenamed("cloud_cover", "cloud_cover_pct")
        )

        # Resolve location_id
        dim_loc = spark.read.format("delta").load(self._dim_loc_path).filter(
            F.col("market_id") == market_id
        ).select("location_id", "location_name")

        # cache: consumed by two actions (unknown-location count + merge)
        joined = bronze.join(dim_loc, on="location_name", how="left").cache()

        unknown = joined.filter(F.col("location_id").isNull())
        unknown_count = unknown.count()
        if unknown_count > 0:
            unknown_names = [
                r["location_name"]
                for r in unknown.select("location_name").distinct().collect()
            ]
            logger.warning(
                "FactWeatherProcessor: dropping %d rows for unknown locations %s "
                "— add them to MARKETS weather_locations config",
                unknown_count,
                unknown_names,
            )

        joined = joined.filter(F.col("location_id").isNotNull())

        incoming_df = joined.select(
            "date_id",
            "time_id",
            "hour_start_utc",
            "location_id",
            "data_type",
            "temperature_c",
            "wind_speed_ms",
            "solar_irradiance_wm2",
            "cloud_cover_pct",
        )

        count = incoming_df.count()
        merge_upsert(
            spark,
            incoming_df,
            self._table_path,
            key_cols=("hour_start_utc", "location_id", "data_type"),
            partition_cols=("location_id", "date_id"),
        )
        logger.info(
            "FactWeatherProcessor: merged %d rows for %s %s–%s",
            count, market_id, start_str, end_str,
        )
