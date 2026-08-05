"""Bronze processor for weather data (Open-Meteo).

Responsibility: read raw JSON files from the landing zone, explode hourly
arrays into rows, and append to the Bronze Delta table.
Append-only — no deduplication. Silver handles cleaning.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
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
from shiden.landing_paths import weather_hourly_json_path
from shiden.processing.delta_io import append_new_observations

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/bronze/weather"

_SCHEMA = StructType(
    [
        StructField("market_id", StringType(), nullable=False),
        StructField("location_name", StringType(), nullable=False),
        StructField("lat", DoubleType(), nullable=False),
        StructField("lon", DoubleType(), nullable=False),
        StructField("timestamp_utc", TimestampType(), nullable=False),
        StructField("data_type", StringType(), nullable=False),  # REALIZED | FORECAST
        StructField("temperature_2m", DoubleType(), nullable=True),
        StructField("wind_speed_10m", DoubleType(), nullable=True),
        StructField("wind_speed_100m", DoubleType(), nullable=True),
        StructField("shortwave_radiation", DoubleType(), nullable=True),
        StructField("cloud_cover", DoubleType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=False),
        StructField("source_url", StringType(), nullable=True),
    ]
)


class WeatherRow(NamedTuple):
    """One exploded hourly row written to the Bronze weather table."""

    market_id: str
    location_name: str
    lat: float
    lon: float
    timestamp_utc: datetime  # naive UTC
    data_type: str  # REALIZED (past date) | FORECAST (today or future)
    temperature_2m: float | None
    wind_speed_10m: float | None
    wind_speed_100m: float | None
    shortwave_radiation: float | None
    cloud_cover: float | None
    ingested_at: datetime  # naive UTC
    source_url: str | None


class WeatherBronzeWriter:
    """Reads weather landing JSONs, explodes to rows, appends to Bronze Delta.

    Append-only with revision capture on
    (market_id, location_name, timestamp_utc, data_type): unchanged re-runs
    write nothing; updated forecast model runs append new rows.  Silver reads
    the latest row per key by ingested_at.

    The observation key deliberately includes data_type.  The previous
    per-(location, date) skip guard did not, so once FORECAST rows existed
    for a date, the later REALIZED ingest for the same date was skipped
    forever — REALIZED training data silently never reached Bronze.
    """

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)

    def process(
        self,
        market_id: str,
        location_names: list[str],
        start: date,
        end: date,
        spark: SparkSession,
    ) -> None:
        """
        Append landing JSON files for [start, end] into the Bronze Delta table.

        Reads:  {landing_base_path}/landing/weather/{market_id}/{location}/
                {YYYY-MM-DD}.json
        Writes: {delta_base_path}/bronze/weather
                (Delta, partitioned by market_id, location_name)
        """
        rows: list[WeatherRow] = []
        ingested_at = datetime.now(timezone.utc).replace(
            tzinfo=None
        )  # naive UTC for Spark

        current = start
        while current <= end:
            for location_name in location_names:
                landing_path = weather_hourly_json_path(
                    settings.landing_base_path, market_id, location_name, current
                )
                if not landing_path.exists():
                    logger.warning(
                        "WeatherBronzeWriter: landing file not found, skipping: %s",
                        landing_path,
                    )
                    continue

                payload = json.loads(landing_path.read_text(encoding="utf-8"))
                day_rows = _parse_landing(payload, market_id, ingested_at)
                rows.extend(day_rows)
                logger.info(
                    "WeatherBronzeWriter: parsed %d rows for %s / %s",
                    len(day_rows),
                    location_name,
                    current.isoformat(),
                )

            current += timedelta(days=1)

        if not rows:
            logger.info(
                "WeatherBronzeWriter: nothing new to write for %s–%s", start, end
            )
            return

        incoming_df = spark.createDataFrame(rows, schema=_SCHEMA)
        append_new_observations(
            spark,
            incoming_df,
            self._table_path,
            key_cols=("market_id", "location_name", "timestamp_utc", "data_type"),
            value_cols=(
                "temperature_2m",
                "wind_speed_10m",
                "wind_speed_100m",
                "shortwave_radiation",
                "cloud_cover",
            ),
            partition_cols=("market_id", "location_name"),
        )

        logger.info(
            "WeatherBronzeWriter: appended new observations from %d rows to %s",
            len(rows),
            self._table_path,
        )


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_landing(
    payload: dict[str, Any], market_id: str, ingested_at: datetime
) -> list[WeatherRow]:
    """
    Explode a single landing JSON file into one WeatherRow per hourly timestamp.

    The landing JSON contains:
      hourly.time            — list of "YYYY-MM-DDTHH:MM" UTC strings
      hourly.temperature_2m  — list of floats (or null)
      data_type              — "REALIZED" (past date) or "FORECAST" (today/future)
      source_url             — exact API URL used during fetch (for lineage)
      ... etc.
    """
    location_name = payload["location"]
    lat = float(payload["lat"])
    lon = float(payload["lon"])
    data_type: str = payload.get("data_type", "REALIZED")  # default for legacy files
    hourly = payload["hourly"]
    times = hourly["time"]
    source_url: str | None = payload.get("source_url")

    rows: list[WeatherRow] = []
    for i, ts_str in enumerate(times):
        # ts_str is "YYYY-MM-DDTHH:MM" in UTC — parse directly as naive UTC
        ts_utc = datetime.fromisoformat(ts_str)

        rows.append(
            WeatherRow(
                market_id=market_id,
                location_name=location_name,
                lat=lat,
                lon=lon,
                timestamp_utc=ts_utc,
                data_type=data_type,
                temperature_2m=_maybe_float(hourly.get("temperature_2m"), i),
                wind_speed_10m=_maybe_float(hourly.get("wind_speed_10m"), i),
                wind_speed_100m=_maybe_float(hourly.get("wind_speed_100m"), i),
                shortwave_radiation=_maybe_float(hourly.get("shortwave_radiation"), i),
                cloud_cover=_maybe_float(hourly.get("cloud_cover"), i),
                ingested_at=ingested_at,
                source_url=source_url,
            )
        )
    return rows


def _maybe_float(series: list[Any] | None, i: int) -> float | None:
    if series is None or i >= len(series):
        return None
    v = series[i]
    return float(v) if v is not None else None
