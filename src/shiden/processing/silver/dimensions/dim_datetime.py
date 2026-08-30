"""Silver dimension: dim_datetime.

Replaces the old 96-row ``dim_time``. Grain is one row per settlement
interval per timezone -- roughly 35,000 rows per zone-year -- rather than
one row per clock position.

Why the grain changed
---------------------
``dim_time`` was a static 96-row table mapping ``time_id`` to a clock label
and a peak flag. That works only if interval N always means the same wall
clock time, which is false on the two DST changeovers: OPCOM numbers
intervals by elapsed slot within the local day, so its own peak window moves
(33-80 normally, 29-76 on 2026-03-29, 37-84 on 2025-10-26). A static table
mislabelled roughly 80 of ~96 intervals on those days and had no rows at all
for intervals 97-100.

A UTC anchor cannot live in a time-of-day dimension either, because the
local-to-UTC offset depends on the date -- 08:00 local is 06:00Z in winter
and 05:00Z in summer. So the dimension has to carry the date, which makes it
a datetime dimension.

Keys
----
``(timezone, timestamp_utc)`` is the primary key. The UTC instant is the
only safe one: on the fall-back day local 03:00 occurs twice, so
``(date_id, local_time_id)`` is genuinely non-unique and anything keyed on it
collapses two distinct hours into one.

``(timezone, date_id, interval_of_day)`` is an equally valid alternate key and
is what price data joins on, since OPCOM publishes interval numbers.

Keyed by IANA timezone, not by market. The axis is a property of the zone:
what instants exist in a local day is decided by the tz rules and nothing
else. European power markets routinely split one country into several bidding
zones -- Italy has ~7, Sweden 4, Norway 5, Denmark 2 -- and market-keying
would store an identical axis once per zone.

Note this does *not* deduplicate two countries with matching rules: Greece is
Europe/Athens and Romania is Europe/Bucharest, distinct tzdb entries with
identical offsets, so each keeps its own rows. That is deliberate. Collapsing
them would mean keying on an offset-rule signature, which silently breaks the
moment tzdb diverges -- as it would if the EU abolishes seasonal clock
changes and member states choose differently.

``is_peak`` deliberately does not live here. Peak is a market convention
(OPCOM publishes 08:00-20:00; other operators differ), so two markets sharing
a timezone could disagree. Peak bounds live on dim_market and the flag is
derived where it is used.

Deliberately no ``local_timestamp`` column
------------------------------------------
The session timezone is pinned to UTC, so a naive local timestamp stored in a
TIMESTAMP column would read back as though it were UTC -- the single most
common way this kind of table goes wrong. ``date_id`` + ``time_label`` +
``is_repeated_hour`` determine the local wall clock unambiguously without
inviting that mistake.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import NamedTuple
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from shiden.config.markets import MARKETS
from shiden.config.settings import settings
from shiden.dates import date_range
from shiden.processing.delta_io import merge_upsert
from shiden.timeaxis import day_intervals

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/silver/dim_datetime"

_SCHEMA = StructType(
    [
        StructField("timezone", StringType(), nullable=False),
        StructField("timestamp_utc", TimestampType(), nullable=False),
        StructField("date_id", IntegerType(), nullable=False),
        StructField("interval_of_day", IntegerType(), nullable=False),
        StructField("local_time_id", IntegerType(), nullable=False),
        StructField("local_hour", IntegerType(), nullable=False),
        StructField("quarter_of_hour", IntegerType(), nullable=False),
        StructField("time_label", StringType(), nullable=False),
        StructField("utc_offset_minutes", IntegerType(), nullable=False),
        StructField("is_dst", BooleanType(), nullable=False),
        StructField("is_repeated_hour", BooleanType(), nullable=False),
        StructField("is_hour_start", BooleanType(), nullable=False),
    ]
)


class DateTimeRow(NamedTuple):
    """Mirrors _SCHEMA field-for-field."""

    timezone: str
    timestamp_utc: object
    date_id: int
    interval_of_day: int
    local_time_id: int
    local_hour: int
    quarter_of_hour: int
    time_label: str
    utc_offset_minutes: int
    is_dst: bool
    is_repeated_hour: bool
    is_hour_start: bool


class DimDateTimeProcessor:
    """Generates and MERGEs interval rows into silver/dim_datetime."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)

    def process(
        self,
        start: date,
        end: date,
        spark: SparkSession,
        timezones: list[str] | None = None,
    ) -> None:
        """Generate rows for [start, end] local delivery dates and MERGE.

        Defaults to the distinct timezones of every configured market, so
        adding a bidding zone in an existing zone costs nothing.
        """
        zones = (
            timezones
            if timezones is not None
            else sorted({cfg.timezone for cfg in MARKETS.values()})
        )
        rows = [row for tz_name in zones for row in make_rows(tz_name, start, end)]

        if not rows:
            logger.warning(
                "DimDateTimeProcessor: nothing to write for %s–%s", start, end
            )
            return

        incoming_df = spark.createDataFrame(rows, schema=_SCHEMA)
        merge_upsert(
            spark,
            incoming_df,
            self._table_path,
            key_cols=("timezone", "timestamp_utc"),
            partition_cols=("timezone", "date_id"),
        )
        logger.info(
            "DimDateTimeProcessor: merged %d rows for %s over %s–%s",
            len(rows),
            ",".join(zones),
            start,
            end,
        )


# ---------------------------------------------------------------------------
# Row factory (module-level, unit-testable without Spark)
# ---------------------------------------------------------------------------


def make_rows(tz_name: str, start: date, end: date) -> list[DateTimeRow]:
    """Build every interval row for one timezone over [start, end]."""
    tz = ZoneInfo(tz_name)

    rows: list[DateTimeRow] = []
    for local_date in date_range(start, end):
        for iv in day_intervals(local_date, tz):
            rows.append(
                DateTimeRow(
                    timezone=tz_name,
                    # Naive UTC: the Spark session timezone is pinned to UTC,
                    # so a naive value round-trips as the same instant.
                    timestamp_utc=iv.timestamp_utc.replace(tzinfo=None),
                    date_id=iv.date_id,
                    interval_of_day=iv.interval_of_day,
                    local_time_id=iv.local_time_id,
                    local_hour=iv.local_hour,
                    quarter_of_hour=iv.quarter_of_hour,
                    time_label=iv.time_label,
                    utc_offset_minutes=iv.utc_offset_minutes,
                    is_dst=iv.is_dst,
                    is_repeated_hour=iv.is_repeated_hour,
                    is_hour_start=iv.is_hour_start,
                )
            )
    return rows
