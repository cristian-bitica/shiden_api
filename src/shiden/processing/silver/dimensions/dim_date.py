"""Silver dimension: dim_date.

One row per calendar day: Romanian public holiday flag, season, ISO week,
quarter and weekday attributes.

Purely calendar-derived — FX rates live in silver/exchange_rates, the
unified reconciled rate table, not here.  (An earlier design stored an
eur_ron_rate attribute on this dimension, which hardcoded Romania into a
conformed dimension shared by all markets.)

MERGE key: date_id (YYYYMMDD integer).
Partition:  none (O(365) rows/year — small enough to broadcast-join).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from shiden.config.settings import settings
from shiden.dates import date_range
from shiden.dates import date_to_id as _date_to_id
from shiden.processing.delta_io import merge_upsert

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/silver/dim_date"

_SCHEMA = StructType(
    [
        StructField("date_id", IntegerType(), nullable=False),
        StructField("full_date", DateType(), nullable=False),
        StructField("year", IntegerType(), nullable=False),
        StructField("month", IntegerType(), nullable=False),
        StructField("day_of_month", IntegerType(), nullable=False),
        StructField("day_of_week", IntegerType(), nullable=False),   # 1=Mon..7=Sun
        StructField("week_of_year", IntegerType(), nullable=False),  # ISO week
        StructField("quarter", IntegerType(), nullable=False),
        StructField("is_weekday", BooleanType(), nullable=False),
        StructField("is_ro_holiday", BooleanType(), nullable=False),
        StructField("season", StringType(), nullable=False),
    ]
)


class DateRow(NamedTuple):
    date_id: int
    full_date: date
    year: int
    month: int
    day_of_month: int
    day_of_week: int
    week_of_year: int
    quarter: int
    is_weekday: bool
    is_ro_holiday: bool
    season: str


class DimDateProcessor:
    """Generates and MERGEs calendar rows into silver/dim_date."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)

    def process(self, start: date, end: date, spark: SparkSession) -> None:
        """Generate dim_date rows for [start, end] and MERGE into the table."""
        holidays = _ro_holidays_set(start.year, end.year)
        rows = [_make_date_row(d, holidays) for d in date_range(start, end)]

        incoming_df = spark.createDataFrame(rows, schema=_SCHEMA)
        merge_upsert(spark, incoming_df, self._table_path, key_cols=("date_id",))
        logger.info(
            "DimDateProcessor: merged %d rows into %s", len(rows), self._table_path
        )


# ---------------------------------------------------------------------------
# Calendar helpers (module-level, unit-testable)
# ---------------------------------------------------------------------------


def _season(month: int) -> str:
    if month in (12, 1, 2):
        return "Winter"
    if month in (3, 4, 5):
        return "Spring"
    if month in (6, 7, 8):
        return "Summer"
    return "Autumn"


def _orthodox_easter(year: int) -> date:
    """Return Orthodox Easter date (Gregorian) for the given year.

    Uses the Julian calendar Meeus algorithm, then adds 13 days for the
    Julian→Gregorian offset (valid for 1900–2099).

    Raises ValueError if year is outside the supported range.
    """
    if not (1900 <= year <= 2099):
        raise ValueError(
            f"_orthodox_easter: algorithm valid for 1900–2099, got {year}"
        )
    a = year % 4
    b = year % 7
    c = year % 19
    d = (19 * c + 15) % 30
    e = (2 * a + 4 * b - d + 34) % 7
    month = (d + e + 114) // 31
    day = (d + e + 114) % 31 + 1
    julian_easter = date(year, month, day)
    return julian_easter + timedelta(days=13)


def _ro_holidays_set(year_from: int, year_to: int) -> set[date]:
    """Return the set of Romanian public holidays from year_from to year_to
    inclusive."""
    holidays: set[date] = set()
    for year in range(year_from, year_to + 1):
        easter = _orthodox_easter(year)
        if year >= 2024:
            # Law 52/2023: Epiphany (Bobotează) and St John the Baptist are
            # legal holidays starting with 2024.
            holidays.add(date(year, 1, 6))
            holidays.add(date(year, 1, 7))
        holidays.update(
            [
                # Fixed holidays
                date(year, 1, 1),   # New Year's Day
                date(year, 1, 2),   # New Year's (second day)
                date(year, 1, 24),  # Unification Day
                date(year, 5, 1),   # Labour Day
                date(year, 6, 1),   # Children's Day
                date(year, 8, 15),  # Assumption of Mary
                date(year, 11, 30), # St Andrew's Day
                date(year, 12, 1),  # National Day (Romania)
                date(year, 12, 25), # Christmas Day
                date(year, 12, 26), # Christmas (second day)
                # Easter-dependent
                easter - timedelta(days=2),  # Good Friday (Orthodox)
                easter,                       # Easter Sunday
                easter + timedelta(days=1),   # Easter Monday
                easter + timedelta(days=49),  # Whit Sunday (Rusalii)
                easter + timedelta(days=50),  # Whit Monday
            ]
        )
    return holidays


def _make_date_row(d: date, holidays: set[date]) -> DateRow:
    iso = d.isocalendar()
    dow = d.weekday() + 1  # 1=Mon..7=Sun
    return DateRow(
        date_id=_date_to_id(d),
        full_date=d,
        year=d.year,
        month=d.month,
        day_of_month=d.day,
        day_of_week=dow,
        week_of_year=iso.week,
        quarter=(d.month - 1) // 3 + 1,
        is_weekday=dow <= 5,
        is_ro_holiday=(d in holidays),
        season=_season(d.month),
    )
