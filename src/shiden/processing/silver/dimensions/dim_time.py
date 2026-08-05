"""Silver dimension: dim_time.

96 static rows — one per 15-minute interval within a day.
This table is created once and never updated; re-running is a no-op.

time_id represents LOCAL time (Europe/Bucharest) to match OPCOM's
interval numbering.  ENTSO-E and weather processors convert UTC → local
before computing time_id.

Peak definition: OPCOM official peak hours 08:00–20:00 local
(time_id 32–79 inclusive, i.e. intervals starting at 08:00 through 19:45).
"""

from __future__ import annotations

import logging
from typing import NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from shiden.config.settings import settings

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/silver/dim_time"

_SCHEMA = StructType(
    [
        StructField("time_id", IntegerType(), nullable=False),
        StructField("hour", IntegerType(), nullable=False),
        StructField("quarter_of_hour", IntegerType(), nullable=False),
        StructField("minute_of_day", IntegerType(), nullable=False),
        StructField("time_label", StringType(), nullable=False),
        StructField("is_hour_start", BooleanType(), nullable=False),
        StructField("is_peak", BooleanType(), nullable=False),
    ]
)

# OPCOM official peak: 08:00–20:00 local (intervals starting at 08:00 through 19:45)
_PEAK_START = 32   # time_id for 08:00 (32 * 15 min = 480 min = 08:00)
_PEAK_END = 79     # time_id for 19:45 (79 * 15 min = 1185 min = 19:45)


class TimeRow(NamedTuple):
    time_id: int
    hour: int
    quarter_of_hour: int
    minute_of_day: int
    time_label: str
    is_hour_start: bool
    is_peak: bool


class DimTimeProcessor:
    """Writes the 96-row dim_time table.  Safe to call multiple times."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)

    def process(self, spark: SparkSession) -> None:
        """Write dim_time if it does not already exist."""
        from delta.tables import DeltaTable

        if DeltaTable.isDeltaTable(spark, self._table_path):
            logger.info("DimTimeProcessor: dim_time already exists, skipping")
            return

        rows = _make_time_rows()
        df = spark.createDataFrame(rows, schema=_SCHEMA)
        df.write.format("delta").mode("overwrite").save(self._table_path)
        logger.info(
            "DimTimeProcessor: wrote %d rows to %s", len(rows), self._table_path
        )


# ---------------------------------------------------------------------------
# Row factory (module-level, unit-testable)
# ---------------------------------------------------------------------------


def _make_time_rows() -> list[TimeRow]:
    """Generate all 96 TimeRow values."""
    rows = []
    for tid in range(96):
        hour = tid // 4
        qoh = tid % 4
        mod = tid * 15
        label = f"{hour:02d}:{qoh * 15:02d}"
        rows.append(
            TimeRow(
                time_id=tid,
                hour=hour,
                quarter_of_hour=qoh,
                minute_of_day=mod,
                time_label=label,
                is_hour_start=(qoh == 0),
                is_peak=(_PEAK_START <= tid <= _PEAK_END),
            )
        )
    return rows
