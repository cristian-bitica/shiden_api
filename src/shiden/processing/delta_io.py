"""Shared Delta Lake I/O helpers.

Centralizes the write/read patterns that were previously copy-pasted across
every Bronze/Silver/Gold module (12 near-identical ``_merge`` helpers, three
``_deduplicate``, two ``_add_local_keys`` and three Gold ``_write`` copies):

    append_new_observations  Bronze: append-only with revision capture
    merge_upsert             Silver: MERGE (update-or-insert) on natural keys
    replace_date_range       Gold:   idempotent replaceWhere for a market/range
    deduplicate_latest       keep the newest row per natural key
    with_local_keys          UTC timestamp → local date_id / time_id columns

Layer policies encoded here:

- Bronze is append-only. ``append_new_observations`` never updates a row;
  a re-ingested row with unchanged values is skipped (growth guard for the
  rolling ingest windows), while a revised value appends a NEW row.  Readers
  recover the current state with ``deduplicate_latest`` on ``ingested_at``.
- Silver tables are keyed — MERGE keeps exactly one row per natural key.
- Gold tables are rebuilt per (market_id, date range) via ``replaceWhere``.
"""

from __future__ import annotations

from collections.abc import Sequence
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def append_new_observations(
    spark: SparkSession,
    df: DataFrame,
    table_path: str,
    key_cols: Sequence[str],
    value_cols: Sequence[str],
    partition_cols: Sequence[str] = (),
) -> None:
    """Append rows whose (key, value) combination is not already present.

    Pure append-only Bronze with revision capture: if an upstream source
    republishes a corrected value for an existing key, the corrected row is
    appended alongside the original (full observation history is preserved);
    if the value is unchanged, nothing is written.  ``ingested_at`` and other
    lineage columns are deliberately excluded from the comparison so that
    re-fetching identical data never grows the table.

    Comparison is null-safe (NULL == NULL matches), so nullable value columns
    do not cause duplicate appends.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, table_path):
        _initial_write(df, table_path, partition_cols)
        return

    compare_cols = list(key_cols) + list(value_cols)
    existing = (
        spark.read.format("delta").load(table_path).select(*compare_cols).distinct()
    )
    match_condition = reduce(
        lambda a, b: a & b,
        [df[c].eqNullSafe(existing[c]) for c in compare_cols],
    )
    new_rows = df.join(existing, match_condition, "left_anti")

    writer = new_rows.write.format("delta").mode("append")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(table_path)


def merge_upsert(
    spark: SparkSession,
    df: DataFrame,
    table_path: str,
    key_cols: Sequence[str],
    partition_cols: Sequence[str] = (),
) -> None:
    """MERGE ``df`` into the Delta table on ``key_cols`` (update-or-insert).

    Creates the table on first write.  Used by Silver processors where each
    natural key must resolve to exactly one current row.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, table_path):
        _initial_write(df, table_path, partition_cols)
        return

    condition = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
    (
        DeltaTable.forPath(spark, table_path)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def replace_date_range(
    spark: SparkSession,
    df: DataFrame,
    table_path: str,
    market_id: str,
    start_id: int,
    end_id: int,
    partition_cols: Sequence[str] = ("market_id", "date_id"),
) -> None:
    """Overwrite one market's ``[start_id, end_id]`` date range (Gold writes).

    ``replaceWhere`` makes re-runs idempotent for a rolling window without
    touching other markets or dates.  Creates the table on first write.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, table_path):
        _initial_write(df, table_path, partition_cols)
        return

    (
        df.write.format("delta")
        .mode("overwrite")
        .option(
            "replaceWhere",
            f"market_id = '{market_id}'"
            f" AND date_id >= {start_id}"
            f" AND date_id <= {end_id}",
        )
        .save(table_path)
    )


def deduplicate_latest(
    df: DataFrame,
    key_cols: Sequence[str],
    order_col: str = "ingested_at",
) -> DataFrame:
    """Keep the row with the greatest ``order_col`` per natural key.

    Standard read pattern over append-only Bronze: the newest ``ingested_at``
    per key is the current observation; older rows are superseded revisions.
    """
    window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def with_local_keys(df: DataFrame, timezone: str) -> DataFrame:
    """Add ``local_ts``, ``date_id`` (YYYYMMDD int) and hour-start ``time_id``
    columns derived from a UTC ``timestamp_utc`` column."""
    return (
        df.withColumn("local_ts", F.from_utc_timestamp("timestamp_utc", timezone))
        .withColumn("date_id", F.date_format("local_ts", "yyyyMMdd").cast("int"))
        .withColumn("time_id", F.hour("local_ts") * 4)
    )


def _initial_write(
    df: DataFrame, table_path: str, partition_cols: Sequence[str]
) -> None:
    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(table_path)


__all__ = [
    "append_new_observations",
    "merge_upsert",
    "replace_date_range",
    "deduplicate_latest",
    "with_local_keys",
]
