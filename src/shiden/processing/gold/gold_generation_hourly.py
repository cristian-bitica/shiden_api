"""Gold table: gold_generation_hourly.

Grain: one row per 1-hour interval × market.

Pivots silver/fact_generation (long format, one row per type) into a wide
table with one column per ENTSO-E production type, plus derived aggregate
columns used in the API and future ML feature store.

Column naming: canonical production type names are slugified to snake_case
with a ``_mw`` suffix (e.g. ``"Wind Onshore"`` → ``wind_onshore_mw``).
Unknown types discovered at runtime (energy_category='Other') are rolled up
into ``other_discovered_mw`` rather than individual sparse columns, keeping
the schema stable across markets.

Derived columns — computed from dim_production_type flags
(is_renewable, energy_category), the single source of truth for
classification; there is deliberately no local renewable/thermal list here:

    total_mw            : sum of all generation (including discovered types)
    renewable_mw        : sum where is_renewable
    thermal_mw          : sum where energy_category='Thermal'
    nuclear_mw_total    : sum where energy_category='Nuclear'
    renewable_share_pct : renewable_mw / total_mw × 100, NULL when total_mw=0

Write strategy: replaceWhere on (market_id, date_id) — idempotent re-run.
Partition: (market_id, date_id).
"""

from __future__ import annotations

import logging
import re
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shiden.config.settings import settings
from shiden.dates import date_to_id
from shiden.processing.delta_io import replace_date_range

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/gold/generation_hourly"

_KEYS = ["date_id", "time_id", "market_id"]


def _col_name(production_type_name: str) -> str:
    """Slugify a production type name to a safe column identifier + _mw suffix."""
    slug = re.sub(r"[^a-z0-9]+", "_", production_type_name.lower()).strip("_")
    return f"{slug}_mw"


# Pre-defined canonical column names (stable schema).
# Keyed by production_type_name → column_name; consistency with _col_name is
# enforced by unit tests.
_CANONICAL_COLS: dict[str, str] = {
    "Biomass":                                   "biomass_mw",
    "Fossil Brown coal/Lignite":                 "fossil_brown_coal_lignite_mw",
    "Fossil Coal-derived gas":                   "fossil_coal_derived_gas_mw",
    "Fossil Gas":                                "fossil_gas_mw",
    "Fossil Hard coal":                          "fossil_hard_coal_mw",
    "Fossil Oil":                                "fossil_oil_mw",
    "Fossil Oil shale":                          "fossil_oil_shale_mw",
    "Fossil Peat":                               "fossil_peat_mw",
    "Geothermal":                                "geothermal_mw",
    "Hydro Pumped Storage|Actual Aggregated":    "hydro_pumped_storage_actual_aggregated_mw",
    "Hydro Pumped Storage|Actual Consumption":   "hydro_pumped_storage_actual_consumption_mw",
    "Hydro Run-of-river and poundage":           "hydro_run_of_river_and_poundage_mw",
    "Hydro Water Reservoir":                     "hydro_water_reservoir_mw",
    "Marine":                                    "marine_mw",
    "Nuclear":                                   "nuclear_mw",
    "Other":                                     "other_mw",
    "Other renewable":                           "other_renewable_mw",
    "Solar":                                     "solar_mw",
    "Waste":                                     "waste_mw",
    "Wind Offshore":                             "wind_offshore_mw",
    "Wind Onshore":                              "wind_onshore_mw",
}


class GoldGenerationHourlyProcessor:
    """Build gold/generation_hourly from silver fact_generation +
    dim_production_type."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)
        self._gen_path = f"{settings.delta_base_path}/silver/fact_generation"
        self._pt_path = f"{settings.delta_base_path}/silver/dim_production_type"

    def process(
        self, market_id: str, start: date, end: date, spark: SparkSession
    ) -> None:
        """
        Compute gold/generation_hourly for market_id over [start, end] and write.

        Requires silver/fact_generation and silver/dim_production_type.
        """
        start_id = date_to_id(start)
        end_id = date_to_id(end)

        # ── 1. Read Silver fact_generation + resolve type names/flags ─────
        gen = (
            spark.read.format("delta").load(self._gen_path)
            .filter(
                (F.col("market_id") == market_id)
                & (F.col("date_id") >= start_id)
                & (F.col("date_id") <= end_id)
            )
        )

        dim_pt = (
            spark.read.format("delta").load(self._pt_path)
            .select("production_type_id", "production_type_name",
                    "is_renewable", "energy_category")
        )

        joined = gen.join(dim_pt, on="production_type_id", how="left")

        # ── 2. Derived aggregates from dim flags (long format) ────────────
        aggregates = joined.groupBy(*_KEYS).agg(
            F.sum("actual_mw").alias("total_mw"),
            F.sum(
                F.when(F.col("is_renewable"), F.col("actual_mw")).otherwise(0.0)
            ).alias("renewable_mw"),
            F.sum(
                F.when(
                    F.col("energy_category") == "Thermal", F.col("actual_mw")
                ).otherwise(0.0)
            ).alias("thermal_mw"),
            F.sum(
                F.when(
                    F.col("energy_category") == "Nuclear", F.col("actual_mw")
                ).otherwise(0.0)
            ).alias("nuclear_mw_total"),
        )

        # ── 3. Pivot canonical types → wide columns ───────────────────────
        # (wide element type matches pyspark's pivot values signature)
        canonical_names: list[bool | float | int | str] = list(_CANONICAL_COLS)
        joined = joined.withColumn(
            "is_canonical", F.col("production_type_name").isin(canonical_names)
        )

        canonical_df = (
            joined.filter(F.col("is_canonical"))
            .groupBy(*_KEYS)
            .pivot("production_type_name", canonical_names)
            .agg(F.first("actual_mw"))
        )
        for type_name, col_name in _CANONICAL_COLS.items():
            if type_name in canonical_df.columns:
                canonical_df = canonical_df.withColumnRenamed(type_name, col_name)

        # ── 4. Roll up discovered (non-canonical) types ───────────────────
        unknown_df = (
            joined.filter(~F.col("is_canonical"))
            .groupBy(*_KEYS)
            .agg(F.sum("actual_mw").alias("other_discovered_mw"))
        )

        wide = (
            canonical_df
            .join(unknown_df, on=_KEYS, how="left")
            .join(aggregates, on=_KEYS, how="left")
        )

        # ── 5. Fill NULL pivot cells with 0 ───────────────────────────────
        # Types not present for this market/date return NULL from pivot;
        # treat as 0 so per-source columns are always summable.
        fill_cols = list(_CANONICAL_COLS.values()) + ["other_discovered_mw"]
        fill_map: dict[str, bool | float | int | str] = {
            c: 0.0 for c in fill_cols if c in wide.columns
        }
        wide = wide.fillna(fill_map)

        # ── 6. Renewable share ────────────────────────────────────────────
        wide = wide.withColumn(
            "renewable_share_pct",
            F.when(
                F.col("total_mw") > 0,
                F.col("renewable_mw") / F.col("total_mw") * 100.0,
            ).otherwise(F.lit(None).cast("double")),
        )

        count = wide.count()
        replace_date_range(
            spark, wide, self._table_path, market_id, start_id, end_id
        )
        logger.info(
            "GoldGenerationHourlyProcessor: wrote %d rows for %s %s–%s",
            count, market_id, start, end,
        )
