"""Silver dimension: dim_production_type.

Semi-static mapping from ENTSO-E production type strings to business
categories used in Silver and Gold aggregations.

The canonical set is hardcoded below (covers all standard ENTSO-E types).
On each run the processor also reads bronze/entsoe_generation to discover
any additional types not in the canonical list and adds them with
energy_category='Other' and the next available surrogate ID.

MERGE key: production_type_name (natural key).
Partition:  none (~20 rows at full scale).
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
from shiden.processing.delta_io import merge_upsert

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/silver/dim_production_type"

_SCHEMA = StructType(
    [
        StructField("production_type_id", IntegerType(), nullable=False),
        StructField("production_type_name", StringType(), nullable=False),
        StructField("energy_category", StringType(), nullable=False),
        StructField("is_renewable", BooleanType(), nullable=False),
        StructField("is_variable", BooleanType(), nullable=False),
        StructField("is_dispatchable", BooleanType(), nullable=False),
    ]
)

# (production_type_id, energy_category, is_renewable, is_variable, is_dispatchable)
_CANONICAL: dict[str, tuple[int, str, bool, bool, bool]] = {
    "Biomass":                                      (1,  "Renewable", True,  False, True),
    "Fossil Brown coal/Lignite":                    (2,  "Thermal",   False, False, True),
    "Fossil Coal-derived gas":                      (3,  "Thermal",   False, False, True),
    "Fossil Gas":                                   (4,  "Thermal",   False, False, True),
    "Fossil Hard coal":                             (5,  "Thermal",   False, False, True),
    "Fossil Oil":                                   (6,  "Thermal",   False, False, True),
    "Fossil Oil shale":                             (7,  "Thermal",   False, False, True),
    "Fossil Peat":                                  (8,  "Thermal",   False, False, True),
    "Geothermal":                                   (9,  "Renewable", True,  False, False),
    "Hydro Pumped Storage|Actual Aggregated":       (10, "Storage",   False, False, True),
    "Hydro Pumped Storage|Actual Consumption":      (11, "Storage",   False, False, True),
    "Hydro Run-of-river and poundage":              (12, "Renewable", True,  False, False),
    "Hydro Water Reservoir":                        (13, "Renewable", True,  False, True),
    "Marine":                                       (14, "Renewable", True,  True,  False),
    "Nuclear":                                      (15, "Nuclear",   False, False, True),
    "Other":                                        (16, "Other",     False, False, False),
    "Other renewable":                              (17, "Renewable", True,  False, False),
    "Solar":                                        (18, "Renewable", True,  True,  False),
    "Waste":                                        (19, "Thermal",   False, False, True),
    "Wind Offshore":                                (20, "Renewable", True,  True,  False),
    "Wind Onshore":                                 (21, "Renewable", True,  True,  False),
}


class ProductionTypeRow(NamedTuple):
    production_type_id: int
    production_type_name: str
    energy_category: str
    is_renewable: bool
    is_variable: bool
    is_dispatchable: bool


class DimProductionTypeProcessor:
    """Writes dim_production_type from canonical list + Bronze discovery."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)
        self._bronze_gen_path = f"{settings.delta_base_path}/bronze/entsoe_generation"

    def process(self, spark: SparkSession) -> None:
        bronze_types = _read_bronze_types(spark, self._bronze_gen_path)
        existing_ids = _read_existing_ids(spark, self._table_path)
        rows = _build_rows(bronze_types, existing_ids)
        incoming_df = spark.createDataFrame(rows, schema=_SCHEMA)
        merge_upsert(
            spark,
            incoming_df,
            self._table_path,
            key_cols=("production_type_name",),
        )
        logger.info(
            "DimProductionTypeProcessor: merged %d rows into %s",
            len(rows),
            self._table_path,
        )


# ---------------------------------------------------------------------------
# Table readers (Spark-side)
# ---------------------------------------------------------------------------


def _read_bronze_types(spark: SparkSession, bronze_gen_path: str) -> set[str]:
    """Distinct production types present in Bronze (empty set if absent)."""
    try:
        return {
            r["production_type"]
            for r in spark.read.format("delta")
            .load(bronze_gen_path)
            .select("production_type")
            .distinct()
            .collect()
        }
    except Exception as exc:
        logger.warning(
            "DimProductionTypeProcessor: cannot read Bronze generation: %s", exc
        )
        return set()


def _read_existing_ids(spark: SparkSession, table_path: str) -> dict[str, int]:
    """Existing name → id assignments from the dim table (empty if absent).

    Surrogate IDs must be STABLE: fact_generation stores production_type_id,
    so an ID that was ever assigned to a name must never be reassigned or
    renumbered. Previously, discovered types were renumbered from
    max(canonical)+1 in alphabetical order on every run — a newly discovered
    type sorting before an existing one silently shifted the existing type's
    ID and re-labelled historical fact rows.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, table_path):
        return {}
    return {
        r["production_type_name"]: r["production_type_id"]
        for r in spark.read.format("delta")
        .load(table_path)
        .select("production_type_name", "production_type_id")
        .collect()
    }


# ---------------------------------------------------------------------------
# Row builder (module-level, unit-testable)
# ---------------------------------------------------------------------------


def _build_rows(
    bronze_types: set[str] | None = None,
    existing_ids: dict[str, int] | None = None,
) -> list[ProductionTypeRow]:
    """Return canonical rows + discovered types with stable surrogate IDs.

    ID assignment rules:
      1. Canonical types keep their fixed IDs (1-21).
      2. A discovered type that already has an ID in ``existing_ids``
         keeps it — never renumbered.
      3. Genuinely new types get the next ID above everything ever assigned
         (canonical ∪ existing), in sorted order.
    """
    bronze_types = bronze_types or set()
    existing_ids = existing_ids or {}

    rows: list[ProductionTypeRow] = [
        ProductionTypeRow(
            production_type_id=tid,
            production_type_name=name,
            energy_category=cat,
            is_renewable=renew,
            is_variable=var,
            is_dispatchable=disp,
        )
        for name, (tid, cat, renew, var, disp) in _CANONICAL.items()
    ]

    discovered = sorted(bronze_types - set(_CANONICAL))
    if not discovered:
        return rows

    used_ids = {r.production_type_id for r in rows} | set(existing_ids.values())
    next_id = max(used_ids) + 1

    for name in discovered:
        if name in existing_ids:
            type_id = existing_ids[name]  # keep the previously assigned ID
        else:
            type_id = next_id
            next_id += 1
            logger.warning(
                "DimProductionTypeProcessor: unknown type %r — assigned id=%d",
                name,
                type_id,
            )
        rows.append(
            ProductionTypeRow(
                production_type_id=type_id,
                production_type_name=name,
                energy_category="Other",
                is_renewable=False,
                is_variable=False,
                is_dispatchable=False,
            )
        )

    return rows
