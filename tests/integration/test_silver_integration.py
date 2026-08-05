"""Integration tests for the Silver layer against real Bronze Delta tables.

These tests run the actual Silver processors against the Bronze data in
data/delta/bronze/.  They require a working Spark + Delta runtime and are
therefore excluded from the fast unit-test run.

To run locally:
    pytest tests/integration/test_silver_integration.py -v

Available Bronze data (snapshot):
    bnr_fx_rates      : 2026-06-02 → 2026-06-05 (EUR + others, partitioned by foreign_currency)
    ecb_fx_rates      : 2026-06-01 → 2026-06-05 (RON + others, partitioned by quote_currency)
    entsoe_generation : 2026-06-03 00:00 UTC → 2026-06-09 23:45 UTC (15-min, 10 types)
    entsoe_load       : 2026-06-03 00:00 UTC → 2026-06-09 23:45 UTC (15-min)
    opcom_pzu_prices  : missing → fact_price cannot be integration-tested
    weather           : missing → fact_weather cannot be integration-tested

UTC→local conversion (Romania, EEST = UTC+3 in June):
    Bronze UTC range June 3 00:00 → June 9 23:45 maps to local dates
    June 3 03:00 → June 10 02:45.  Date filter trims June 10 rows, so:
      June 3 local: hours 3-23 only = 21 hours  (00:00-02:00 EEST have no UTC source)
      June 4-9 local: all 24 hours each

Expected Silver rows for the June 3-9 test window:
    dim_date            :   7 rows
    dim_production_type :  22 rows (21 canonical + "Energy storage" from Bronze)
    fact_generation     : 1650 rows (165 hours × 10 Bronze types)
    fact_load           :  165 rows (165 hours × 1 market)

Fixture pattern:
    Each test class creates an isolated temp Delta root.  The real Bronze data
    is accessible via an ``os.symlink`` from ``<tmp>/bronze`` to the project's
    ``data/delta/bronze`` directory.  Silver writes land in ``<tmp>/silver/``.
    ``settings.delta_base_path`` is temporarily overridden to ``<tmp>`` inside
    each class fixture and restored in the yield teardown.
"""

from __future__ import annotations

import os
from datetime import date

import pytest
from pyspark.sql import SparkSession

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Test window constants — calibrated to the bronze snapshot
# ---------------------------------------------------------------------------

_START = date(2026, 6, 3)
_END   = date(2026, 6, 9)

_JUNE3_LOCAL_HOURS = 21   # UTC starts June 3 00:00; EEST June 3 00:00-02:00 have no source
_FULL_DAY_HOURS    = 24
_DAYS_IN_WINDOW    = 7
_TOTAL_HOURS       = _JUNE3_LOCAL_HOURS + (_DAYS_IN_WINDOW - 1) * _FULL_DAY_HOURS  # 165
_GEN_TYPES         = 10   # production types in the Bronze snapshot
_CANONICAL_TYPES   = 21   # hardcoded in dim_production_type._CANONICAL
_UNKNOWN_TYPES     = 1    # "Energy storage" → not in canonical, discovered from Bronze


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _make_delta_root(tmp_path: os.PathLike) -> str:
    """Return a temp delta root with ``bronze`` symlinked to the real data dir."""
    root = os.path.join(str(tmp_path), "delta")
    os.makedirs(root, exist_ok=True)
    real_bronze = os.path.join(_project_root(), "data", "delta", "bronze")
    bronze_link = os.path.join(root, "bronze")
    if not os.path.exists(bronze_link):
        os.symlink(real_bronze, bronze_link)
    return root


# ---------------------------------------------------------------------------
# DimDate
# ---------------------------------------------------------------------------


class TestDimDate:
    """DimDateProcessor writes 7 calendar rows for Jun 3-9 (FX lives in
    silver/exchange_rates, not here)."""

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, spark: SparkSession, tmp_path_factory):
        from shiden.config.settings import settings

        delta_root = _make_delta_root(tmp_path_factory.mktemp("dim_date"))
        orig = settings.delta_base_path
        settings.delta_base_path = delta_root

        try:
            from shiden.processing.silver.dimensions.dim_date import DimDateProcessor
            DimDateProcessor().process(_START, _END, spark)
            self.df = spark.read.format("delta").load(f"{delta_root}/silver/dim_date")
            self.rows = {r["date_id"]: r for r in self.df.collect()}
        finally:
            settings.delta_base_path = orig

    # --- basic shape ---

    def test_row_count(self):
        assert self.df.count() == _DAYS_IN_WINDOW

    def test_date_ids_match_window(self):
        expected = [20260603, 20260604, 20260605, 20260606,
                    20260607, 20260608, 20260609]
        assert sorted(self.rows.keys()) == expected

    def test_no_null_date_ids(self):
        from pyspark.sql import functions as F
        assert self.df.filter(F.col("date_id").isNull()).count() == 0

    # --- calendar flags ---

    def test_weekday_flags(self):
        # Jun 2026: 3=Wed, 4=Thu, 5=Fri, 6=Sat, 7=Sun, 8=Mon, 9=Tue
        for d in [20260603, 20260604, 20260605, 20260608, 20260609]:
            assert self.rows[d]["is_weekday"] is True
        assert self.rows[20260606]["is_weekday"] is False  # Saturday
        assert self.rows[20260607]["is_weekday"] is False  # Sunday

    def test_season_is_summer(self):
        for row in self.rows.values():
            assert row["season"] == "Summer"

    def test_no_fx_columns(self):
        # FX moved to silver/exchange_rates — dim_date is calendar-only.
        assert "eur_ron_rate" not in self.df.columns
        assert "fx_source" not in self.df.columns


# ---------------------------------------------------------------------------
# ExchangeRates
# ---------------------------------------------------------------------------


class TestExchangeRates:
    """silver/exchange_rates: BNR primary, forward-filled across gaps.

    Bronze snapshot: BNR EUR row Jun 2-5, ECB RON Jun 1-5.  For the Jun 3-9
    window: Jun 3-5 come from BNR; Jun 6-9 (weekend + beyond snapshot) are
    FORWARD_FILLED from Jun 5's rate.
    """

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, spark: SparkSession, tmp_path_factory):
        from shiden.config.settings import settings

        delta_root = _make_delta_root(tmp_path_factory.mktemp("fx"))
        orig = settings.delta_base_path
        settings.delta_base_path = delta_root

        try:
            from shiden.processing.silver.exchange_rates import (
                ExchangeRatesProcessor,
            )
            ExchangeRatesProcessor().process(_START, _END, spark)
            self.df = spark.read.format("delta").load(
                f"{delta_root}/silver/exchange_rates"
            )
            self.rows = {r["date"].isoformat(): r for r in self.df.collect()}
        finally:
            settings.delta_base_path = orig

    def test_one_row_per_day_in_window(self):
        assert self.df.count() == _DAYS_IN_WINDOW

    def test_base_and_quote_currency(self):
        for r in self.rows.values():
            assert r["base_currency"] == "EUR"
            assert r["quote_currency"] == "RON"

    def test_bnr_source_on_business_days(self):
        for day in ["2026-06-03", "2026-06-04", "2026-06-05"]:
            assert self.rows[day]["source"] == "BNR"
            assert 4.5 < self.rows[day]["rate"] < 6.0

    def test_weekend_and_gap_days_forward_filled(self):
        # Jun 6-7 weekend; Jun 8-9 beyond the Bronze snapshot — all carry
        # Jun 5's rate with source=FORWARD_FILLED (regression: these used to
        # end up NULL and permanently null weekend EUR prices in Gold).
        jun5_rate = self.rows["2026-06-05"]["rate"]
        for day in ["2026-06-06", "2026-06-07", "2026-06-08", "2026-06-09"]:
            assert self.rows[day]["source"] == "FORWARD_FILLED"
            assert self.rows[day]["rate"] == pytest.approx(jun5_rate)

    def test_no_null_rates(self):
        from pyspark.sql import functions as F
        assert self.df.filter(F.col("rate").isNull()).count() == 0


# ---------------------------------------------------------------------------
# DimProductionType
# ---------------------------------------------------------------------------


class TestDimProductionType:
    """21 canonical types + 1 discovered from Bronze (Energy storage)."""

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, spark: SparkSession, tmp_path_factory):
        from shiden.config.settings import settings

        delta_root = _make_delta_root(tmp_path_factory.mktemp("dim_pt"))
        orig = settings.delta_base_path
        settings.delta_base_path = delta_root

        try:
            from shiden.processing.silver.dimensions.dim_production_type import (
                DimProductionTypeProcessor,
            )
            DimProductionTypeProcessor().process(spark)
            self.df = spark.read.format("delta").load(
                f"{delta_root}/silver/dim_production_type"
            )
            self.rows = {r["production_type_name"]: r for r in self.df.collect()}
        finally:
            settings.delta_base_path = orig

    def test_total_rows(self):
        assert self.df.count() == _CANONICAL_TYPES + _UNKNOWN_TYPES

    def test_unique_ids(self):
        ids = [r["production_type_id"] for r in self.rows.values()]
        assert len(ids) == len(set(ids)), "Duplicate production_type_id values"

    def test_canonical_biomass(self):
        assert "Biomass" in self.rows
        assert self.rows["Biomass"]["energy_category"] == "Renewable"
        assert self.rows["Biomass"]["is_renewable"] is True

    def test_canonical_nuclear(self):
        assert "Nuclear" in self.rows
        r = self.rows["Nuclear"]
        assert r["is_renewable"] is False
        assert r["is_dispatchable"] is True

    def test_canonical_wind_onshore(self):
        assert "Wind Onshore" in self.rows
        r = self.rows["Wind Onshore"]
        assert r["is_variable"] is True

    def test_energy_storage_discovered_from_bronze(self):
        # "Energy storage" appears in Bronze but is not in _CANONICAL
        assert "Energy storage" in self.rows, (
            "'Energy storage' should be auto-discovered from Bronze generation data"
        )
        r = self.rows["Energy storage"]
        assert r["energy_category"] == "Other"
        assert r["is_renewable"] is False

    def test_all_bronze_types_present_in_dim(self):
        bronze_types = {
            "Biomass",
            "Energy storage",
            "Fossil Brown coal/Lignite",
            "Fossil Gas",
            "Fossil Hard coal",
            "Hydro Run-of-river and poundage",
            "Hydro Water Reservoir",
            "Nuclear",
            "Solar",
            "Wind Onshore",
        }
        missing = bronze_types - set(self.rows)
        assert not missing, f"Types in Bronze but missing from dim: {missing}"


# ---------------------------------------------------------------------------
# FactGeneration
# ---------------------------------------------------------------------------


class TestFactGeneration:
    """1 hourly-averaged MW row per hour × production type.  1650 rows total."""

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, spark: SparkSession, tmp_path_factory):
        from shiden.config.settings import settings

        delta_root = _make_delta_root(tmp_path_factory.mktemp("fact_gen"))
        orig = settings.delta_base_path
        settings.delta_base_path = delta_root

        try:
            # dim_production_type must exist before FactGenerationProcessor reads it
            from shiden.processing.silver.dimensions.dim_production_type import (
                DimProductionTypeProcessor,
            )
            DimProductionTypeProcessor().process(spark)

            from shiden.processing.silver.fact_generation import FactGenerationProcessor
            FactGenerationProcessor().process("RO", _START, _END, spark)

            self.delta_root = delta_root
            self.df = spark.read.format("delta").load(
                f"{delta_root}/silver/fact_generation"
            )
        finally:
            settings.delta_base_path = orig

    # --- row count ---

    def test_total_row_count(self):
        assert self.df.count() == _TOTAL_HOURS * _GEN_TYPES

    # --- time_id correctness ---

    def test_only_hour_start_time_ids(self):
        from pyspark.sql import functions as F
        bad = self.df.filter(F.col("time_id") % 4 != 0)
        assert bad.count() == 0, "Non-hour-start time_ids found"

    def test_time_ids_in_valid_range(self):
        from pyspark.sql import functions as F
        bad = self.df.filter((F.col("time_id") < 0) | (F.col("time_id") > 92))
        assert bad.count() == 0

    # --- date_id correctness ---

    def test_date_ids_within_window(self):
        from pyspark.sql import functions as F
        bad = self.df.filter(
            (F.col("date_id") < 20260603) | (F.col("date_id") > 20260609)
        )
        assert bad.count() == 0

    def test_june3_has_21_hours_only(self):
        from pyspark.sql import functions as F
        june3 = self.df.filter(F.col("date_id") == 20260603)
        assert june3.count() == _JUNE3_LOCAL_HOURS * _GEN_TYPES

    def test_june3_starts_at_time_id_12(self):
        # First Bronze UTC row (Jun 3 00:00 UTC) → EEST 03:00 → time_id = 3*4 = 12
        from pyspark.sql import functions as F
        min_tid = (
            self.df.filter(F.col("date_id") == 20260603)
            .agg(F.min("time_id"))
            .collect()[0][0]
        )
        assert min_tid == 12

    def test_full_days_have_24_hours(self):
        from pyspark.sql import functions as F
        for date_id in [20260604, 20260605, 20260606,
                        20260607, 20260608, 20260609]:
            n = self.df.filter(F.col("date_id") == date_id).count()
            assert n == _FULL_DAY_HOURS * _GEN_TYPES, (
                f"date_id={date_id}: expected {_FULL_DAY_HOURS * _GEN_TYPES}, got {n}"
            )

    # --- data quality ---

    def test_market_id_is_ro_only(self):
        from pyspark.sql import functions as F
        assert self.df.filter(F.col("market_id") != "RO").count() == 0

    def test_no_null_production_type_id(self):
        from pyspark.sql import functions as F
        assert self.df.filter(F.col("production_type_id").isNull()).count() == 0

    def test_no_null_actual_mw(self):
        from pyspark.sql import functions as F
        # All rows in this Bronze snapshot carry non-null actual_mw
        assert self.df.filter(F.col("actual_mw").isNull()).count() == 0

    # --- merge key uniqueness ---

    def test_no_duplicate_merge_keys(self):
        from pyspark.sql import functions as F
        dupes = (
            self.df
            .groupBy("date_id", "time_id", "market_id", "production_type_id")
            .count()
            .filter(F.col("count") > 1)
        )
        assert dupes.count() == 0, "Duplicate MERGE keys in fact_generation"

    # --- idempotency ---

    def test_idempotent_second_run(self, spark: SparkSession):
        """Running the processor twice must not change row count."""
        from shiden.config.settings import settings

        count_before = self.df.count()
        orig = settings.delta_base_path
        settings.delta_base_path = self.delta_root
        try:
            from shiden.processing.silver.fact_generation import FactGenerationProcessor
            FactGenerationProcessor().process("RO", _START, _END, spark)
        finally:
            settings.delta_base_path = orig

        count_after = spark.read.format("delta").load(
            f"{self.delta_root}/silver/fact_generation"
        ).count()
        assert count_after == count_before


# ---------------------------------------------------------------------------
# FactLoad
# ---------------------------------------------------------------------------


class TestFactLoad:
    """1 hourly-averaged load row per hour × market.  165 rows total."""

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, spark: SparkSession, tmp_path_factory):
        from shiden.config.settings import settings

        delta_root = _make_delta_root(tmp_path_factory.mktemp("fact_load"))
        orig = settings.delta_base_path
        settings.delta_base_path = delta_root

        try:
            from shiden.processing.silver.fact_load import FactLoadProcessor
            FactLoadProcessor().process("RO", _START, _END, spark)

            self.delta_root = delta_root
            self.df = spark.read.format("delta").load(
                f"{delta_root}/silver/fact_load"
            )
        finally:
            settings.delta_base_path = orig

    # --- row count ---

    def test_total_row_count(self):
        assert self.df.count() == _TOTAL_HOURS

    # --- time_id correctness ---

    def test_only_hour_start_time_ids(self):
        from pyspark.sql import functions as F
        bad = self.df.filter(F.col("time_id") % 4 != 0)
        assert bad.count() == 0, "Non-hour-start time_ids found"

    def test_time_ids_in_valid_range(self):
        from pyspark.sql import functions as F
        bad = self.df.filter((F.col("time_id") < 0) | (F.col("time_id") > 92))
        assert bad.count() == 0

    # --- date_id correctness ---

    def test_date_ids_within_window(self):
        from pyspark.sql import functions as F
        bad = self.df.filter(
            (F.col("date_id") < 20260603) | (F.col("date_id") > 20260609)
        )
        assert bad.count() == 0

    def test_june3_has_21_hours_only(self):
        from pyspark.sql import functions as F
        assert self.df.filter(F.col("date_id") == 20260603).count() == _JUNE3_LOCAL_HOURS

    def test_june3_starts_at_time_id_12(self):
        from pyspark.sql import functions as F
        min_tid = (
            self.df.filter(F.col("date_id") == 20260603)
            .agg(F.min("time_id"))
            .collect()[0][0]
        )
        assert min_tid == 12

    def test_full_days_have_24_hours(self):
        from pyspark.sql import functions as F
        for date_id in [20260604, 20260605, 20260606,
                        20260607, 20260608, 20260609]:
            n = self.df.filter(F.col("date_id") == date_id).count()
            assert n == _FULL_DAY_HOURS, f"date_id={date_id}: expected 24 rows, got {n}"

    # --- data quality ---

    def test_market_id_is_ro_only(self):
        from pyspark.sql import functions as F
        assert self.df.filter(F.col("market_id") != "RO").count() == 0

    def test_actual_load_not_null(self):
        from pyspark.sql import functions as F
        assert self.df.filter(F.col("actual_load_mw").isNull()).count() == 0

    def test_load_values_within_romania_range(self):
        from pyspark.sql import functions as F
        # Romania demand is typically 3,000-12,000 MW
        bad = self.df.filter(
            (F.col("actual_load_mw") < 1_000) | (F.col("actual_load_mw") > 20_000)
        )
        assert bad.count() == 0, "Found unreasonable load_mw values outside 1,000-20,000"

    # --- merge key uniqueness ---

    def test_no_duplicate_merge_keys(self):
        from pyspark.sql import functions as F
        dupes = (
            self.df
            .groupBy("date_id", "time_id", "market_id")
            .count()
            .filter(F.col("count") > 1)
        )
        assert dupes.count() == 0, "Duplicate MERGE keys in fact_load"

    # --- idempotency ---

    def test_idempotent_second_run(self, spark: SparkSession):
        """Running the processor twice must not change row count."""
        from shiden.config.settings import settings

        count_before = self.df.count()
        orig = settings.delta_base_path
        settings.delta_base_path = self.delta_root
        try:
            from shiden.processing.silver.fact_load import FactLoadProcessor
            FactLoadProcessor().process("RO", _START, _END, spark)
        finally:
            settings.delta_base_path = orig

        count_after = spark.read.format("delta").load(
            f"{self.delta_root}/silver/fact_load"
        ).count()
        assert count_after == count_before
