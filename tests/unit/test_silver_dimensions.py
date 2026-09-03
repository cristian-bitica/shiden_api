"""Unit tests for Silver dimension helpers.

No network access, no Spark.  Tests pure-Python functions from:
  - silver.dimensions.dim_date
  - silver.dimensions.dim_datetime (see tests/unit/test_timeaxis.py)
  - silver.dimensions.dim_production_type
  - silver.dimensions.dim_location
"""

from __future__ import annotations

from datetime import date

import pytest

from shiden.processing.silver.dimensions.dim_date import (
    _date_to_id,
    _make_date_row,
    _orthodox_easter,
    _ro_holidays_set,
    _season,
)
from shiden.processing.silver.dimensions.dim_location import (
    _build_rows as _build_location_rows,
)
from shiden.processing.silver.dimensions.dim_location import (
    _location_id,
)
from shiden.processing.silver.dimensions.dim_production_type import (
    _CANONICAL,
)
from shiden.processing.silver.dimensions.dim_production_type import (
    _build_rows as _build_pt_rows,
)

# ---------------------------------------------------------------------------
# dim_date helpers
# ---------------------------------------------------------------------------


class TestDateToId:
    def test_known_date(self):
        assert _date_to_id(date(2024, 1, 15)) == 20240115

    def test_single_digit_month_and_day(self):
        assert _date_to_id(date(2024, 3, 5)) == 20240305

    def test_year_boundary(self):
        assert _date_to_id(date(2024, 12, 31)) == 20241231
        assert _date_to_id(date(2025, 1, 1)) == 20250101


class TestSeason:
    def test_winter_months(self):
        for m in (12, 1, 2):
            assert _season(m) == "Winter"

    def test_spring_months(self):
        for m in (3, 4, 5):
            assert _season(m) == "Spring"

    def test_summer_months(self):
        for m in (6, 7, 8):
            assert _season(m) == "Summer"

    def test_autumn_months(self):
        for m in (9, 10, 11):
            assert _season(m) == "Autumn"


class TestOrthodoxEaster:
    @pytest.mark.parametrize(
        "year, expected",
        [
            (2024, date(2024, 5, 5)),
            (2023, date(2023, 4, 16)),
            (2022, date(2022, 4, 24)),
            (2025, date(2025, 4, 20)),
        ],
    )
    def test_known_years(self, year, expected):
        assert _orthodox_easter(year) == expected

    def test_boundary_years_valid(self):
        # 1900 and 2099 are inclusive bounds — must not raise
        _orthodox_easter(1900)
        _orthodox_easter(2099)

    @pytest.mark.parametrize("year", [1899, 2100, 0, -1, 9999])
    def test_out_of_range_raises(self, year):
        with pytest.raises(ValueError, match="1900"):
            _orthodox_easter(year)


class TestRoHolidaysSet:
    def test_new_year_present(self):
        h = _ro_holidays_set(2024, 2024)
        assert date(2024, 1, 1) in h
        assert date(2024, 1, 2) in h

    def test_national_day_present(self):
        h = _ro_holidays_set(2024, 2024)
        assert date(2024, 12, 1) in h

    def test_christmas_present(self):
        h = _ro_holidays_set(2024, 2024)
        assert date(2024, 12, 25) in h
        assert date(2024, 12, 26) in h

    def test_easter_monday_2024(self):
        # 2024 Orthodox Easter = May 5
        h = _ro_holidays_set(2024, 2024)
        assert date(2024, 5, 6) in h  # Easter Monday

    def test_good_friday_2024(self):
        h = _ro_holidays_set(2024, 2024)
        assert date(2024, 5, 3) in h  # Good Friday

    def test_multiple_years_included(self):
        h = _ro_holidays_set(2024, 2025)
        assert date(2024, 1, 1) in h
        assert date(2025, 1, 1) in h

    def test_epiphany_and_st_john_from_2024(self):
        # Law 52/2023 — Jan 6 and Jan 7 are legal holidays starting 2024
        h = _ro_holidays_set(2024, 2025)
        assert date(2024, 1, 6) in h
        assert date(2024, 1, 7) in h
        assert date(2025, 1, 6) in h
        assert date(2025, 1, 7) in h

    def test_epiphany_not_holiday_before_2024(self):
        h = _ro_holidays_set(2023, 2023)
        assert date(2023, 1, 6) not in h
        assert date(2023, 1, 7) not in h

    def test_holiday_count_from_2024(self):
        # 12 fixed (incl. Jan 6+7 since Law 52/2023) + 5 Easter-dependent;
        # no overlaps in 2024 → 17 distinct days.
        h = _ro_holidays_set(2024, 2024)
        assert sum(1 for d in h if d.year == 2024) == 17

    def test_regular_weekday_not_holiday(self):
        h = _ro_holidays_set(2024, 2024)
        assert date(2024, 3, 15) not in h  # random Monday

    def test_at_least_10_holidays_per_year(self):
        h = _ro_holidays_set(2024, 2024)
        count_2024 = sum(1 for d in h if d.year == 2024)
        assert count_2024 >= 10


class TestMakeDateRow:
    def setup_method(self):
        self.holidays = _ro_holidays_set(2024, 2024)

    def test_weekday_flag_monday(self):
        row = _make_date_row(date(2024, 1, 15), self.holidays)
        assert row.is_weekday is True  # Monday
        assert row.day_of_week == 1

    def test_weekday_flag_saturday(self):
        row = _make_date_row(date(2024, 1, 13), self.holidays)
        assert row.is_weekday is False  # Saturday
        assert row.day_of_week == 6

    def test_holiday_flag_new_year(self):
        row = _make_date_row(date(2024, 1, 1), self.holidays)
        assert row.is_ro_holiday is True

    def test_not_holiday_normal_day(self):
        row = _make_date_row(date(2024, 3, 15), self.holidays)
        assert row.is_ro_holiday is False

    def test_no_fx_fields_on_calendar_dimension(self):
        # FX now lives in silver/exchange_rates — dim_date is calendar-only.
        row = _make_date_row(date(2024, 1, 15), self.holidays)
        assert not hasattr(row, "eur_ron_rate")
        assert not hasattr(row, "fx_source")

    def test_date_id_correct(self):
        row = _make_date_row(date(2024, 6, 15), self.holidays)
        assert row.date_id == 20240615

    def test_season_winter(self):
        row = _make_date_row(date(2024, 1, 15), self.holidays)
        assert row.season == "Winter"

    def test_quarter(self):
        assert _make_date_row(date(2024, 1, 1), self.holidays).quarter == 1
        assert _make_date_row(date(2024, 4, 1), self.holidays).quarter == 2
        assert _make_date_row(date(2024, 7, 1), self.holidays).quarter == 3
        assert _make_date_row(date(2024, 10, 1), self.holidays).quarter == 4


# ---------------------------------------------------------------------------
# dim_production_type helpers
# ---------------------------------------------------------------------------


class TestBuildProductionTypeRows:
    def setup_method(self):
        # No bronze types, no existing IDs — returns canonical rows only
        self.rows = _build_pt_rows()

    def test_canonical_count(self):
        assert len(self.rows) == len(_CANONICAL)

    def test_all_canonical_names_present(self):
        names = {r.production_type_name for r in self.rows}
        assert names == set(_CANONICAL)

    def test_wind_onshore_is_renewable_and_variable(self):
        r = next(x for x in self.rows if x.production_type_name == "Wind Onshore")
        assert r.is_renewable is True
        assert r.is_variable is True
        assert r.is_dispatchable is False
        assert r.energy_category == "Renewable"

    def test_nuclear_is_dispatchable_not_renewable(self):
        r = next(x for x in self.rows if x.production_type_name == "Nuclear")
        assert r.is_renewable is False
        assert r.is_dispatchable is True
        assert r.energy_category == "Nuclear"

    def test_pumped_storage_is_storage(self):
        r = next(
            x for x in self.rows
            if x.production_type_name == "Hydro Pumped Storage|Actual Aggregated"
        )
        assert r.energy_category == "Storage"
        assert r.is_dispatchable is True

    def test_unique_ids(self):
        ids = [r.production_type_id for r in self.rows]
        assert len(ids) == len(set(ids))

    def test_no_zero_ids(self):
        assert all(r.production_type_id > 0 for r in self.rows)


class TestDiscoveredTypeIdStability:
    """Surrogate IDs for discovered types must never shift between runs."""

    def _ids(self, rows):
        return {r.production_type_name: r.production_type_id for r in rows}

    def test_new_type_gets_next_id(self):
        rows = _build_pt_rows(bronze_types={"Energy storage"})
        ids = self._ids(rows)
        assert ids["Energy storage"] == len(_CANONICAL) + 1

    def test_existing_id_is_kept(self):
        existing = {"Energy storage": 22}
        rows = _build_pt_rows(
            bronze_types={"Energy storage"}, existing_ids=existing
        )
        assert self._ids(rows)["Energy storage"] == 22

    def test_new_type_sorting_before_existing_does_not_shift_it(self):
        # Regression: "Aggregated storage" sorts before "Energy storage";
        # the old max+1-in-sorted-order logic would have stolen id 22.
        existing = {"Energy storage": 22}
        rows = _build_pt_rows(
            bronze_types={"Aggregated storage", "Energy storage"},
            existing_ids=existing,
        )
        ids = self._ids(rows)
        assert ids["Energy storage"] == 22
        assert ids["Aggregated storage"] == 23

    def test_ids_unique_with_discovered_types(self):
        rows = _build_pt_rows(
            bronze_types={"A new type", "Z new type", "Energy storage"},
            existing_ids={"Energy storage": 25},
        )
        ids = [r.production_type_id for r in rows]
        assert len(ids) == len(set(ids))

    def test_canonical_types_in_bronze_are_not_duplicated(self):
        rows = _build_pt_rows(bronze_types={"Wind Onshore", "Solar"})
        names = [r.production_type_name for r in rows]
        assert len(names) == len(set(names)) == len(_CANONICAL)


# ---------------------------------------------------------------------------
# dim_location helpers
# ---------------------------------------------------------------------------


class TestLocationId:
    def test_different_names_give_different_ids(self):
        id1 = _location_id("RO", "Bucharest")
        id2 = _location_id("RO", "Dobrogea")
        assert id1 != id2

    def test_same_inputs_give_same_id(self):
        assert _location_id("RO", "Dobrogea") == _location_id("RO", "Dobrogea")

    def test_id_is_in_valid_range(self):
        for name in ("Bucharest", "Dobrogea", "Oltenia", "Transylvania", "Moldova", "Banat"):
            lid = _location_id("RO", name)
            assert 100_000 <= lid <= 999_999


class TestBuildLocationRows:
    def setup_method(self):
        self.rows = _build_location_rows()

    def test_six_ro_locations(self):
        ro_rows = [r for r in self.rows if r.market_id == "RO"]
        assert len(ro_rows) == 6

    def test_location_names(self):
        names = {r.location_name for r in self.rows if r.market_id == "RO"}
        assert names == {"Bucharest", "Dobrogea", "Oltenia", "Transylvania", "Moldova", "Banat"}

    def test_dobrogea_high_wind_weight(self):
        dobrogea = next(r for r in self.rows if r.location_name == "Dobrogea")
        assert dobrogea.wind_weight == pytest.approx(0.70)

    def test_bucharest_high_demand_weight(self):
        buc = next(r for r in self.rows if r.location_name == "Bucharest")
        assert buc.demand_weight == pytest.approx(0.35)

    def test_wind_weights_sum_to_one(self):
        total = sum(r.wind_weight for r in self.rows if r.market_id == "RO")
        assert total == pytest.approx(1.0, abs=0.01)

    def test_solar_weights_sum_to_one(self):
        total = sum(r.solar_weight for r in self.rows if r.market_id == "RO")
        assert total == pytest.approx(1.0, abs=0.01)

    def test_demand_weights_sum_to_one(self):
        total = sum(r.demand_weight for r in self.rows if r.market_id == "RO")
        assert total == pytest.approx(1.0, abs=0.01)

    def test_unique_location_ids(self):
        ids = [r.location_id for r in self.rows]
        assert len(ids) == len(set(ids))

    def test_coordinates_in_romania(self):
        for r in self.rows:
            if r.market_id == "RO":
                assert 43.5 < r.latitude < 48.5
                assert 20.0 < r.longitude < 30.0
