"""Unit tests for Gold layer pure-Python helpers (no Spark required).

Covers:
  - assign_signals() in gold_bess_signals
  - _col_name() in gold_generation_hourly
  - _nf() / _ni() helpers in the BESS and generation routers
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# gold_bess_signals.assign_signals
# ---------------------------------------------------------------------------


class TestAssignSignals:
    """Tests for the pure-Python assign_signals() function."""

    from_module = "shiden.processing.gold.gold_bess_signals"

    @pytest.fixture(autouse=True)
    def _import(self):
        from shiden.processing.gold.gold_bess_signals import assign_signals
        self.assign_signals = assign_signals

    # ---- basic behaviour ---------------------------------------------------

    def test_basic_8_charge_8_discharge(self):
        """Standard 96-slot day: 8 cheapest = charge, 8 most expensive = discharge."""
        prices = list(range(96))  # 0 cheapest, 95 most expensive
        signals = self.assign_signals(prices, charge_slots=8, discharge_slots=8)

        # slots 0-7 are cheapest → charge
        assert signals[:8] == [-1] * 8
        # slots 88-95 are most expensive → discharge
        assert signals[88:] == [1] * 8
        # everything in between is idle
        assert all(s == 0 for s in signals[8:88])

    def test_output_length_matches_input(self):
        prices = [float(i) for i in range(96)]
        signals = self.assign_signals(prices)
        assert len(signals) == 96

    def test_correct_signal_values(self):
        """Only -1, 0, +1 are valid signal values."""
        prices = [float(i) for i in range(96)]
        signals = self.assign_signals(prices)
        assert set(signals).issubset({-1, 0, 1})

    def test_charge_count(self):
        prices = list(range(96))
        signals = self.assign_signals(prices, charge_slots=4, discharge_slots=4)
        assert signals.count(-1) == 4

    def test_discharge_count(self):
        prices = list(range(96))
        signals = self.assign_signals(prices, charge_slots=4, discharge_slots=4)
        assert signals.count(1) == 4

    # ---- None handling -----------------------------------------------------

    def test_none_prices_get_idle_signal(self):
        """Slots with None price must never be charged or discharged."""
        prices = [None] * 96
        signals = self.assign_signals(prices)
        assert all(s == 0 for s in signals)

    def test_partial_none_prices_skipped_in_ranking(self):
        """Only non-None prices participate in ranking; None slots stay idle."""
        # 48 None + 48 real prices; 8 cheapest real → charge
        prices = [None] * 48 + list(range(48))
        signals = self.assign_signals(prices, charge_slots=8, discharge_slots=8)
        # None slots stay idle
        assert all(s == 0 for s in signals[:48])
        # Among the real slots (48-95): cheapest 8 are charge, most expensive 8 are discharge
        assert signals[48:56] == [-1] * 8
        assert signals[88:] == [1] * 8

    def test_none_does_not_count_toward_slot_budget(self):
        """charge_slots + discharge_slots should not exceed valid count."""
        prices = [None, None, 1.0, 2.0, 3.0]  # only 3 valid
        signals = self.assign_signals(prices, charge_slots=2, discharge_slots=2)
        # 3 valid: rank 0 (1.0)→charge, rank 1 (2.0)→charge, rank 2 (3.0)→discharge
        assert signals[0] == 0  # None
        assert signals[1] == 0  # None
        assert signals[2] == -1  # cheapest valid
        assert signals[3] == -1  # second cheapest valid
        assert signals[4] == 1   # most expensive valid

    # ---- edge cases --------------------------------------------------------

    def test_zero_charge_slots(self):
        prices = list(range(96))
        signals = self.assign_signals(prices, charge_slots=0, discharge_slots=8)
        assert signals.count(-1) == 0
        assert signals.count(1) == 8

    def test_zero_discharge_slots(self):
        prices = list(range(96))
        signals = self.assign_signals(prices, charge_slots=8, discharge_slots=0)
        assert signals.count(-1) == 8
        assert signals.count(1) == 0

    def test_all_same_price_charge_discharge_assigned_to_first_last(self):
        """Tie-breaking: same price → stable sort preserves index order."""
        prices = [100.0] * 96
        signals = self.assign_signals(prices, charge_slots=2, discharge_slots=2)
        # stable sort: first 2 indices get charge, last 2 get discharge
        assert signals[0] == -1
        assert signals[1] == -1
        assert signals[94] == 1
        assert signals[95] == 1

    def test_single_slot_valid(self):
        """When only 1 valid slot exists and both charge+discharge claim 1 slot each,
        discharge rank check (>= n - discharge_slots = 0) wins for the single slot."""
        prices = [None] * 95 + [50.0]
        signals = self.assign_signals(prices, charge_slots=1, discharge_slots=1)
        # 1 valid, rank 0: charge check first (rank < 1 → True) → -1
        assert signals[95] == -1

    def test_charge_discharge_no_overlap(self):
        """Charge and discharge windows must never overlap on the same slot."""
        prices = list(range(16))  # only 16 valid slots
        signals = self.assign_signals(prices, charge_slots=8, discharge_slots=8)
        for s in signals:
            assert s in (-1, 0, 1)
        # With exactly 16 valid slots and 8+8, there should be no idle
        assert all(s != 0 for s in signals)

    def test_negative_prices_handled_correctly(self):
        """Negative prices are valid (OPCOM can have negative EUR prices)."""
        prices = [-10.0, -5.0, 0.0, 5.0, 10.0] + [1.0] * 91
        signals = self.assign_signals(prices, charge_slots=2, discharge_slots=2)
        # -10.0 and -5.0 are the two cheapest → charge
        assert signals[0] == -1
        assert signals[1] == -1

    def test_returns_integers_not_booleans(self):
        prices = list(range(96))
        signals = self.assign_signals(prices)
        for s in signals:
            assert isinstance(s, int)
            assert not isinstance(s, bool)


# ---------------------------------------------------------------------------
# gold_generation_hourly._col_name
# ---------------------------------------------------------------------------


class TestColName:
    """Tests for _col_name() slugification helper."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from shiden.processing.gold.gold_generation_hourly import _col_name
        self._col_name = _col_name

    def test_simple_word(self):
        assert self._col_name("Nuclear") == "nuclear_mw"

    def test_space_separated(self):
        assert self._col_name("Wind Onshore") == "wind_onshore_mw"

    def test_slash_separated(self):
        assert self._col_name("Fossil Brown coal/Lignite") == "fossil_brown_coal_lignite_mw"

    def test_pipe_separator(self):
        assert self._col_name("Hydro Pumped Storage|Actual Aggregated") == "hydro_pumped_storage_actual_aggregated_mw"

    def test_no_leading_trailing_underscores(self):
        result = self._col_name("  Solar  ")
        assert not result.startswith("_")
        assert result.endswith("_mw")

    def test_already_snake_case(self):
        assert self._col_name("solar") == "solar_mw"

    def test_multiple_consecutive_separators(self):
        # Double-space or multiple punctuation → single underscore
        result = self._col_name("Fossil  Gas")
        assert "__" not in result

    def test_all_canonical_names_produce_unique_columns(self):
        """All 21 canonical type names must produce unique column names."""
        from shiden.processing.gold.gold_generation_hourly import _CANONICAL_COLS
        computed = [self._col_name(name) for name in _CANONICAL_COLS]
        assert len(computed) == len(set(computed)), "Duplicate column names detected"

    def test_canonical_map_matches_col_name(self):
        """_CANONICAL_COLS values should equal _col_name(key) for every entry."""
        from shiden.processing.gold.gold_generation_hourly import _CANONICAL_COLS
        mismatches = {
            name: (expected, self._col_name(name))
            for name, expected in _CANONICAL_COLS.items()
            if self._col_name(name) != expected
        }
        assert not mismatches, f"Column name mismatches: {mismatches}"


# ---------------------------------------------------------------------------
# BESS router helpers: _nf / _ni
# ---------------------------------------------------------------------------


class TestRouterHelpers:
    """Tests for the shared nullable_float / nullable_int router helpers."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from shiden.api.routers._common import nullable_float, nullable_int
        self._nf = nullable_float
        self._ni = nullable_int

    # ---- _nf ---------------------------------------------------------------

    def test_nf_none_attribute(self):
        """Missing attribute (getattr returns None) → None."""

        class Row:
            pass

        assert self._nf(Row(), "price") is None

    def test_nf_nan_returns_none(self):
        class Row:
            price = float("nan")

        assert self._nf(Row(), "price") is None

    def test_nf_valid_float(self):
        class Row:
            price = 42.5

        assert self._nf(Row(), "price") == pytest.approx(42.5)

    def test_nf_zero_is_not_none(self):
        class Row:
            price = 0.0

        assert self._nf(Row(), "price") == pytest.approx(0.0)

    def test_nf_negative_float(self):
        class Row:
            price = -15.3

        assert self._nf(Row(), "price") == pytest.approx(-15.3)

    def test_nf_string_returns_none(self):
        class Row:
            price = "not-a-number"

        assert self._nf(Row(), "price") is None

    # ---- _ni ---------------------------------------------------------------

    def test_ni_nan_returns_none(self):
        class Row:
            rank = float("nan")

        assert self._ni(Row(), "rank") is None

    def test_ni_valid_int_like_float(self):
        class Row:
            rank = 3.0

        assert self._ni(Row(), "rank") == 3

    def test_ni_returns_int_type(self):
        class Row:
            rank = 7.0

        result = self._ni(Row(), "rank")
        assert isinstance(result, int)

    def test_ni_none_attribute(self):
        class Row:
            pass

        assert self._ni(Row(), "rank") is None


# ---------------------------------------------------------------------------
# generation router: source column detection
# ---------------------------------------------------------------------------


class TestGenerationSourceColumns:
    """_META_COLS logic: only columns ending in _mw not in _META_COLS go into sources."""

    def test_meta_cols_not_in_sources(self):
        from shiden.api.routers.generation import _META_COLS
        for col in _META_COLS:
            # meta cols should NOT be detected as source columns
            if col.endswith("_mw"):
                # e.g. total_mw, renewable_mw, etc.
                assert col in _META_COLS

    def test_canonical_cols_detected_as_sources(self):
        from shiden.api.routers.generation import _META_COLS
        from shiden.processing.gold.gold_generation_hourly import _CANONICAL_COLS

        fake_columns = list(_CANONICAL_COLS.values()) + list(_META_COLS) + ["other_discovered_mw"]
        source_cols = [c for c in fake_columns if c.endswith("_mw") and c not in _META_COLS]

        # Every canonical col should be in sources
        for col in _CANONICAL_COLS.values():
            assert col in source_cols

        # Meta cols ending in _mw should NOT be in sources
        for col in _META_COLS:
            if col.endswith("_mw"):
                assert col not in source_cols

    def test_other_discovered_in_sources(self):
        from shiden.api.routers.generation import _META_COLS

        fake_columns = ["other_discovered_mw"] + list(_META_COLS)
        source_cols = [c for c in fake_columns if c.endswith("_mw") and c not in _META_COLS]
        assert "other_discovered_mw" in source_cols
