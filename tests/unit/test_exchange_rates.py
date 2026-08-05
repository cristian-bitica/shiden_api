"""Unit tests for silver/exchange_rates pure helpers (no Spark, no network)."""

from __future__ import annotations

from datetime import date

import pytest

from shiden.processing.silver.exchange_rates import (
    _required_pairs,
    reconcile_and_fill,
)


class TestReconcileAndFill:
    def test_primary_wins_over_secondary(self):
        d = date(2026, 6, 3)
        out = reconcile_and_fill(
            d, d, {d: 5.0}, "BNR", {d: 5.1}, "ECB"
        )
        assert out == [(d, 5.0, "BNR")]

    def test_secondary_fills_primary_gap(self):
        d1, d2 = date(2026, 6, 3), date(2026, 6, 4)
        out = reconcile_and_fill(
            d1, d2, {d1: 5.0}, "BNR", {d2: 5.1}, "ECB"
        )
        assert out == [(d1, 5.0, "BNR"), (d2, 5.1, "ECB")]

    def test_gap_forward_filled_from_last_observation(self):
        d1, d2, d3 = date(2026, 6, 5), date(2026, 6, 6), date(2026, 6, 7)
        out = reconcile_and_fill(d1, d3, {d1: 5.0}, "BNR")
        assert out == [
            (d1, 5.0, "BNR"),
            (d2, 5.0, "FORWARD_FILLED"),
            (d3, 5.0, "FORWARD_FILLED"),
        ]

    def test_fill_seeded_from_lookback_before_start(self):
        # Regression for the weekend-nulling bug: a window starting on a
        # Saturday must inherit Friday's rate via the lookback seed.
        friday = date(2026, 6, 5)
        saturday = date(2026, 6, 6)
        sunday = date(2026, 6, 7)
        out = reconcile_and_fill(
            saturday,
            sunday,
            {friday: 5.0},
            "BNR",
            fill_start=friday,
        )
        assert out == [
            (saturday, 5.0, "FORWARD_FILLED"),
            (sunday, 5.0, "FORWARD_FILLED"),
        ]

    def test_no_backfill_before_first_observation(self):
        d1, d2 = date(2026, 6, 3), date(2026, 6, 4)
        out = reconcile_and_fill(d1, d2, {d2: 5.0}, "BNR")
        assert out == [(d2, 5.0, "BNR")]  # d1 omitted, not back-filled

    def test_forward_fill_uses_latest_not_first(self):
        d1, d2, d3 = date(2026, 6, 3), date(2026, 6, 4), date(2026, 6, 5)
        out = reconcile_and_fill(d1, d3, {d1: 5.0, d2: 5.2}, "BNR")
        assert out[-1] == (d3, 5.2, "FORWARD_FILLED")

    def test_secondary_observation_also_seeds_fill(self):
        d1, d2 = date(2026, 6, 3), date(2026, 6, 4)
        out = reconcile_and_fill(d1, d2, {}, "BNR", {d1: 5.1}, "ECB")
        assert out == [(d1, 5.1, "ECB"), (d2, 5.1, "FORWARD_FILLED")]

    def test_empty_sources_yield_nothing(self):
        out = reconcile_and_fill(date(2026, 6, 3), date(2026, 6, 9), {}, "BNR")
        assert out == []


class TestRequiredPairs:
    def test_ro_needs_bnr_ron(self):
        pairs = _required_pairs()
        assert ("RON", "BNR") in pairs

    def test_pairs_are_unique(self):
        pairs = _required_pairs()
        assert len(pairs) == len(set(pairs))

    def test_eur_market_would_be_synthetic(self):
        # Simulate a future EUR market (e.g. Germany) without editing config.
        from unittest.mock import patch

        from shiden.config.markets import MARKETS, MarketConfig

        de = MarketConfig(
            bidding_zone="10Y1001A1001A83F",
            timezone="Europe/Berlin",
            currency="EUR",
            reporting_currency="EUR",
            fx_source="SYNTHETIC",
        )
        with patch.dict(MARKETS, {"DE": de}):
            pairs = _required_pairs()
        assert ("EUR", "SYNTHETIC") in pairs

    def test_gbp_market_would_use_ecb(self):
        from unittest.mock import patch

        from shiden.config.markets import MARKETS, MarketConfig

        gb = MarketConfig(
            bidding_zone="10YGB----------A",
            timezone="Europe/London",
            currency="GBP",
            reporting_currency="EUR",
            fx_source="ECB",
        )
        with patch.dict(MARKETS, {"GB": gb}):
            pairs = _required_pairs()
        assert ("GBP", "ECB") in pairs


class TestRatesAreSane:
    def test_reconcile_preserves_rate_values_exactly(self):
        d = date(2026, 6, 3)
        out = reconcile_and_fill(d, d, {d: 4.9761}, "BNR")
        assert out[0][1] == pytest.approx(4.9761)
