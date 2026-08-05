"""Unit tests for config layer — markets registry and settings."""

from __future__ import annotations

import pytest

from shiden.config.markets import MARKETS, get_market


class TestGetMarket:
    def test_ro_returns_correct_bidding_zone(self):
        m = get_market("RO")
        assert m.bidding_zone == "10YRO-TEL------P"

    def test_ro_currency_is_ron(self):
        assert get_market("RO").currency == "RON"

    def test_ro_reporting_currency_is_eur(self):
        assert get_market("RO").reporting_currency == "EUR"

    def test_ro_fx_source_is_bnr(self):
        assert get_market("RO").fx_source == "BNR"

    def test_ro_has_weather_locations(self):
        assert len(get_market("RO").weather_locations) > 0

    def test_unknown_market_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown market"):
            get_market("XX")

    def test_error_message_lists_available_markets(self):
        with pytest.raises(ValueError, match="RO"):
            get_market("INVALID")

    def test_all_markets_have_required_fields(self):
        for market_id, config in MARKETS.items():
            assert config.bidding_zone, f"{market_id} missing bidding_zone"
            assert config.timezone, f"{market_id} missing timezone"
            assert config.currency, f"{market_id} missing currency"
