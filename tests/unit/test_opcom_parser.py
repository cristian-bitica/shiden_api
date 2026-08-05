"""Unit tests for OPCOM CSV parser and number parsing."""

from __future__ import annotations

from datetime import date
from textwrap import dedent
from unittest.mock import MagicMock, patch

import pytest

from shiden.ingestion.opcom import DataNotAvailableError, OpcomIngester
from shiden.processing.bronze.opcom import _parse_csv, _parse_float

# ---------------------------------------------------------------------------
# _parse_float
# ---------------------------------------------------------------------------


class TestParseFloat:
    def test_simple_decimal(self) -> None:
        assert _parse_float("636.23") == pytest.approx(636.23)

    def test_integer_string(self) -> None:
        assert _parse_float("400") == pytest.approx(400.0)

    def test_romanian_locale_comma_decimal(self) -> None:
        # e.g. "1.638,5" in HTML table → 1638.5
        assert _parse_float("1.638,5") == pytest.approx(1638.5)

    def test_romanian_locale_no_thousands(self) -> None:
        # "676,75" → 676.75
        assert _parse_float("676,75") == pytest.approx(676.75)

    def test_whitespace_stripped(self) -> None:
        assert _parse_float("  797.74  ") == pytest.approx(797.74)

    def test_zero(self) -> None:
        assert _parse_float("0") == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# _parse_csv — sample CSV content
# ---------------------------------------------------------------------------

_CSV_ZONE_HEADER = (
    '"Zona de tranzactionare","Interval"'
    ',"Pret de Inchidere a Pietei [lei/MWh]"'
    ',"Volum Tranzactionat [MW]"'
    ',"Volum Tranzactionat pe cumparare [MW]"'
    ',"Volum Tranzactionat pe vanzare [MW]","Rezolutie"'
)

_SAMPLE_CSV = dedent(
    f"""\
    "PIP si volum tranzactionat pentru ziua de livrare: 02/06/2026"

    "","Pret mediu [lei/MWh]","Volum [MWh]","Rezolutie"
    "ROPEX_DAM_Base (1-24)","636.23","39032.4","PT15M"
    "ROPEX_DAM_Peak (33-80)","547.12","21639.8","PT15M"
    "ROPEX_DAM_Off_Peak (1-32) & (81-96)","725.33","17392.5","PT15M"

    {_CSV_ZONE_HEADER}
    "Romania","1","797.74","1170.8","825.6","1170.8","PT15M"
    "Romania","2","755.29","1199.1","812.2","1199.1","PT15M"
    "Romania","3","733.55","1226.5","802.2","1226.5","PT15M"
    """
)


class TestParseCsv:
    def setup_method(self) -> None:
        self.delivery_date = date(2026, 6, 2)

    def test_returns_correct_interval_count(self) -> None:
        records = _parse_csv(_SAMPLE_CSV, self.delivery_date)
        assert len(records) == 3

    def test_first_record_values(self) -> None:
        records = _parse_csv(_SAMPLE_CSV, self.delivery_date)
        r = records[0]
        assert r.interval_15min == 1
        assert r.price_lei_mwh == pytest.approx(797.74)
        assert r.volume_mw == pytest.approx(1170.8)
        assert r.volume_buy_mw == pytest.approx(825.6)
        assert r.volume_sell_mw == pytest.approx(1170.8)

    def test_summary_rows_excluded(self) -> None:
        records = _parse_csv(_SAMPLE_CSV, self.delivery_date)
        intervals = [r.interval_15min for r in records]
        # Summary rows have non-integer interval labels — must not appear
        assert all(isinstance(i, int) for i in intervals)

    def test_empty_csv_returns_empty_list(self) -> None:
        records = _parse_csv("", self.delivery_date)
        assert records == []


# ---------------------------------------------------------------------------
# fetch() — HTTP mocking
# ---------------------------------------------------------------------------


def _make_full_csv(delivery_date: date) -> str:
    """Build a synthetic PT15 CSV with 96 intervals."""
    lines = [
        (
            '"PIP si volum tranzactionat pentru ziua de livrare:'
            f' {delivery_date.strftime("%d/%m/%Y")}"'
        ),
        "",
        '"","Pret mediu [lei/MWh]","Volum [MWh]","Rezolutie"',
        '"ROPEX_DAM_Base (1-24)","600.00","38000.0","PT15M"',
        "",
        _CSV_ZONE_HEADER,
    ]
    for i in range(1, 97):
        lines.append(f'"Romania","{i}","600.00","1000.0","500.0","1000.0","PT15M"')
    return "\n".join(lines)


class TestOpcomIngesterFetch:
    """The ingester delegates HTTP (incl. retries) to
    shiden.ingestion.http.get_text_with_retry — tests mock that seam."""

    def test_fetch_returns_raw_csv(self) -> None:
        target_date = date(2026, 5, 30)
        csv_text = _make_full_csv(target_date)

        fetch = MagicMock(return_value=csv_text)
        with patch("shiden.ingestion.opcom.get_text_with_retry", fetch):
            result = OpcomIngester().fetch("RO", target_date, target_date)

        assert result["market_id"] == "RO"
        assert len(result["delivery_dates"]) == 1
        day = result["delivery_dates"][0]
        assert day["date"] == target_date.isoformat()
        assert "raw_csv" in day
        assert "Zona de tranzactionare" in day["raw_csv"]

    def test_fetch_raises_on_empty_response(self) -> None:
        target_date = date(2026, 5, 30)
        # Completely empty body triggers DataNotAvailableError
        fetch = MagicMock(return_value="   ")
        with patch("shiden.ingestion.opcom.get_text_with_retry", fetch):
            with pytest.raises(DataNotAvailableError):
                OpcomIngester().fetch("RO", target_date, target_date)

    def test_fetch_url_contains_correct_date(self) -> None:
        target_date = date(2026, 5, 1)
        csv_text = _make_full_csv(target_date)

        fetch = MagicMock(return_value=csv_text)
        with patch("shiden.ingestion.opcom.get_text_with_retry", fetch):
            result = OpcomIngester().fetch("RO", target_date, target_date)

        called_url = fetch.call_args[0][0]
        assert "01/05/2026" in called_url
        assert result["delivery_dates"][0]["source_url"] == called_url
