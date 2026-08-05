"""Unit tests for ECB XML / JSON parsing logic.

No network access, no Spark.  Tests the _parse_ecb_xml helper (ingester) and
_parse_landing_json helper (Bronze layer) in isolation.
"""

from __future__ import annotations

import json
import textwrap
from datetime import date, datetime
from typing import Any

import pytest

from shiden.ingestion.ecb import _date_range, _landing_path, _parse_ecb_xml
from shiden.processing.bronze.ecb import _parse_landing_json

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_ECB_XML_SAMPLE = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <gesmes:Envelope
        xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01"
        xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
      <gesmes:subject>Reference rates</gesmes:subject>
      <gesmes:Sender><gesmes:name>European Central Bank</gesmes:name></gesmes:Sender>
      <Cube>
        <Cube time="2024-01-15">
          <Cube currency="USD" rate="1.0921"/>
          <Cube currency="GBP" rate="0.8567"/>
          <Cube currency="RON" rate="4.9698"/>
          <Cube currency="JPY" rate="160.35"/>
        </Cube>
        <Cube time="2024-01-12">
          <Cube currency="USD" rate="1.0898"/>
          <Cube currency="GBP" rate="0.8551"/>
          <Cube currency="RON" rate="4.9712"/>
        </Cube>
      </Cube>
    </gesmes:Envelope>
""")

_ECB_XML_NONNUMERIC = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <gesmes:Envelope
        xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01"
        xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
      <Cube>
        <Cube time="2024-01-15">
          <Cube currency="USD" rate="1.0921"/>
          <Cube currency="BAD" rate="N/A"/>
        </Cube>
      </Cube>
    </gesmes:Envelope>
""")

_INGESTED_AT = datetime(2024, 1, 16, 12, 0, 0)
_TARGET_DATE = date(2024, 1, 15)


# ---------------------------------------------------------------------------
# Tests for _parse_ecb_xml (ingester layer)
# ---------------------------------------------------------------------------


class TestParseEcbXml:
    def test_returns_two_dates(self):
        result = _parse_ecb_xml(_ECB_XML_SAMPLE)
        assert set(result.keys()) == {"2024-01-15", "2024-01-12"}

    def test_rate_values_jan15(self):
        result = _parse_ecb_xml(_ECB_XML_SAMPLE)
        rates = result["2024-01-15"]
        assert rates["USD"] == pytest.approx(1.0921)
        assert rates["GBP"] == pytest.approx(0.8567)
        assert rates["RON"] == pytest.approx(4.9698)
        assert rates["JPY"] == pytest.approx(160.35)

    def test_rate_values_jan12(self):
        result = _parse_ecb_xml(_ECB_XML_SAMPLE)
        rates = result["2024-01-12"]
        assert len(rates) == 3
        assert rates["USD"] == pytest.approx(1.0898)

    def test_nonnumeric_rate_skipped(self):
        result = _parse_ecb_xml(_ECB_XML_NONNUMERIC)
        rates = result["2024-01-15"]
        assert "USD" in rates
        assert "BAD" not in rates

    def test_invalid_xml_raises_value_error(self):
        with pytest.raises(ValueError, match="XML parse error"):
            _parse_ecb_xml("not xml at all")

    def test_empty_envelope_returns_empty(self):
        xml = textwrap.dedent("""\
            <?xml version="1.0"?>
            <gesmes:Envelope
                xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01"
                xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
              <Cube/>
            </gesmes:Envelope>
        """)
        result = _parse_ecb_xml(xml)
        assert result == {}

    def test_all_currencies_present(self):
        result = _parse_ecb_xml(_ECB_XML_SAMPLE)
        assert set(result["2024-01-15"].keys()) == {"USD", "GBP", "RON", "JPY"}


# ---------------------------------------------------------------------------
# Tests for _parse_landing_json (Bronze layer)
# ---------------------------------------------------------------------------


def _make_landing_json(
    tmp_path: Any,
    date_str: str,
    rates: dict[str, Any],
    fetched_at: str = "2024-01-16T12:00:00+00:00",
) -> Any:
    p = tmp_path / f"{date_str}.json"
    p.write_text(
        json.dumps(
            {
                "date": date_str,
                "rates": rates,
                "source_url": "https://ecb.europa.eu/eurofxref-hist.xml",
                "fetched_at": fetched_at,
            }
        ),
        encoding="utf-8",
    )
    return p


class TestParseLandingJson:
    def test_basic_parse(self, tmp_path):
        p = _make_landing_json(tmp_path, "2024-01-15", {"USD": 1.0921, "GBP": 0.8567})
        rows = _parse_landing_json(p, _TARGET_DATE, _INGESTED_AT)
        assert len(rows) == 2

    def test_usd_row_values(self, tmp_path):
        p = _make_landing_json(tmp_path, "2024-01-15", {"USD": 1.0921, "GBP": 0.8567})
        rows = _parse_landing_json(p, _TARGET_DATE, _INGESTED_AT)
        usd = next(r for r in rows if r.quote_currency == "USD")
        assert usd.rate == pytest.approx(1.0921)
        assert usd.date == _TARGET_DATE

    def test_source_file_date_from_fetched_at(self, tmp_path):
        p = _make_landing_json(
            tmp_path,
            "2024-01-15",
            {"EUR_RON": 4.97},
            fetched_at="2024-01-16T18:00:00+00:00",
        )
        rows = _parse_landing_json(p, _TARGET_DATE, _INGESTED_AT)
        assert rows[0].source_file_date == date(2024, 1, 16)

    def test_ingested_at_stored(self, tmp_path):
        p = _make_landing_json(tmp_path, "2024-01-15", {"USD": 1.09})
        rows = _parse_landing_json(p, _TARGET_DATE, _INGESTED_AT)
        assert rows[0].ingested_at == _INGESTED_AT

    def test_missing_file_returns_empty(self, tmp_path):
        p = tmp_path / "nonexistent.json"
        rows = _parse_landing_json(p, _TARGET_DATE, _INGESTED_AT)
        assert rows == []

    def test_invalid_json_returns_empty(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("{ not valid json", encoding="utf-8")
        rows = _parse_landing_json(p, _TARGET_DATE, _INGESTED_AT)
        assert rows == []

    def test_bad_rate_value_skipped(self, tmp_path):
        p = _make_landing_json(tmp_path, "2024-01-15", {"USD": 1.09, "BAD": "N/A"})
        rows = _parse_landing_json(p, _TARGET_DATE, _INGESTED_AT)
        currencies = {r.quote_currency for r in rows}
        assert "USD" in currencies
        assert "BAD" not in currencies

    def test_date_from_json_overrides_target_date(self, tmp_path):
        """date field in JSON should override the target_date argument."""
        p = _make_landing_json(tmp_path, "2024-01-15", {"USD": 1.09})
        rows = _parse_landing_json(p, date(2099, 1, 1), _INGESTED_AT)
        assert rows[0].date == date(2024, 1, 15)

    def test_empty_rates_returns_empty(self, tmp_path):
        p = _make_landing_json(tmp_path, "2024-01-15", {})
        rows = _parse_landing_json(p, _TARGET_DATE, _INGESTED_AT)
        assert rows == []

    def test_ron_rate_in_reasonable_range(self, tmp_path):
        """Sanity check: EUR/RON should be around 4.5–5.5."""
        p = _make_landing_json(tmp_path, "2024-01-15", {"RON": 4.9698})
        rows = _parse_landing_json(p, _TARGET_DATE, _INGESTED_AT)
        ron = next(r for r in rows if r.quote_currency == "RON")
        assert 4.0 < ron.rate < 6.0


# ---------------------------------------------------------------------------
# Ingester path helpers and date_range (pure Python, no network)
# ---------------------------------------------------------------------------


class TestEcbIngesterHelpers:
    def test_date_range_inclusive(self):
        dates = list(_date_range(date(2024, 1, 1), date(2024, 1, 3)))
        assert dates == [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]

    def test_date_range_single_day(self):
        result = list(_date_range(date(2024, 6, 1), date(2024, 6, 1)))
        assert result == [date(2024, 6, 1)]

    def test_landing_path_contains_date(self):
        p = _landing_path(date(2024, 1, 15))
        assert "2024-01-15.json" in str(p)
        assert "ecb_fx_rates" in str(p)

    def test_landing_path_is_json(self):
        assert str(_landing_path(date(2024, 3, 5))).endswith(".json")
