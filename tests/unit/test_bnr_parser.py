"""Unit tests for BNR XML parsing logic.

No network access, no Spark.  Tests the _extract_cubes and _parse_landing_xml
helpers in isolation.
"""

from __future__ import annotations

import textwrap
import xml.etree.ElementTree as ET
from datetime import date, datetime

import pytest

from shiden.ingestion.bnr import _date_range, _extract_cubes, _sidecar_path, _xml_path
from shiden.processing.bronze.bnr import _parse_landing_xml

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_BNR_ARCHIVE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="utf-8"?>
    <DataSet xmlns="http://www.bnr.ro/xsd">
      <Header>
        <Publisher>National Bank of Romania</Publisher>
        <PublishingDate>2024-01-16</PublishingDate>
      </Header>
      <Body>
        <Cube date="2024-01-15">
          <Rate currency="EUR">4.9742</Rate>
          <Rate currency="USD">4.5123</Rate>
          <Rate currency="GBP">5.6789</Rate>
          <Rate currency="JPY" multiplier="100">3.4567</Rate>
        </Cube>
        <Cube date="2024-01-12">
          <Rate currency="EUR">4.9700</Rate>
          <Rate currency="USD">4.5000</Rate>
        </Cube>
      </Body>
    </DataSet>
""")

_BNR_DAILY_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="utf-8"?>
    <DataSet xmlns="http://www.bnr.ro/xsd">
      <Header>
        <PublishingDate>2024-01-16</PublishingDate>
      </Header>
      <Body>
        <Cube date="2024-01-16">
          <Rate currency="EUR">4.9800</Rate>
          <Rate currency="USD">4.5200</Rate>
        </Cube>
      </Body>
    </DataSet>
""")

_BNR_SINGLE_CUBE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="utf-8"?>
    <Cube xmlns="http://www.bnr.ro/xsd" date="2024-01-15">
      <Rate currency="EUR">4.9742</Rate>
      <Rate currency="USD">4.5123</Rate>
      <Rate currency="JPY" multiplier="100">3.4567</Rate>
      <Rate currency="CHF">5.2000</Rate>
    </Cube>
""")

_BNR_WITH_NONNUMERIC_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="utf-8"?>
    <Cube xmlns="http://www.bnr.ro/xsd" date="2024-01-15">
      <Rate currency="EUR">4.9742</Rate>
      <Rate currency="XYZ">-</Rate>
    </Cube>
""")


# ---------------------------------------------------------------------------
# Tests for _extract_cubes (ingester layer)
# ---------------------------------------------------------------------------


class TestExtractCubes:
    def test_archive_returns_two_dates(self):
        result = _extract_cubes(_BNR_ARCHIVE_XML, "https://bnr.ro/archive.xml")
        assert set(result.keys()) == {"2024-01-15", "2024-01-12"}

    def test_daily_returns_one_date(self):
        result = _extract_cubes(_BNR_DAILY_XML, "https://bnr.ro/daily.xml")
        assert "2024-01-16" in result
        assert len(result) == 1

    def test_source_url_preserved(self):
        url = "https://bnr.ro/files/xml/years/nbrfxrates2024.xml"
        result = _extract_cubes(_BNR_ARCHIVE_XML, url)
        _, src = result["2024-01-15"]
        assert src == url

    def test_cube_xml_is_valid_xml(self):
        result = _extract_cubes(_BNR_ARCHIVE_XML, "url")
        cube_xml, _ = result["2024-01-15"]
        # Should be parseable as XML
        elem = ET.fromstring(cube_xml)
        assert elem is not None

    def test_invalid_xml_raises_value_error(self):
        with pytest.raises(ValueError, match="XML parse error"):
            _extract_cubes("this is not xml", "url")

    def test_empty_xml_returns_empty_dict(self):
        result = _extract_cubes(
            '<?xml version="1.0"?><root xmlns="http://www.bnr.ro/xsd"></root>',
            "url",
        )
        assert result == {}


# ---------------------------------------------------------------------------
# Tests for _parse_landing_xml (Bronze layer)
# ---------------------------------------------------------------------------

_INGESTED_AT = datetime(2024, 1, 16, 12, 0, 0)
_TARGET_DATE = date(2024, 1, 15)


class TestParseLandingXml:
    def test_basic_parse(self, tmp_path):
        p = tmp_path / "2024-01-15.xml"
        p.write_text(_BNR_SINGLE_CUBE_XML, encoding="utf-8")
        rows = _parse_landing_xml(p, _TARGET_DATE, "https://bnr.ro", _INGESTED_AT)
        assert len(rows) == 4

    def test_eur_row_values(self, tmp_path):
        p = tmp_path / "2024-01-15.xml"
        p.write_text(_BNR_SINGLE_CUBE_XML, encoding="utf-8")
        rows = _parse_landing_xml(p, _TARGET_DATE, "https://bnr.ro", _INGESTED_AT)
        eur = next(r for r in rows if r.foreign_currency == "EUR")
        assert eur.rate_ron == pytest.approx(4.9742)
        assert eur.multiplier == 1
        assert eur.date == _TARGET_DATE

    def test_jpy_multiplier_preserved_raw(self, tmp_path):
        """Bronze keeps raw multiplier; Silver normalises. multiplier=100 for JPY."""
        p = tmp_path / "2024-01-15.xml"
        p.write_text(_BNR_SINGLE_CUBE_XML, encoding="utf-8")
        rows = _parse_landing_xml(p, _TARGET_DATE, "https://bnr.ro", _INGESTED_AT)
        jpy = next(r for r in rows if r.foreign_currency == "JPY")
        assert jpy.multiplier == 100
        assert jpy.rate_ron == pytest.approx(3.4567)

    def test_nonnumeric_rate_skipped(self, tmp_path):
        """A '-' rate string must be skipped without raising."""
        p = tmp_path / "2024-01-15.xml"
        p.write_text(_BNR_WITH_NONNUMERIC_XML, encoding="utf-8")
        rows = _parse_landing_xml(p, _TARGET_DATE, "https://bnr.ro", _INGESTED_AT)
        currencies = {r.foreign_currency for r in rows}
        assert "EUR" in currencies
        assert "XYZ" not in currencies

    def test_source_url_stored(self, tmp_path):
        p = tmp_path / "2024-01-15.xml"
        p.write_text(_BNR_SINGLE_CUBE_XML, encoding="utf-8")
        rows = _parse_landing_xml(p, _TARGET_DATE, "https://bnr.ro/test", _INGESTED_AT)
        assert all(r.source_url == "https://bnr.ro/test" for r in rows)

    def test_ingested_at_stored(self, tmp_path):
        p = tmp_path / "2024-01-15.xml"
        p.write_text(_BNR_SINGLE_CUBE_XML, encoding="utf-8")
        rows = _parse_landing_xml(p, _TARGET_DATE, "url", _INGESTED_AT)
        assert all(r.ingested_at == _INGESTED_AT for r in rows)

    def test_missing_file_returns_empty(self, tmp_path):
        p = tmp_path / "nonexistent.xml"
        rows = _parse_landing_xml(p, _TARGET_DATE, "url", _INGESTED_AT)
        assert rows == []

    def test_date_taken_from_xml_attribute(self, tmp_path):
        """The date in the Cube attribute should override the target_date fallback."""
        p = tmp_path / "2024-01-15.xml"
        p.write_text(_BNR_SINGLE_CUBE_XML, encoding="utf-8")
        rows = _parse_landing_xml(p, date(2099, 1, 1), "url", _INGESTED_AT)
        # XML says 2024-01-15, so that should win
        assert all(r.date == date(2024, 1, 15) for r in rows)

    def test_corrupt_xml_returns_empty(self, tmp_path):
        """Corrupt XML in landing zone must return [] without raising."""
        p = tmp_path / "2024-01-15.xml"
        p.write_text(
            "<?xml version='1.0'?><Cube date='2024-01-15'><Rate",
            encoding="utf-8",
        )
        rows = _parse_landing_xml(p, _TARGET_DATE, "url", _INGESTED_AT)
        assert rows == []


# ---------------------------------------------------------------------------
# Ingester path helpers and date_range (pure Python, no network)
# ---------------------------------------------------------------------------


class TestBnrIngesterHelpers:
    def test_date_range_inclusive(self):
        dates = list(_date_range(date(2024, 1, 1), date(2024, 1, 3)))
        assert dates == [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]

    def test_date_range_single_day(self):
        result = list(_date_range(date(2024, 6, 1), date(2024, 6, 1)))
        assert result == [date(2024, 6, 1)]

    def test_xml_path_contains_date_and_year(self):
        p = _xml_path(date(2024, 1, 15))
        assert "2024-01-15.xml" in str(p)
        assert "2024" in str(p)
        assert "bnr_fx_rates" in str(p)

    def test_sidecar_path_is_json(self):
        p = _sidecar_path(date(2024, 1, 15))
        assert str(p).endswith("2024-01-15.json")
        assert "bnr_fx_rates" in str(p)

    def test_xml_and_sidecar_share_directory(self):
        assert (
            _xml_path(date(2024, 3, 5)).parent == _sidecar_path(date(2024, 3, 5)).parent
        )
