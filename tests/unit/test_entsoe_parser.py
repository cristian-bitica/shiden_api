"""Unit tests for ENTSO-E ingester and Bronze parser helpers.

All tests are pure Python — no Spark, no network, no entsoe-py import needed.
Tests cover:
  - _generation_to_records()     : flat columns, MultiIndex columns, NaN handling
  - _load_to_records()           : normal values, NaN handling
  - _filter_day()                : UTC date filtering
  - _ts_to_utc_str()             : tz-aware and naive timestamps
  - _parse_landing_generation_json() : happy path, null actual_mw, missing file,
                                       bad JSON, bad record
  - _parse_landing_load_json()   : happy path, null actual_load_mw, missing file,
                                   bad JSON, bad record
"""

from __future__ import annotations

import json
from datetime import date, datetime

import pandas as pd
import pytest

from shiden.ingestion.entsoe import (
    _date_range,
    _filter_day,
    _generation_to_records,
    _load_to_records,
    _ts_to_utc_str,
)
from shiden.processing.bronze.entsoe import (
    _parse_landing_generation_json,
    _parse_landing_load_json,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_INGESTED_AT = datetime(2024, 1, 16, 8, 0, 0)  # naive UTC

_GEN_JSON = {
    "market_id": "RO",
    "date": "2024-01-15",
    "bidding_zone": "10YRO-TEL------P",
    "document_type": "A75",
    "fetched_at": "2024-01-16T06:00:00+00:00",
    "records": [
        {
            "timestamp_utc": "2024-01-15T00:00:00",
            "production_type": "Wind Onshore",
            "actual_mw": 1823.0,
        },
        {
            "timestamp_utc": "2024-01-15T00:00:00",
            "production_type": "Nuclear",
            "actual_mw": 1400.0,
        },
        {
            "timestamp_utc": "2024-01-15T01:00:00",
            "production_type": "Wind Onshore",
            "actual_mw": 1756.0,
        },
        {
            "timestamp_utc": "2024-01-15T01:00:00",
            "production_type": "Nuclear",
            "actual_mw": None,
        },
    ],
}

_LOAD_JSON = {
    "market_id": "RO",
    "date": "2024-01-15",
    "bidding_zone": "10YRO-TEL------P",
    "document_type": "A65",
    "fetched_at": "2024-01-16T06:00:00+00:00",
    "records": [
        {"timestamp_utc": "2024-01-15T00:00:00", "actual_load_mw": 7234.0},
        {"timestamp_utc": "2024-01-15T01:00:00", "actual_load_mw": 7180.0},
        {"timestamp_utc": "2024-01-15T02:00:00", "actual_load_mw": None},
    ],
}


# ---------------------------------------------------------------------------
# _ts_to_utc_str
# ---------------------------------------------------------------------------


class TestTsToUtcStr:
    def test_tz_aware_converts_to_utc(self):
        ts = pd.Timestamp("2024-01-15T02:00:00+02:00")
        assert _ts_to_utc_str(ts) == "2024-01-15T00:00:00"

    def test_utc_timestamp(self):
        ts = pd.Timestamp("2024-01-15T00:00:00+00:00")
        assert _ts_to_utc_str(ts) == "2024-01-15T00:00:00"

    def test_naive_timestamp_passes_through(self):
        ts = pd.Timestamp("2024-01-15T12:30:00")
        assert _ts_to_utc_str(ts) == "2024-01-15T12:30:00"


# ---------------------------------------------------------------------------
# _filter_day
# ---------------------------------------------------------------------------


class TestFilterDay:
    def _make_df(self):
        idx = pd.DatetimeIndex(
            [
                "2024-01-14T23:00:00+00:00",  # previous day
                "2024-01-15T00:00:00+00:00",  # target
                "2024-01-15T12:00:00+00:00",  # target
                "2024-01-15T23:00:00+00:00",  # target
                "2024-01-16T00:00:00+00:00",  # next day
            ],
            tz="UTC",
        )
        return pd.DataFrame({"value": range(5)}, index=idx)

    def test_returns_only_target_date_rows(self):
        df = self._make_df()
        result = _filter_day(df, date(2024, 1, 15))
        assert len(result) == 3

    def test_excludes_adjacent_days(self):
        df = self._make_df()
        assert len(_filter_day(df, date(2024, 1, 14))) == 1
        assert len(_filter_day(df, date(2024, 1, 16))) == 1

    def test_no_match_returns_empty(self):
        df = self._make_df()
        assert _filter_day(df, date(2024, 1, 20)).empty


# ---------------------------------------------------------------------------
# _generation_to_records — flat columns
# ---------------------------------------------------------------------------


class TestGenerationToRecordsFlatColumns:
    def _make_df(self):
        idx = pd.DatetimeIndex(
            ["2024-01-15T00:00:00+00:00", "2024-01-15T01:00:00+00:00"], tz="UTC"
        )
        return pd.DataFrame(
            {
                "Wind Onshore": [1823.0, 1756.0],
                "Nuclear": [1400.0, float("nan")],
                "Fossil Gas": [832.0, 890.0],
            },
            index=idx,
        )

    def test_record_count(self):
        df = self._make_df()
        records = _generation_to_records(df)
        # 2 timestamps × 3 production types = 6
        assert len(records) == 6

    def test_record_keys(self):
        records = _generation_to_records(self._make_df())
        assert set(records[0].keys()) == {
            "timestamp_utc",
            "production_type",
            "actual_mw",
        }

    def test_timestamp_is_naive_utc_string(self):
        records = _generation_to_records(self._make_df())
        assert records[0]["timestamp_utc"] == "2024-01-15T00:00:00"

    def test_nan_becomes_null(self):
        records = _generation_to_records(self._make_df())
        nuclear_h1 = next(
            r
            for r in records
            if r["production_type"] == "Nuclear"
            and r["timestamp_utc"] == "2024-01-15T01:00:00"
        )
        assert nuclear_h1["actual_mw"] is None

    def test_normal_value_is_float(self):
        records = _generation_to_records(self._make_df())
        wind_h0 = next(
            r
            for r in records
            if r["production_type"] == "Wind Onshore"
            and r["timestamp_utc"] == "2024-01-15T00:00:00"
        )
        assert wind_h0["actual_mw"] == pytest.approx(1823.0)


# ---------------------------------------------------------------------------
# _generation_to_records — MultiIndex columns
# ---------------------------------------------------------------------------


class TestGenerationToRecordsMultiIndex:
    def _make_df(self):
        idx = pd.DatetimeIndex(["2024-01-15T00:00:00+00:00"], tz="UTC")
        columns = pd.MultiIndex.from_tuples(
            [
                ("Wind Onshore", "Actual Aggregated"),
                ("Hydro Pumped Storage", "Actual Aggregated"),
                ("Hydro Pumped Storage", "Actual Consumption"),
            ]
        )
        return pd.DataFrame([[1823.0, 450.0, 120.0]], index=idx, columns=columns)

    def test_multiindex_flattened_with_pipe(self):
        records = _generation_to_records(self._make_df())
        prod_types = {r["production_type"] for r in records}
        assert "Wind Onshore|Actual Aggregated" in prod_types
        assert "Hydro Pumped Storage|Actual Aggregated" in prod_types
        assert "Hydro Pumped Storage|Actual Consumption" in prod_types

    def test_multiindex_record_count(self):
        # 1 timestamp × 3 columns
        assert len(_generation_to_records(self._make_df())) == 3

    def test_values_preserved(self):
        records = _generation_to_records(self._make_df())
        pumped_consumption = next(
            r
            for r in records
            if r["production_type"] == "Hydro Pumped Storage|Actual Consumption"
        )
        assert pumped_consumption["actual_mw"] == pytest.approx(120.0)


# ---------------------------------------------------------------------------
# _load_to_records
# ---------------------------------------------------------------------------


class TestLoadToRecords:
    def _make_df(self):
        idx = pd.DatetimeIndex(
            ["2024-01-15T00:00:00+00:00", "2024-01-15T01:00:00+00:00"], tz="UTC"
        )
        return pd.DataFrame({"Actual Load": [7234.0, float("nan")]}, index=idx)

    def test_record_count(self):
        assert len(_load_to_records(self._make_df())) == 2

    def test_record_keys(self):
        records = _load_to_records(self._make_df())
        assert set(records[0].keys()) == {"timestamp_utc", "actual_load_mw"}

    def test_normal_value(self):
        records = _load_to_records(self._make_df())
        assert records[0]["actual_load_mw"] == pytest.approx(7234.0)

    def test_nan_becomes_null(self):
        records = _load_to_records(self._make_df())
        assert records[1]["actual_load_mw"] is None

    def test_timestamp_format(self):
        records = _load_to_records(self._make_df())
        assert records[0]["timestamp_utc"] == "2024-01-15T00:00:00"


# ---------------------------------------------------------------------------
# _parse_landing_generation_json
# ---------------------------------------------------------------------------


class TestParseLandingGenerationJson:
    def test_happy_path(self, tmp_path):
        p = tmp_path / "2024-01-15.json"
        p.write_text(json.dumps(_GEN_JSON), encoding="utf-8")
        rows = _parse_landing_generation_json(p, "RO", _INGESTED_AT)
        assert len(rows) == 4

    def test_market_id_injected(self, tmp_path):
        p = tmp_path / "gen.json"
        p.write_text(json.dumps(_GEN_JSON), encoding="utf-8")
        rows = _parse_landing_generation_json(p, "RO", _INGESTED_AT)
        assert all(r.market_id == "RO" for r in rows)

    def test_ingested_at_set(self, tmp_path):
        p = tmp_path / "gen.json"
        p.write_text(json.dumps(_GEN_JSON), encoding="utf-8")
        rows = _parse_landing_generation_json(p, "RO", _INGESTED_AT)
        assert all(r.ingested_at == _INGESTED_AT for r in rows)

    def test_null_actual_mw_preserved(self, tmp_path):
        p = tmp_path / "gen.json"
        p.write_text(json.dumps(_GEN_JSON), encoding="utf-8")
        rows = _parse_landing_generation_json(p, "RO", _INGESTED_AT)
        null_rows = [r for r in rows if r.actual_mw is None]
        assert len(null_rows) == 1
        assert null_rows[0].production_type == "Nuclear"
        assert null_rows[0].timestamp_utc == datetime(2024, 1, 15, 1, 0, 0)

    def test_numeric_value_parsed(self, tmp_path):
        p = tmp_path / "gen.json"
        p.write_text(json.dumps(_GEN_JSON), encoding="utf-8")
        rows = _parse_landing_generation_json(p, "RO", _INGESTED_AT)
        wind_h0 = next(
            r
            for r in rows
            if r.production_type == "Wind Onshore" and r.timestamp_utc.hour == 0
        )
        assert wind_h0.actual_mw == pytest.approx(1823.0)

    def test_missing_file_returns_empty(self, tmp_path):
        p = tmp_path / "missing.json"
        rows = _parse_landing_generation_json(p, "RO", _INGESTED_AT)
        assert rows == []

    def test_corrupt_json_returns_empty(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("{not valid json", encoding="utf-8")
        rows = _parse_landing_generation_json(p, "RO", _INGESTED_AT)
        assert rows == []

    def test_bad_record_skipped(self, tmp_path):
        data = dict(_GEN_JSON)
        data["records"] = [
            {
                "timestamp_utc": "not-a-date",
                "production_type": "Wind Onshore",
                "actual_mw": 100.0,
            },
            {
                "timestamp_utc": "2024-01-15T00:00:00",
                "production_type": "Nuclear",
                "actual_mw": 1400.0,
            },
        ]
        p = tmp_path / "partial.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        rows = _parse_landing_generation_json(p, "RO", _INGESTED_AT)
        # Bad record skipped; good record returned
        assert len(rows) == 1
        assert rows[0].production_type == "Nuclear"

    def test_empty_records_list(self, tmp_path):
        data = dict(_GEN_JSON, records=[])
        p = tmp_path / "empty.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        assert _parse_landing_generation_json(p, "RO", _INGESTED_AT) == []


# ---------------------------------------------------------------------------
# _parse_landing_load_json
# ---------------------------------------------------------------------------


class TestParseLandingLoadJson:
    def test_happy_path(self, tmp_path):
        p = tmp_path / "2024-01-15.json"
        p.write_text(json.dumps(_LOAD_JSON), encoding="utf-8")
        rows = _parse_landing_load_json(p, "RO", _INGESTED_AT)
        assert len(rows) == 3

    def test_market_id_injected(self, tmp_path):
        p = tmp_path / "load.json"
        p.write_text(json.dumps(_LOAD_JSON), encoding="utf-8")
        rows = _parse_landing_load_json(p, "RO", _INGESTED_AT)
        assert all(r.market_id == "RO" for r in rows)

    def test_null_load_preserved(self, tmp_path):
        p = tmp_path / "load.json"
        p.write_text(json.dumps(_LOAD_JSON), encoding="utf-8")
        rows = _parse_landing_load_json(p, "RO", _INGESTED_AT)
        null_rows = [r for r in rows if r.actual_load_mw is None]
        assert len(null_rows) == 1
        assert null_rows[0].timestamp_utc.hour == 2

    def test_numeric_value_parsed(self, tmp_path):
        p = tmp_path / "load.json"
        p.write_text(json.dumps(_LOAD_JSON), encoding="utf-8")
        rows = _parse_landing_load_json(p, "RO", _INGESTED_AT)
        assert rows[0].actual_load_mw == pytest.approx(7234.0)

    def test_missing_file_returns_empty(self, tmp_path):
        p = tmp_path / "missing.json"
        assert _parse_landing_load_json(p, "RO", _INGESTED_AT) == []

    def test_corrupt_json_returns_empty(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("{{broken", encoding="utf-8")
        assert _parse_landing_load_json(p, "RO", _INGESTED_AT) == []

    def test_bad_record_skipped_good_record_kept(self, tmp_path):
        data = dict(_LOAD_JSON)
        data["records"] = [
            {"timestamp_utc": "INVALID", "actual_load_mw": 999.0},
            {"timestamp_utc": "2024-01-15T00:00:00", "actual_load_mw": 7234.0},
        ]
        p = tmp_path / "partial.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        rows = _parse_landing_load_json(p, "RO", _INGESTED_AT)
        assert len(rows) == 1
        assert rows[0].actual_load_mw == pytest.approx(7234.0)

    def test_ingested_at_set(self, tmp_path):
        p = tmp_path / "load.json"
        p.write_text(json.dumps(_LOAD_JSON), encoding="utf-8")
        rows = _parse_landing_load_json(p, "RO", _INGESTED_AT)
        assert all(r.ingested_at == _INGESTED_AT for r in rows)


# ---------------------------------------------------------------------------
# Path helpers and date_range (pure Python, no network)
# ---------------------------------------------------------------------------


class TestEntsoeIngesterHelpers:
    def test_date_range_inclusive(self):
        dates = list(_date_range(date(2024, 1, 1), date(2024, 1, 3)))
        assert dates == [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]

    def test_date_range_single_day(self):
        result = list(_date_range(date(2024, 6, 1), date(2024, 6, 1)))
        assert result == [date(2024, 6, 1)]

    def test_gen_landing_path_contains_market_and_date(self):
        from shiden.ingestion.entsoe import _gen_landing_path

        p = _gen_landing_path("RO", date(2024, 1, 15))
        assert "entsoe/RO/generation" in str(p)
        assert "2024-01-15.json" in str(p)

    def test_load_landing_path_contains_market_and_date(self):
        from shiden.ingestion.entsoe import _load_landing_path

        p = _load_landing_path("RO", date(2024, 1, 15))
        assert "entsoe/RO/load" in str(p)
        assert "2024-01-15.json" in str(p)

    def test_filter_day_naive_index(self):
        idx = pd.DatetimeIndex(
            ["2024-01-15T00:00:00", "2024-01-15T12:00:00", "2024-01-16T00:00:00"]
        )
        df = pd.DataFrame({"value": [1, 2, 3]}, index=idx)
        result = _filter_day(df, date(2024, 1, 15))
        assert len(result) == 2


# ---------------------------------------------------------------------------
# EntsoeIngester guard — no Spark, no network needed
# ---------------------------------------------------------------------------


class TestEntsoeIngesterApiKeyGuard:
    def test_raises_when_api_key_empty(self, monkeypatch):
        from shiden.config.settings import settings
        from shiden.ingestion.entsoe import EntsoeIngester

        monkeypatch.setattr(settings, "entsoe_api_key", "")
        with pytest.raises(RuntimeError, match="entsoe_api_key not set"):
            EntsoeIngester().ingest("RO", date(2024, 1, 15), date(2024, 1, 15))
