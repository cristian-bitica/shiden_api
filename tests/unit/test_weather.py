"""Unit tests for weather ingestion and Bronze parsing."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from shiden.processing.bronze.weather import WeatherRow, _maybe_float, _parse_landing

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_open_meteo_response(n_hours: int = 24) -> dict[str, Any]:
    """Build a minimal Open-Meteo API response."""
    times = [f"2026-06-03T{h:02d}:00" for h in range(n_hours)]
    return {
        "latitude": 44.46,
        "longitude": 26.09,
        "timezone": "UTC",
        "hourly": {
            "time": times,
            "temperature_2m": [20.0] * n_hours,
            "wind_speed_10m": [10.0] * n_hours,
            "wind_speed_100m": [20.0] * n_hours,
            "shortwave_radiation": [100.0] * n_hours,
            "cloud_cover": [30.0] * n_hours,
        },
    }


@pytest.fixture()
def mock_fetch():
    """Patch the shared HTTP helper at the ingester's seam.

    The ingester delegates HTTP (incl. retries) to
    shiden.ingestion.http.get_text_with_retry, so tests mock that function
    and return the Open-Meteo JSON as text.
    """
    fetch = MagicMock(return_value=json.dumps(_make_open_meteo_response()))
    with patch("shiden.ingestion.weather.get_text_with_retry", fetch):
        yield fetch


# ---------------------------------------------------------------------------
# _maybe_float
# ---------------------------------------------------------------------------


class TestMaybeFloat:
    def test_normal_value(self) -> None:
        assert _maybe_float([1.5, 2.5], 0) == pytest.approx(1.5)

    def test_none_in_series(self) -> None:
        assert _maybe_float([None, 2.5], 0) is None

    def test_none_series(self) -> None:
        assert _maybe_float(None, 0) is None

    def test_index_out_of_range(self) -> None:
        assert _maybe_float([1.0], 5) is None

    def test_integer_coerced_to_float(self) -> None:
        assert isinstance(_maybe_float([4], 0), float)


# ---------------------------------------------------------------------------
# _parse_landing — landing JSON → list[WeatherRow]
# ---------------------------------------------------------------------------

_SAMPLE_PAYLOAD: dict[str, Any] = {
    "market_id": "RO",
    "location": "Bucharest",
    "lat": 44.43,
    "lon": 26.09,
    "date": "2026-06-03",
    "fetched_at": "2026-06-04T08:00:00+00:00",
    "source_url": "https://archive-api.open-meteo.com/v1/archive?latitude=44.43&longitude=26.09",
    "hourly": {
        "time": ["2026-06-03T00:00", "2026-06-03T01:00", "2026-06-03T02:00"],
        "temperature_2m": [17.2, 16.8, 16.3],
        "wind_speed_10m": [8.6, 7.9, 7.2],
        "wind_speed_100m": [19.3, 18.1, 17.4],
        "shortwave_radiation": [0.0, 0.0, 0.0],
        "cloud_cover": [2.0, 5.0, 8.0],
    },
}


class TestParseLanding:
    def setup_method(self) -> None:
        self.ingested_at = datetime(2026, 6, 4, 8, 0)  # naive UTC

    def test_row_count_matches_hours(self) -> None:
        rows = _parse_landing(_SAMPLE_PAYLOAD, "RO", self.ingested_at)
        assert len(rows) == 3

    def test_first_row_fields(self) -> None:
        row = _parse_landing(_SAMPLE_PAYLOAD, "RO", self.ingested_at)[0]
        assert isinstance(row, WeatherRow)
        assert row.market_id == "RO"
        assert row.location_name == "Bucharest"
        assert row.lat == pytest.approx(44.43)
        assert row.lon == pytest.approx(26.09)
        assert row.timestamp_utc == datetime(2026, 6, 3, 0, 0)  # naive UTC
        assert row.temperature_2m == pytest.approx(17.2)
        assert row.wind_speed_10m == pytest.approx(8.6)
        assert row.wind_speed_100m == pytest.approx(19.3)
        assert row.shortwave_radiation == pytest.approx(0.0)
        assert row.cloud_cover == pytest.approx(2.0)
        assert row.ingested_at == self.ingested_at

    def test_source_url_is_read_from_payload(self) -> None:
        row = _parse_landing(_SAMPLE_PAYLOAD, "RO", self.ingested_at)[0]
        assert row.source_url == _SAMPLE_PAYLOAD["source_url"]

    def test_source_url_is_none_when_absent(self) -> None:
        payload = {k: v for k, v in _SAMPLE_PAYLOAD.items() if k != "source_url"}
        row = _parse_landing(payload, "RO", self.ingested_at)[0]
        assert row.source_url is None

    def test_null_variable_propagates(self) -> None:
        payload = {
            **_SAMPLE_PAYLOAD,
            "hourly": {
                **_SAMPLE_PAYLOAD["hourly"],
                "temperature_2m": [None, 16.8, 16.3],
            },
        }
        rows = _parse_landing(payload, "RO", self.ingested_at)
        assert rows[0].temperature_2m is None
        assert rows[1].temperature_2m == pytest.approx(16.8)


# ---------------------------------------------------------------------------
# WeatherIngester.fetch() — HTTP mocking
# ---------------------------------------------------------------------------


class TestWeatherIngesterFetch:
    def test_fetch_returns_all_locations(self, mock_fetch) -> None:
        from shiden.config.markets import get_market
        from shiden.ingestion.weather import WeatherIngester

        result = WeatherIngester().fetch("RO", date(2026, 6, 3), date(2026, 6, 3))

        n_locations = len(get_market("RO").weather_locations)
        assert result["market_id"] == "RO"
        assert len(result["locations"]) == n_locations
        assert mock_fetch.call_count == n_locations

    def test_fetch_uses_historical_url_for_past_dates(self, mock_fetch) -> None:
        from shiden.ingestion.weather import OPEN_METEO_HISTORICAL_URL, WeatherIngester

        WeatherIngester().fetch("RO", date(2026, 1, 1), date(2026, 1, 1))

        args, _ = mock_fetch.call_args
        assert args[0] == OPEN_METEO_HISTORICAL_URL

    def test_fetch_uses_forecast_url_for_today(self, mock_fetch) -> None:
        from shiden.ingestion.weather import OPEN_METEO_FORECAST_URL, WeatherIngester

        WeatherIngester().fetch("RO", date.today(), date.today())

        args, _ = mock_fetch.call_args
        assert args[0] == OPEN_METEO_FORECAST_URL

    def test_fetch_passes_utc_timezone_param(self, mock_fetch) -> None:
        from shiden.ingestion.weather import WeatherIngester

        WeatherIngester().fetch("RO", date(2026, 6, 3), date(2026, 6, 3))

        _, kwargs = mock_fetch.call_args
        assert kwargs["params"]["timezone"] == "UTC"

    def test_fetch_skips_location_on_bad_response_shape(self) -> None:
        from shiden.ingestion.weather import WeatherIngester

        bad = MagicMock(return_value=json.dumps({"error": True, "reason": "invalid"}))
        with patch("shiden.ingestion.weather.get_text_with_retry", bad):
            result = WeatherIngester().fetch("RO", date(2026, 6, 3), date(2026, 6, 3))

        assert result["locations"] == []

    def test_fetch_raises_on_http_error(self) -> None:
        from shiden.ingestion.weather import WeatherIngester

        err = httpx.HTTPStatusError("429", request=MagicMock(), response=MagicMock())
        failing = MagicMock(side_effect=err)
        with patch("shiden.ingestion.weather.get_text_with_retry", failing):
            with pytest.raises(httpx.HTTPStatusError):
                WeatherIngester().fetch("RO", date(2026, 6, 3), date(2026, 6, 3))


# ---------------------------------------------------------------------------
# WeatherIngester.ingest() — file writing
# ---------------------------------------------------------------------------


class TestWeatherIngesterIngest:
    def test_ingest_writes_one_file_per_location(
        self, tmp_path, mock_fetch
    ) -> None:
        from shiden.config import settings as settings_module
        from shiden.config.markets import get_market
        from shiden.ingestion.weather import WeatherIngester

        with patch.object(settings_module.settings, "landing_base_path", str(tmp_path)):
            WeatherIngester().ingest("RO", date(2026, 6, 3), date(2026, 6, 3))

        n_locations = len(get_market("RO").weather_locations)
        assert len(list(tmp_path.rglob("*.json"))) == n_locations

    def test_ingest_file_content_is_valid_json(
        self, tmp_path, mock_fetch
    ) -> None:
        from shiden.config import settings as settings_module
        from shiden.ingestion.weather import WeatherIngester

        with patch.object(settings_module.settings, "landing_base_path", str(tmp_path)):
            WeatherIngester().ingest("RO", date(2026, 6, 3), date(2026, 6, 3))

        buch_file = (
            tmp_path / "landing" / "weather" / "RO" / "Bucharest" / "2026-06-03.json"
        )
        assert buch_file.exists()
        data = json.loads(buch_file.read_text())
        assert data["market_id"] == "RO"
        assert data["location"] == "Bucharest"
        assert data["date"] == "2026-06-03"
        assert len(data["hourly"]["time"]) == 24

    def test_ingest_stores_source_url_in_landing_file(
        self, tmp_path, mock_fetch
    ) -> None:
        from shiden.config import settings as settings_module
        from shiden.ingestion.weather import WeatherIngester

        with patch.object(settings_module.settings, "landing_base_path", str(tmp_path)):
            WeatherIngester().ingest("RO", date(2026, 6, 3), date(2026, 6, 3))

        buch_file = (
            tmp_path / "landing" / "weather" / "RO" / "Bucharest" / "2026-06-03.json"
        )
        data = json.loads(buch_file.read_text())
        assert "source_url" in data
        assert "open-meteo.com" in data["source_url"]
        assert "timezone=UTC" in data["source_url"]

    def test_ingest_splits_multi_day_range_into_separate_files(self, tmp_path) -> None:
        from shiden.config import settings as settings_module
        from shiden.config.markets import get_market
        from shiden.ingestion.weather import WeatherIngester

        two_day_response = {
            "latitude": 44.46,
            "longitude": 26.09,
            "timezone": "UTC",
            "hourly": {
                "time": [f"2026-06-0{d}T{h:02d}:00" for d in [3, 4] for h in range(24)],
                **{
                    var: [1.0] * 48
                    for var in [
                        "temperature_2m",
                        "wind_speed_10m",
                        "wind_speed_100m",
                        "shortwave_radiation",
                        "cloud_cover",
                    ]
                },
            },
        }

        fetch = MagicMock(return_value=json.dumps(two_day_response))
        with patch("shiden.ingestion.weather.get_text_with_retry", fetch):
            with patch.object(
                settings_module.settings, "landing_base_path", str(tmp_path)
            ):
                WeatherIngester().ingest("RO", date(2026, 6, 3), date(2026, 6, 4))

        n_locations = len(get_market("RO").weather_locations)
        assert len(list(tmp_path.rglob("*.json"))) == n_locations * 2


# ---------------------------------------------------------------------------
# WeatherBronzeWriter observation key
# ---------------------------------------------------------------------------


class TestObservationKeyIncludesDataType:
    """Regression guard: the Bronze observation key must include data_type.

    The old per-(location, date) skip guard ignored data_type, so once
    FORECAST rows existed for a date, the later REALIZED ingest for the same
    date was skipped forever.  Idempotency now lives in
    delta_io.append_new_observations keyed on
    (market_id, location_name, timestamp_utc, data_type).
    """

    def test_writer_has_no_date_level_skip_guard(self) -> None:
        from shiden.processing.bronze.weather import WeatherBronzeWriter

        assert not hasattr(WeatherBronzeWriter, "_is_already_loaded")

    def test_process_keys_observations_on_data_type(self) -> None:
        import inspect

        from shiden.processing.bronze.weather import WeatherBronzeWriter

        source = inspect.getsource(WeatherBronzeWriter.process)
        assert '"data_type"' in source  # part of key_cols
