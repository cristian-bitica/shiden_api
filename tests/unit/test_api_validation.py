"""API request-validation tests (no Gold tables required).

Validation runs before any Delta read, so these tests exercise the routers
end-to-end via TestClient without a data lake.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from shiden.api.main import app


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


class TestHealth:
    def test_health_ok(self, client: TestClient) -> None:
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


class TestMarketValidation:
    """Unknown markets must 404 instead of returning an empty 200."""

    @pytest.mark.parametrize(
        "path",
        [
            "/v1/prices/XX?start=2026-06-01&end=2026-06-02",
            "/v1/generation/XX?start=2026-06-01&end=2026-06-02",
            "/v1/weather/XX?start=2026-06-01&end=2026-06-02",
            "/v1/bess/XX/signals?date=2026-06-01",
            "/v1/bess/XX/arbitrage-windows?date=2026-06-01",
            "/v1/bess/XX/forecast?target_date=2026-06-01",
        ],
    )
    def test_unknown_market_returns_404(self, client: TestClient, path: str) -> None:
        r = client.get(path)
        assert r.status_code == 404
        assert "Unknown market" in r.json()["detail"]


class TestDateRangeValidation:
    @pytest.mark.parametrize(
        "path",
        [
            "/v1/prices/RO?start=2026-06-05&end=2026-06-01",
            "/v1/generation/RO?start=2026-06-05&end=2026-06-01",
            "/v1/weather/RO?start=2026-06-05&end=2026-06-01",
        ],
    )
    def test_inverted_range_returns_422(self, client: TestClient, path: str) -> None:
        r = client.get(path)
        assert r.status_code == 422

    def test_malformed_date_returns_422(self, client: TestClient) -> None:
        r = client.get("/v1/prices/RO?start=not-a-date&end=2026-06-02")
        assert r.status_code == 422


class TestPlannedEndpoints:
    """Unimplemented endpoints must return 501, not a bare 500."""

    def test_weather_returns_501(self, client: TestClient) -> None:
        r = client.get("/v1/weather/RO?start=2026-06-01&end=2026-06-02")
        assert r.status_code == 501

    def test_forecast_returns_501(self, client: TestClient) -> None:
        r = client.get("/v1/bess/RO/forecast?target_date=2026-06-01")
        assert r.status_code == 501
