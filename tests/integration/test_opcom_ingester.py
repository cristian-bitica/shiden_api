"""
Integration tests for OpcomIngester.

These tests make real HTTP requests to OPCOM and write to a local Delta table.
Run with:  pytest -m integration tests/integration/test_opcom_ingester.py
Skip in CI by not passing -m integration (tests are marked and excluded by default).
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def ingester():
    from shiden.ingestion.opcom import OpcomIngester

    return OpcomIngester()


class TestOpcomIngesterLive:
    def test_fetch_recent_day(self, ingester) -> None:
        """Fetch yesterday's data — should always be available."""
        yesterday = date.today() - timedelta(days=1)
        result = ingester.fetch("RO", yesterday, yesterday)

        assert result["market_id"] == "RO"
        assert len(result["delivery_dates"]) == 1
        day = result["delivery_dates"][0]
        assert day["date"] == yesterday.isoformat()

        # fetch() returns raw CSV — parse it to verify interval count
        import datetime as _dt

        from shiden.processing.bronze.opcom import _parse_csv

        records = _parse_csv(day["raw_csv"], _dt.date.fromisoformat(day["date"]))
        assert len(records) == 96, f"Expected 96 intervals, got {len(records)}"

        for rec in records:
            assert 1 <= rec.interval_15min <= 96
            assert rec.price_lei_mwh > 0
            assert rec.volume_mw >= 0
            assert rec.volume_buy_mw >= 0
            assert rec.volume_sell_mw >= 0

    def test_fetch_two_day_range(self, ingester) -> None:
        """Fetch a two-day range and confirm both days are returned."""
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=1)
        result = ingester.fetch("RO", start, end)

        assert len(result["delivery_dates"]) == 2
        dates_returned = [d["date"] for d in result["delivery_dates"]]
        assert start.isoformat() in dates_returned
        assert end.isoformat() in dates_returned

    def test_ingest_writes_to_delta(self, ingester, tmp_path, spark) -> None:
        """End-to-end: fetch + write to a temp Delta table, verify row count."""
        from shiden.config import settings as settings_module
        from shiden.processing.bronze.opcom import OpcomBronzeWriter

        yesterday = date.today() - timedelta(days=1)

        # Override delta_base_path to use a temp directory
        original_path = settings_module.settings.delta_base_path
        settings_module.settings.__dict__["delta_base_path"] = str(tmp_path)

        try:
            ingester.ingest("RO", yesterday, yesterday)
            OpcomBronzeWriter().process("RO", yesterday, yesterday, spark)

            table_path = str(tmp_path / "bronze" / "opcom_pzu_prices")
            df = spark.read.format("delta").load(table_path)
            assert df.count() == 96, f"Expected 96 rows, got {df.count()}"

            # Bronze is append-only: a second run adds another 96 rows
            OpcomBronzeWriter().process("RO", yesterday, yesterday, spark)
            df2 = spark.read.format("delta").load(table_path)
            assert df2.count() == 192, "Expected 192 rows after second append"
        finally:
            settings_module.settings.__dict__["delta_base_path"] = original_path
