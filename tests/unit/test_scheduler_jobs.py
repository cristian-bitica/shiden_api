"""Unit tests for scheduler/jobs.py: daily job step error isolation and the
``--job`` CLI dispatch affordance.

Mirrors ``tests/acceptance/features/daily_job_step_isolation.feature``:
a failing pipeline step inside a daily job must be logged as an ERROR and
must not raise out of the job.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from shiden.scheduler import jobs


class TestRunOpcomPzuDailyErrorIsolation:
    """run_opcom_pzu_daily was the one daily job missing _run_safe isolation."""

    @patch("shiden.processing.spark.get_spark")
    @patch("shiden.processing.bronze.opcom.OpcomBronzeWriter")
    @patch("shiden.ingestion.opcom.OpcomIngester")
    def test_ingest_failure_is_logged_not_raised(
        self, mock_ingester_cls, mock_writer_cls, mock_get_spark, caplog
    ):
        mock_ingester_cls.return_value.ingest.side_effect = RuntimeError("boom")
        mock_get_spark.return_value = MagicMock()

        with caplog.at_level("INFO"):
            jobs.run_opcom_pzu_daily()  # must not raise

        errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
        assert any("FAILED" in m and "boom" in m for m in errors)

    @patch("shiden.processing.spark.get_spark")
    @patch("shiden.processing.bronze.opcom.OpcomBronzeWriter")
    @patch("shiden.ingestion.opcom.OpcomIngester")
    def test_ingest_failure_does_not_block_bronze_step(
        self, mock_ingester_cls, mock_writer_cls, mock_get_spark
    ):
        mock_ingester_cls.return_value.ingest.side_effect = RuntimeError("boom")
        mock_get_spark.return_value = MagicMock()

        jobs.run_opcom_pzu_daily()

        mock_writer_cls.return_value.process.assert_called_once()

    @patch("shiden.processing.spark.get_spark")
    @patch("shiden.processing.bronze.opcom.OpcomBronzeWriter")
    @patch("shiden.ingestion.opcom.OpcomIngester")
    def test_bronze_failure_is_logged_not_raised(
        self, mock_ingester_cls, mock_writer_cls, mock_get_spark, caplog
    ):
        mock_ingester_cls.return_value.ingest.return_value = MagicMock(
            failed=[], summary=lambda: "ok"
        )
        mock_writer_cls.return_value.process.side_effect = RuntimeError("bronze boom")
        mock_get_spark.return_value = MagicMock()

        with caplog.at_level("INFO"):
            jobs.run_opcom_pzu_daily()  # must not raise

        errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
        assert any("FAILED" in m and "bronze boom" in m for m in errors)

    @patch("shiden.processing.spark.get_spark")
    @patch("shiden.processing.bronze.opcom.OpcomBronzeWriter")
    @patch("shiden.ingestion.opcom.OpcomIngester")
    def test_completes_log_line_present_after_step_failure(
        self, mock_ingester_cls, mock_writer_cls, mock_get_spark, caplog
    ):
        mock_ingester_cls.return_value.ingest.side_effect = RuntimeError("boom")
        mock_get_spark.return_value = MagicMock()

        with caplog.at_level("INFO"):
            jobs.run_opcom_pzu_daily()

        infos = [r.getMessage() for r in caplog.records if r.levelname == "INFO"]
        assert any("Daily OPCOM PZU pipeline complete" in m for m in infos)


class TestMainJobDispatch:
    """``--job <name>`` runs exactly that job and nothing else."""

    _JOB_NAMES = (
        "run_opcom_pzu_daily",
        "run_weather_daily",
        "run_entsoe_daily",
        "run_fx_rates_daily",
        "run_silver_daily",
        "run_gold_daily",
    )

    def _patch_all_jobs(self, monkeypatch):
        calls: list[str] = []
        for name in self._JOB_NAMES:
            monkeypatch.setattr(
                jobs, name, (lambda n: lambda: calls.append(n))(name)
            )
        return calls

    @pytest.mark.parametrize("job_name", _JOB_NAMES)
    def test_job_flag_runs_only_named_job(self, monkeypatch, job_name):
        calls = self._patch_all_jobs(monkeypatch)

        jobs.main(["--job", job_name])

        assert calls == [job_name]

    def test_no_job_flag_runs_full_pipeline_in_order(self, monkeypatch):
        calls = self._patch_all_jobs(monkeypatch)

        jobs.main([])

        assert calls == [
            "run_weather_daily",
            "run_fx_rates_daily",
            "run_entsoe_daily",
            "run_opcom_pzu_daily",
            "run_silver_daily",
            "run_gold_daily",
        ]

    def test_unknown_job_name_rejected(self):
        with pytest.raises(SystemExit):
            jobs.main(["--job", "not_a_real_job"])
