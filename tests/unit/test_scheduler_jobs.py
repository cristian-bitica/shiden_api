"""Unit tests for scheduler/jobs.py: daily job step error isolation and the
``--job`` CLI dispatch affordance.

Mirrors ``tests/acceptance/features/daily_job_step_isolation.feature``:
a failing pipeline step inside a daily job must be logged as an ERROR and
must not raise out of the job.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from functools import partial
from unittest.mock import MagicMock, patch

import pytest

from shiden.scheduler import jobs


class TestRunSafe:
    """_run_safe is the shared error-isolation wrapper every daily job uses."""

    def test_success_runs_fn_and_logs_nothing(self, caplog):
        calls = []

        with caplog.at_level("INFO"):
            jobs._run_safe("Step", lambda: calls.append(1))

        assert calls == [1]
        assert caplog.records == []

    def test_failure_logs_exact_message_with_traceback_and_does_not_raise(
        self, caplog
    ):
        def _boom():
            raise RuntimeError("boom")

        with caplog.at_level("ERROR"):
            jobs._run_safe("MyStep", _boom)  # must not raise

        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelname == "ERROR"
        assert record.getMessage() == "Pipeline step MyStep FAILED: boom"
        assert record.exc_info  # truthy tuple; None/False both mean "no traceback"


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
        assert errors == ["Pipeline step OpcomIngest FAILED: boom"]

    @patch("shiden.processing.spark.get_spark")
    @patch("shiden.processing.bronze.opcom.OpcomBronzeWriter")
    @patch("shiden.ingestion.opcom.OpcomIngester")
    def test_ingest_and_bronze_called_with_expected_window_and_spark(
        self, mock_ingester_cls, mock_writer_cls, mock_get_spark
    ):
        mock_ingester_cls.return_value.ingest.return_value = MagicMock(
            failed=[], summary=lambda: "ok"
        )
        mock_get_spark.return_value = MagicMock()

        jobs.run_opcom_pzu_daily()

        today = date.today()
        start = today - timedelta(days=jobs.OPCOM_LOOKBACK_DAYS)
        mock_ingester_cls.return_value.ingest.assert_called_once_with(
            "RO", start, today, skip_existing=True
        )
        mock_writer_cls.return_value.process.assert_called_once_with(
            "RO", start, today, mock_get_spark.return_value
        )

    @patch("shiden.processing.spark.get_spark")
    @patch("shiden.processing.bronze.opcom.OpcomBronzeWriter")
    @patch("shiden.ingestion.opcom.OpcomIngester")
    def test_startup_and_completion_log_messages_exact(
        self, mock_ingester_cls, mock_writer_cls, mock_get_spark, caplog
    ):
        mock_ingester_cls.return_value.ingest.return_value = MagicMock(
            failed=[], summary=lambda: "ok"
        )
        mock_get_spark.return_value = MagicMock()
        today = date.today()
        start = today - timedelta(days=jobs.OPCOM_LOOKBACK_DAYS)

        with caplog.at_level("INFO"):
            jobs.run_opcom_pzu_daily()

        infos = [r.getMessage() for r in caplog.records if r.levelname == "INFO"]
        assert infos == [
            f"Starting daily OPCOM PZU ingest for {start}–{today}",
            "OPCOM ingest: ok",
            f"Daily OPCOM PZU pipeline complete for {start}–{today}",
        ]

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
    def test_partial_ingest_failure_logs_gap_warning_per_date(
        self, mock_ingester_cls, mock_writer_cls, mock_get_spark, caplog
    ):
        gap_date = date(2026, 1, 5)
        mock_ingester_cls.return_value.ingest.return_value = MagicMock(
            failed=[(gap_date, "upstream 500")], summary=lambda: "failed=1"
        )
        mock_get_spark.return_value = MagicMock()

        with caplog.at_level("INFO"):
            jobs.run_opcom_pzu_daily()

        infos = [r.getMessage() for r in caplog.records if r.levelname == "INFO"]
        assert "OPCOM ingest: failed=1" in infos

        warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
        assert warnings == [f"OPCOM gap remains for {gap_date} — upstream 500"]

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
        assert errors == ["Pipeline step OpcomBronze FAILED: bronze boom"]

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
            monkeypatch.setattr(jobs, name, partial(calls.append, name))
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

    def test_configures_logging_with_info_level_and_timestamped_format(
        self, monkeypatch
    ):
        calls = []
        monkeypatch.setattr(logging, "basicConfig", lambda **kw: calls.append(kw))
        self._patch_all_jobs(monkeypatch)

        jobs.main([])

        assert calls == [
            {
                "level": logging.INFO,
                "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
            }
        ]

    def test_parser_description_and_job_help_text_exact(self, monkeypatch):
        captured = {}
        real_init = argparse.ArgumentParser.__init__
        real_add_argument = argparse.ArgumentParser.add_argument

        def fake_init(self, *args, **kwargs):
            captured["description"] = kwargs.get("description")
            return real_init(self, *args, **kwargs)

        def fake_add_argument(self, *args, **kwargs):
            if args and args[0] == "--job":
                captured["help"] = kwargs.get("help")
            return real_add_argument(self, *args, **kwargs)

        monkeypatch.setattr(argparse.ArgumentParser, "__init__", fake_init)
        monkeypatch.setattr(argparse.ArgumentParser, "add_argument", fake_add_argument)
        self._patch_all_jobs(monkeypatch)

        jobs.main([])

        assert captured["description"] == "Run daily pipeline jobs."
        assert captured["help"] == "Run exactly this named daily job once, then exit."
