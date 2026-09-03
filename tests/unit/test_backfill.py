"""Backfill CLI and resilient-ingest tests.

No network: the HTTP seam (``get_text_with_retry``) is mocked throughout.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from shiden.backfill import _month_chunks, build_parser, main
from shiden.ingestion.opcom import DataNotAvailableError, IngestReport, OpcomIngester
from shiden.processing.bronze.opcom import MTU_TRANSITION_DATE


def _valid_csv(n: int = 96) -> str:
    header = (
        '"Zona de tranzactionare","Interval","Pret","Volum","Cumparare","Vanzare"'
    )
    rows = [f'"Romania","{i}","600.00","1000.0","500.0","1000.0"' for i in range(1, n + 1)]
    return "\n".join(['"titlu"', "", header, *rows])


@pytest.fixture
def landing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    from shiden.config.settings import settings

    monkeypatch.setattr(settings, "landing_base_path", str(tmp_path))
    monkeypatch.setattr(settings, "opcom_rate_limit_sleep", 0.0)
    return tmp_path


class TestMonthChunks:
    def test_single_day(self) -> None:
        assert _month_chunks(date(2026, 3, 5), date(2026, 3, 5)) == [
            (date(2026, 3, 5), date(2026, 3, 5))
        ]

    def test_within_one_month(self) -> None:
        assert _month_chunks(date(2026, 3, 5), date(2026, 3, 20)) == [
            (date(2026, 3, 5), date(2026, 3, 20))
        ]

    def test_splits_on_month_boundaries(self) -> None:
        chunks = _month_chunks(date(2026, 1, 15), date(2026, 4, 3))
        assert chunks == [
            (date(2026, 1, 15), date(2026, 1, 31)),
            (date(2026, 2, 1), date(2026, 2, 28)),
            (date(2026, 3, 1), date(2026, 3, 31)),
            (date(2026, 4, 1), date(2026, 4, 3)),
        ]

    def test_crosses_year_boundary(self) -> None:
        chunks = _month_chunks(date(2025, 12, 20), date(2026, 1, 5))
        assert chunks == [
            (date(2025, 12, 20), date(2025, 12, 31)),
            (date(2026, 1, 1), date(2026, 1, 5)),
        ]

    def test_chunks_are_contiguous_and_complete(self) -> None:
        start, end = date(2025, 10, 1), date(2026, 8, 10)
        chunks = _month_chunks(start, end)
        assert chunks[0][0] == start
        assert chunks[-1][1] == end
        total = sum((c[1] - c[0]).days + 1 for c in chunks)
        assert total == (end - start).days + 1


class TestResilientIngest:
    def test_collects_failures_instead_of_aborting(self, landing: Path) -> None:
        """One missing day must not kill a 300-day backfill."""

        def flaky(url: str, **kwargs: object) -> str:
            return "" if "02/06/2026" in url else _valid_csv()

        with patch("shiden.ingestion.opcom.get_text_with_retry", flaky):
            report = OpcomIngester().ingest(
                "RO", date(2026, 6, 1), date(2026, 6, 3)
            )

        assert len(report.fetched) == 2
        assert len(report.failed) == 1
        assert report.failed[0][0] == date(2026, 6, 2)
        assert not report.ok

    def test_stop_on_error_still_available(self, landing: Path) -> None:
        with patch("shiden.ingestion.opcom.get_text_with_retry", MagicMock(return_value="")):
            with pytest.raises(DataNotAvailableError):
                OpcomIngester().ingest(
                    "RO", date(2026, 6, 1), date(2026, 6, 3), stop_on_error=True
                )

    def test_skip_existing_makes_runs_resumable(self, landing: Path) -> None:
        fetch = MagicMock(return_value=_valid_csv())
        with patch("shiden.ingestion.opcom.get_text_with_retry", fetch):
            first = OpcomIngester().ingest(
                "RO", date(2026, 6, 1), date(2026, 6, 3), skip_existing=True
            )
            second = OpcomIngester().ingest(
                "RO", date(2026, 6, 1), date(2026, 6, 3), skip_existing=True
            )

        assert len(first.fetched) == 3
        assert len(second.fetched) == 0
        assert len(second.skipped) == 3
        assert fetch.call_count == 3, "resumed run refetched existing days"

    def test_refetch_overwrites(self, landing: Path) -> None:
        fetch = MagicMock(return_value=_valid_csv())
        with patch("shiden.ingestion.opcom.get_text_with_retry", fetch):
            OpcomIngester().ingest("RO", date(2026, 6, 1), date(2026, 6, 1))
            OpcomIngester().ingest("RO", date(2026, 6, 1), date(2026, 6, 1))
        assert fetch.call_count == 2

    def test_files_land_on_disk(self, landing: Path) -> None:
        with patch(
            "shiden.ingestion.opcom.get_text_with_retry",
            MagicMock(return_value=_valid_csv()),
        ):
            OpcomIngester().ingest("RO", date(2026, 6, 1), date(2026, 6, 2))
        written = list(landing.rglob("*.csv"))
        assert len(written) == 2

    def test_report_summary_is_readable(self) -> None:
        report = IngestReport(
            fetched=[date(2026, 6, 1)],
            skipped=[date(2026, 6, 2)],
            failed=[(date(2026, 6, 3), "boom")],
        )
        assert report.summary() == "fetched=1 skipped=1 failed=1"


class TestBackfillCli:
    def test_defaults_to_the_mtu_transition(self) -> None:
        args = build_parser().parse_args([])
        assert args.start == MTU_TRANSITION_DATE

    def test_rejects_pre_transition_start(self, capsys: pytest.CaptureFixture) -> None:
        code = main(["--from", "2025-06-01", "--to", "2025-07-01", "--dry-run"])
        assert code == 2
        assert "15-minute MTU" in capsys.readouterr().err

    def test_allow_pre_mtu_escape_hatch(self, capsys: pytest.CaptureFixture) -> None:
        code = main(
            ["--from", "2025-06-01", "--to", "2025-07-01", "--allow-pre-mtu", "--dry-run"]
        )
        assert code == 0

    def test_rejects_inverted_range(self, capsys: pytest.CaptureFixture) -> None:
        code = main(["--from", "2026-08-01", "--to", "2026-07-01", "--dry-run"])
        assert code == 2
        assert "--to must be >=" in capsys.readouterr().err

    def test_rejects_unknown_market(self, capsys: pytest.CaptureFixture) -> None:
        code = main(["--market", "ZZ", "--dry-run"])
        assert code == 2
        assert "unknown market" in capsys.readouterr().err

    def test_dry_run_touches_nothing(self, capsys: pytest.CaptureFixture) -> None:
        code = main(["--from", "2025-10-01", "--to", "2025-10-05", "--dry-run"])
        out = capsys.readouterr().out
        assert code == 0
        assert "5 days" in out
        assert "Dry run" in out

    def test_rejects_unknown_stage(self) -> None:
        with pytest.raises(SystemExit):
            build_parser().parse_args(["--stages", "bronze,platinum"])

    def test_rejects_unknown_source(self) -> None:
        with pytest.raises(SystemExit):
            build_parser().parse_args(["--sources", "opcom,nasdaq"])

    def test_stages_parse_to_a_set(self) -> None:
        args = build_parser().parse_args(["--stages", "silver,gold"])
        assert args.stages == {"silver", "gold"}
