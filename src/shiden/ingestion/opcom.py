"""OPCOM Day-Ahead Market (PZU) ingester — landing zone only.

Responsibility: pull raw CSV files from OPCOM and persist them as-is to
the landing zone.  No parsing, no transformation.
The Bronze layer picks up files from the landing zone separately.

OPCOM is the Romanian market operator — this source only exists for RO.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from shiden.config.settings import settings
from shiden.dates import date_range
from shiden.ingestion.http import get_text_with_retry
from shiden.landing_paths import opcom_pzu_csv_path

logger = logging.getLogger(__name__)

# CSV export URL: date is embedded as DD/MM/YYYY in path
_CSV_URL_TMPL = (
    "https://www.opcom.ro/rapoarte-pzu-raportPIP-export-csv"
    "/{day:02d}/{month:02d}/{year}/ro?resolution=15"
)


class DataNotAvailableError(Exception):
    """Raised when OPCOM has not yet published data for the requested date."""


@dataclass
class IngestReport:
    """Outcome of a multi-day ingest.

    A backfill spans hundreds of days across a public website; some of them
    will fail for reasons that say nothing about the rest (a gap in OPCOM's
    archive, a transient 5xx). Aborting the run on the first one wastes every
    day already fetched, so failures are collected and surfaced here instead.
    """

    fetched: list[date] = field(default_factory=list)
    skipped: list[date] = field(default_factory=list)
    failed: list[tuple[date, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed

    def summary(self) -> str:
        parts = [
            f"fetched={len(self.fetched)}",
            f"skipped={len(self.skipped)}",
            f"failed={len(self.failed)}",
        ]
        return " ".join(parts)


class OpcomIngester:
    """Ingests PZU (Day-Ahead) price and volume data from OPCOM."""

    def fetch(self, market_id: str, start: date, end: date) -> dict[str, Any]:
        """
        Download raw PZU CSV text for each date in [start, end] (inclusive).

        Returns a dict with:
            market_id: str
            delivery_dates: list of dicts, each containing:
                date:       str  — ISO delivery date
                source_url: str  — URL the CSV was fetched from
                raw_csv:    str  — unmodified CSV text from OPCOM

        Raises DataNotAvailableError on the first date with an empty
        response (data not yet published).
        """
        results: list[dict[str, Any]] = []
        for current in date_range(start, end):
            if results:
                time.sleep(settings.opcom_rate_limit_sleep)
            url, raw_csv = self._fetch_day(current)
            results.append(
                {
                    "date": current.isoformat(),
                    "source_url": url,
                    "raw_csv": raw_csv,
                }
            )
        return {"market_id": market_id, "delivery_dates": results}

    def ingest(
        self,
        market_id: str,
        start: date,
        end: date,
        skip_existing: bool = False,
        stop_on_error: bool = False,
    ) -> IngestReport:
        """
        Fetch raw CSVs and save them as-is to the landing zone.

        Fetch-and-save runs per day, so a failure (or not-yet-published
        data) partway through a range keeps every previously fetched day
        on disk instead of discarding the whole batch.

        Parameters
        ----------
        skip_existing:
            Skip dates already present in the landing zone. Makes a long
            backfill resumable -- rerun the same range and it picks up where
            it stopped instead of refetching months of files.
        stop_on_error:
            Abort on the first failure instead of collecting it. Off by
            default so backfills survive isolated gaps; the daily job turns
            it on, where a failure means today's auction genuinely is missing
            and should be noticed.

        Landing path:
            {landing_base_path}/landing/opcom_pzu/{market_id}/{YYYY-MM-DD}.csv
        """
        report = IngestReport()

        for current in date_range(start, end):
            landing_path = opcom_pzu_csv_path(
                settings.landing_base_path, market_id, current
            )

            if skip_existing and landing_path.exists():
                report.skipped.append(current)
                continue

            if report.fetched:
                time.sleep(settings.opcom_rate_limit_sleep)

            try:
                _, raw_csv = self._fetch_day(current)
            except Exception as exc:  # noqa: BLE001 - recorded, then continue
                reason = f"{type(exc).__name__}: {exc}"
                logger.warning("OPCOM ingest: %s failed — %s", current, reason)
                report.failed.append((current, reason))
                if stop_on_error:
                    raise
                continue

            landing_path.parent.mkdir(parents=True, exist_ok=True)
            landing_path.write_text(raw_csv, encoding="utf-8")
            report.fetched.append(current)
            logger.info("OPCOM landing: saved %s", landing_path)

        logger.info(
            "OPCOM ingest complete: market=%s start=%s end=%s %s",
            market_id,
            start.isoformat(),
            end.isoformat(),
            report.summary(),
        )
        return report

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_day(self, delivery_date: date) -> tuple[str, str]:
        """Fetch one day's CSV; returns (url, csv_text).

        Raises DataNotAvailableError when OPCOM returns an empty body —
        the data for that delivery date has not been published yet.
        """
        url = _CSV_URL_TMPL.format(
            day=delivery_date.day,
            month=delivery_date.month,
            year=delivery_date.year,
        )
        t0 = time.monotonic()
        raw_csv = get_text_with_retry(url, timeout=30, source="OPCOM")
        elapsed = time.monotonic() - t0

        if not raw_csv.strip():
            raise DataNotAvailableError(
                f"Empty response for {delivery_date.isoformat()}. "
                "Data may not yet be published."
            )

        logger.info(
            "OPCOM fetch: date=%s bytes=%d duration=%.2fs url=%s",
            delivery_date.isoformat(),
            len(raw_csv),
            elapsed,
            url,
        )
        return url, raw_csv
