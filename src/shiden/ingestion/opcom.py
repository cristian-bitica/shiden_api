"""OPCOM Day-Ahead Market (PZU) ingester — landing zone only.

Responsibility: pull raw CSV files from OPCOM and persist them as-is to
the landing zone.  No parsing, no transformation.
The Bronze layer picks up files from the landing zone separately.

OPCOM is the Romanian market operator — this source only exists for RO.
"""

from __future__ import annotations

import logging
import time
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

    def ingest(self, market_id: str, start: date, end: date) -> None:
        """
        Fetch raw CSVs and save them as-is to the landing zone.

        Fetch-and-save runs per day, so a failure (or not-yet-published
        data) partway through a range keeps every previously fetched day
        on disk instead of discarding the whole batch.

        Landing path:
            {landing_base_path}/landing/opcom_pzu/{market_id}/{YYYY-MM-DD}.csv
        """
        saved = 0
        for current in date_range(start, end):
            if saved:
                time.sleep(settings.opcom_rate_limit_sleep)
            _, raw_csv = self._fetch_day(current)

            landing_path = opcom_pzu_csv_path(
                settings.landing_base_path, market_id, current
            )
            landing_path.parent.mkdir(parents=True, exist_ok=True)
            landing_path.write_text(raw_csv, encoding="utf-8")
            saved += 1
            logger.info("OPCOM landing: saved %s", landing_path)

        logger.info(
            "OPCOM ingest complete: market=%s start=%s end=%s files=%d",
            market_id,
            start.isoformat(),
            end.isoformat(),
            saved,
        )

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
