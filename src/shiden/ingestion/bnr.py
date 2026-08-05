"""BNR (National Bank of Romania) exchange rate ingester — landing zone only.

Responsibility: fetch BNR XML rate files, split into per-date XML + JSON sidecar
files, and persist them to the landing zone.  No parsing beyond extraction.
The Bronze layer handles all transformation.

BNR publishes official RON exchange rates on business days.
  Today / recent:  https://www.bnr.ro/nbrfxrates.xml
  Archive by year: https://www.bnr.ro/files/xml/years/nbrfxrates{YYYY}.xml

The archive file contains all business days for a given year as multiple
<Cube date="..."> elements.  We split them into one file per date so the
landing zone stays consistent with all other sources (one file per date).
"""

from __future__ import annotations

import json
import logging
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from shiden.config.settings import settings
from shiden.dates import date_range as _date_range
from shiden.ingestion.http import get_text_with_retry
from shiden.landing_paths import bnr_fx_sidecar_path, bnr_fx_xml_path

logger = logging.getLogger(__name__)

_BNR_DAILY_URL = "https://www.bnr.ro/nbrfxrates.xml"
_BNR_ARCHIVE_URL = "https://www.bnr.ro/files/xml/years/nbrfxrates{year}.xml"

# BNR XML namespace
_BNR_NS = "http://www.bnr.ro/xsd"

# Polite delay between archive-year requests (seconds)
_INTER_REQUEST_SLEEP = 0.5


class BnrIngester:
    """Fetches BNR daily FX rates to the landing zone."""

    def fetch(self, start: date, end: date) -> dict[str, Any]:
        """
        Download BNR XML for all business days in [start, end].

        Fetches one archive file per calendar year needed, then falls back to
        the daily URL for any dates missing from the archive (recent days).

        Returns::

            {
                "source": "BNR",
                "dates": [
                    {
                        "date": "YYYY-MM-DD",
                        "cube_xml": "<Cube ...>...</Cube>",
                        "source_url": "https://...",
                        "fetched_at": "...",
                    },
                    ...
                ]
            }
        """
        years_needed: set[int] = set()
        current = start
        while current <= end:
            years_needed.add(current.year)
            current += timedelta(days=1)

        # One HTTP request per year
        cubes_by_date: dict[str, tuple[str, str]] = {}  # date_str → (cube_xml, url)
        for i, year in enumerate(sorted(years_needed)):
            url = _BNR_ARCHIVE_URL.format(year=year)
            try:
                raw_xml = get_text_with_retry(url, timeout=30, source="BNR")
                year_cubes = _extract_cubes(raw_xml, url)
                cubes_by_date.update(year_cubes)
                logger.info(
                    "BNR fetch: year=%d cubes=%d url=%s", year, len(year_cubes), url
                )
            except Exception as exc:
                logger.warning(
                    "BNR archive fetch failed for year=%d (%s); will try daily URL",
                    year,
                    exc,
                )
            if i < len(years_needed) - 1:
                time.sleep(_INTER_REQUEST_SLEEP)

        # Fall back to daily URL for any dates missing from archives
        missing = [
            d for d in _date_range(start, end) if d.isoformat() not in cubes_by_date
        ]
        if missing:
            try:
                raw_xml = get_text_with_retry(_BNR_DAILY_URL, timeout=30, source="BNR")
                daily_cubes = _extract_cubes(raw_xml, _BNR_DAILY_URL)
                for date_str, cube_data in daily_cubes.items():
                    if date_str not in cubes_by_date:
                        cubes_by_date[date_str] = cube_data
                logger.info("BNR daily fetch: found %d cubes", len(daily_cubes))
            except Exception as exc:
                logger.warning("BNR daily URL fetch failed: %s", exc)

        # Build result list, ordered by date; silently skip weekends/holidays
        fetched_at = datetime.now(timezone.utc).isoformat()
        results = []
        for d in _date_range(start, end):
            date_str = d.isoformat()
            if date_str in cubes_by_date:
                cube_xml, source_url = cubes_by_date[date_str]
                results.append(
                    {
                        "date": date_str,
                        "cube_xml": cube_xml,
                        "source_url": source_url,
                        "fetched_at": fetched_at,
                    }
                )
            else:
                logger.debug(
                    "BNR: no data for %s (weekend or holiday — expected)", date_str
                )

        return {"source": "BNR", "dates": results}

    def ingest(self, start: date, end: date) -> None:
        """
        Fetch BNR rates and write per-date XML + JSON sidecar to landing zone.

        Skips dates that already have a landing file (idempotent).

        Landing paths::

            {landing_base_path}/landing/bnr_fx_rates/{YYYY}/{YYYY-MM-DD}.xml
            {landing_base_path}/landing/bnr_fx_rates/{YYYY}/{YYYY-MM-DD}.json
        """
        # Pre-compute which dates already have a landing file so we can skip them
        # after fetching.  We cannot short-circuit the fetch entirely without
        # knowing which dates are BNR business days (unknown until we parse the
        # XML), so we always fetch and rely on per-entry skip for idempotency.
        already_present = {
            d.isoformat() for d in _date_range(start, end) if _xml_path(d).exists()
        }

        payload = self.fetch(start, end)
        saved = 0

        for entry in payload["dates"]:
            date_str = entry["date"]
            if date_str in already_present:
                logger.debug(
                    "BNR ingest: skipping %s — already in landing zone",
                    date_str,
                )
                continue

            d = date.fromisoformat(date_str)
            xml_p = _xml_path(d)
            sidecar_p = _sidecar_path(d)
            xml_p.parent.mkdir(parents=True, exist_ok=True)

            xml_p.write_text(
                '<?xml version="1.0" encoding="utf-8"?>\n' + entry["cube_xml"],
                encoding="utf-8",
            )
            sidecar_p.write_text(
                json.dumps(
                    {
                        "date": date_str,
                        "source_url": entry["source_url"],
                        "fetched_at": entry["fetched_at"],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            saved += 1
            logger.info("BNR landing: saved %s", xml_p)

        logger.info(
            "BNR ingest complete: start=%s end=%s written=%d skipped=%d",
            start.isoformat(),
            end.isoformat(),
            saved,
            len(already_present),
        )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _xml_path(d: date) -> Path:
    return bnr_fx_xml_path(settings.landing_base_path, d)


def _sidecar_path(d: date) -> Path:
    return bnr_fx_sidecar_path(settings.landing_base_path, d)


def _extract_cubes(raw_xml: str, source_url: str) -> dict[str, tuple[str, str]]:
    """
    Parse a BNR XML document and return date_str → (cube_xml, source_url).

    Handles both daily files (one Cube) and archive files (many Cubes).
    The ``cube_xml`` string is a serialised ``<Cube>`` element suitable for
    writing directly to the landing zone.
    """
    try:
        root = ET.fromstring(raw_xml)
    except ET.ParseError as exc:
        raise ValueError(f"BNR XML parse error: {exc}") from exc

    cubes: dict[str, tuple[str, str]] = {}
    for cube in root.iter(f"{{{_BNR_NS}}}Cube"):
        date_attr = cube.get("date")
        if not date_attr:
            continue
        cube_xml = ET.tostring(cube, encoding="unicode")
        cubes[date_attr] = (cube_xml, source_url)

    return cubes
