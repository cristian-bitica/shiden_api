"""ECB (European Central Bank) exchange rate ingester — landing zone only.

Responsibility: download the ECB historical EUR reference rates XML (a single
~5 MB file containing all business days), extract per-date JSON slices, and
write them to the landing zone.

URL (full history, one file):
    https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml

ECB publishes on business days only; weekends and holidays are absent — Silver
forward-fills them.  The base currency is always EUR; rates express how many
units of the quote currency equal 1 EUR.

Landing zone layout::

    {landing_base_path}/landing/ecb_fx_rates/{YYYY-MM-DD}.json
"""

from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from shiden.config.settings import settings
from shiden.dates import date_range as _date_range
from shiden.ingestion.http import get_text_with_retry
from shiden.landing_paths import ecb_fx_json_path

logger = logging.getLogger(__name__)

_ECB_HIST_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"

# ECB XML namespaces
_ECB_NS = "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"
_GESMES_NS = "http://www.gesmes.org/xml/2002-08-01"


class EcbIngester:
    """Downloads ECB historical FX rates and writes per-date JSON to landing zone."""

    def fetch(self, start: date, end: date) -> dict[str, Any]:
        """
        Download the ECB full-history XML and extract rates for [start, end].

        A single HTTP request retrieves all available history; we then filter
        to the requested range.

        Returns::

            {
                "source": "ECB",
                "dates": [
                    {
                        "date": "YYYY-MM-DD",
                        "rates": {"USD": 1.0921, "GBP": 0.8567, ...},
                        "source_url": "https://...",
                        "fetched_at": "...",
                    },
                    ...
                ]
            }
        """
        logger.info("ECB fetch: downloading full history from %s", _ECB_HIST_URL)
        raw_xml = get_text_with_retry(_ECB_HIST_URL, timeout=60, source="ECB")
        fetched_at = datetime.now(timezone.utc).isoformat()

        all_cubes = _parse_ecb_xml(raw_xml)
        logger.info("ECB fetch: parsed %d date entries from XML", len(all_cubes))

        results = []
        for d in _date_range(start, end):
            date_str = d.isoformat()
            if date_str in all_cubes:
                results.append(
                    {
                        "date": date_str,
                        "rates": all_cubes[date_str],
                        "source_url": _ECB_HIST_URL,
                        "fetched_at": fetched_at,
                    }
                )
            else:
                logger.debug(
                    "ECB: no data for %s (weekend or holiday — expected)", date_str
                )

        logger.info(
            "ECB fetch: %d dates in range %s–%s, %d with data",
            (end - start).days + 1,
            start.isoformat(),
            end.isoformat(),
            len(results),
        )
        return {"source": "ECB", "dates": results}

    def ingest(self, start: date, end: date) -> None:
        """
        Fetch ECB rates and write per-date JSON files to landing zone.

        Skips dates that already have a landing file (idempotent).
        """
        # Pre-compute which dates already have a landing file.
        # We cannot skip the ECB download without knowing which dates are
        # business days (only the XML tells us), so we always fetch and rely
        # on per-entry skip for idempotency.
        already_present = {
            d.isoformat() for d in _date_range(start, end) if _landing_path(d).exists()
        }

        payload = self.fetch(start, end)
        saved = 0

        for entry in payload["dates"]:
            date_str = entry["date"]
            if date_str in already_present:
                logger.debug("ECB ingest: skipping %s — already present", date_str)
                continue

            p = _landing_path(date.fromisoformat(date_str))
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(
                json.dumps(entry, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            saved += 1
            logger.info("ECB landing: saved %s", p)

        logger.info(
            "ECB ingest complete: start=%s end=%s written=%d skipped=%d",
            start.isoformat(),
            end.isoformat(),
            saved,
            len(already_present),
        )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _landing_path(d: date) -> Path:
    return ecb_fx_json_path(settings.landing_base_path, d)


def _parse_ecb_xml(raw_xml: str) -> dict[str, dict[str, float]]:
    """
    Parse ECB eurofxref-hist.xml and return date_str → {currency: rate}.

    The ECB XML structure is::

        <gesmes:Envelope>
          <Cube>
            <Cube time="2024-01-15">
              <Cube currency="USD" rate="1.0921"/>
              <Cube currency="GBP" rate="0.8567"/>
              ...
            </Cube>
            ...
          </Cube>
        </gesmes:Envelope>

    Base currency is always EUR; ``rate`` = how many quote units per 1 EUR.
    Non-numeric rates are skipped with a warning.
    """
    try:
        root = ET.fromstring(raw_xml)
    except ET.ParseError as exc:
        raise ValueError(f"ECB XML parse error: {exc}") from exc

    result: dict[str, dict[str, float]] = {}

    # The outer <Cube> has no attributes; date-level <Cube> have ``time``;
    # rate-level <Cube> have ``currency`` and ``rate``.
    for day_cube in root.iter(f"{{{_ECB_NS}}}Cube"):
        time_attr = day_cube.get("time")
        if not time_attr:
            continue
        rates: dict[str, float] = {}
        for rate_cube in day_cube:
            currency = rate_cube.get("currency")
            rate_str = rate_cube.get("rate")
            if not currency or not rate_str:
                continue
            try:
                rates[currency] = float(rate_str)
            except ValueError:
                logger.warning(
                    "ECB: skipping non-numeric rate for %s on %s: %r",
                    currency,
                    time_attr,
                    rate_str,
                )
        if rates:
            result[time_attr] = rates

    return result
