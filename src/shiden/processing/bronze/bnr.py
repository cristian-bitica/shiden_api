"""Bronze processor for BNR (National Bank of Romania) FX rate data.

Responsibility: read per-date XML + JSON sidecar files from the landing zone,
parse the <Rate> elements, and append to the bronze/bnr_fx_rates Delta table.

Bronze is pure append-only with revision capture: re-ingesting unchanged
rates writes nothing (growth guard); a revised rate appends a new row.
Silver reads the latest row per (date, foreign_currency) by ingested_at.
No business logic; Silver handles multiplier normalisation and EUR/RON
reconciliation.
"""

from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from shiden.config.settings import settings
from shiden.landing_paths import bnr_fx_sidecar_path, bnr_fx_xml_path
from shiden.processing.delta_io import append_new_observations

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/bronze/bnr_fx_rates"

_BNR_NS = "http://www.bnr.ro/xsd"

_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=False),
        StructField("foreign_currency", StringType(), nullable=False),
        StructField("rate_ron", DoubleType(), nullable=False),
        StructField("multiplier", IntegerType(), nullable=False),
        StructField("source_url", StringType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
    ]
)


class BnrRow(NamedTuple):
    """One parsed rate row for the Bronze table."""

    date: date
    foreign_currency: str
    rate_ron: float
    multiplier: int
    source_url: str
    ingested_at: datetime  # naive UTC


class BnrBronzeWriter:
    """Parses BNR landing files, appends new observations to Bronze."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)

    def process(self, start: date, end: date, spark: SparkSession) -> None:
        """
        Parse landing XML files for [start, end] and append to Bronze.

        Reads:  {landing_base_path}/landing/bnr_fx_rates/{YYYY}/{YYYY-MM-DD}.xml
                {landing_base_path}/landing/bnr_fx_rates/{YYYY}/{YYYY-MM-DD}.json
        Writes: {delta_base_path}/bronze/bnr_fx_rates
                (Delta, partitioned by foreign_currency)

        Append-only with revision capture on (date, foreign_currency) keyed
        values (rate_ron, multiplier) — safe to re-run.
        BNR only publishes on business days; missing landing files are skipped.
        """
        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        rows: list[BnrRow] = []

        current = start
        while current <= end:
            xml_p = _xml_path(current)
            sidecar_p = _sidecar_path(current)

            if not xml_p.exists():
                logger.debug(
                    "BnrBronzeWriter: no landing file for %s"
                    " (weekend/holiday?) — skipping",
                    current.isoformat(),
                )
                current += timedelta(days=1)
                continue

            source_url = _read_source_url(sidecar_p, current)
            day_rows = _parse_landing_xml(xml_p, current, source_url, ingested_at)
            rows.extend(day_rows)
            logger.info(
                "BnrBronzeWriter: parsed %d rates for %s",
                len(day_rows),
                current.isoformat(),
            )
            current += timedelta(days=1)

        if not rows:
            logger.info("BnrBronzeWriter: nothing to write for %s–%s", start, end)
            return

        incoming_df = spark.createDataFrame(rows, schema=_SCHEMA)
        append_new_observations(
            spark,
            incoming_df,
            self._table_path,
            key_cols=("date", "foreign_currency"),
            value_cols=("rate_ron", "multiplier"),
            partition_cols=("foreign_currency",),
        )
        logger.info(
            "BnrBronzeWriter: appended new observations from %d parsed rows to %s",
            len(rows),
            self._table_path,
        )


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_landing_xml(
    xml_path: Path,
    target_date: date,
    source_url: str,
    ingested_at: datetime,
) -> list[BnrRow]:
    """
    Parse a per-date BNR landing XML and return one BnrRow per currency.

    The XML written to landing is::

        <?xml version="1.0" encoding="utf-8"?>
        <Cube xmlns="http://www.bnr.ro/xsd" date="YYYY-MM-DD">
          <Rate currency="EUR">4.9742</Rate>
          <Rate currency="USD">4.5123</Rate>
          <Rate currency="JPY" multiplier="100">3.4567</Rate>
        </Cube>

    Non-numeric rate text (e.g. "-") is skipped with a warning.
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except (ET.ParseError, OSError) as exc:
        logger.warning("BnrBronzeWriter: cannot read/parse %s: %s", xml_path, exc)
        return []

    rows: list[BnrRow] = []

    # Root may be a namespaced <Cube> or contain one
    cube = (
        root
        if root.tag in (f"{{{_BNR_NS}}}Cube", "Cube")
        else root.find(f"{{{_BNR_NS}}}Cube")
    )

    if cube is None:
        logger.warning("BnrBronzeWriter: no Cube element found in %s", xml_path)
        return []

    # Use the date from the XML attribute when present; fall back to target_date
    date_attr = cube.get("date")
    effective_date = date.fromisoformat(date_attr) if date_attr else target_date

    for rate_elem in cube:
        currency = rate_elem.get("currency")
        if not currency:
            continue
        try:
            text = rate_elem.text
            if text is None:
                continue
            rate_ron = float(text.strip())
        except (ValueError, AttributeError):
            logger.warning(
                "BnrBronzeWriter: skipping non-numeric rate for %s on %s: %r",
                currency,
                effective_date,
                rate_elem.text,
            )
            continue
        multiplier = int(rate_elem.get("multiplier", "1"))
        rows.append(
            BnrRow(
                date=effective_date,
                foreign_currency=currency,
                rate_ron=rate_ron,
                multiplier=multiplier,
                source_url=source_url,
                ingested_at=ingested_at,
            )
        )

    return rows


def _read_source_url(sidecar_path: Path, fallback_date: date) -> str:
    """Return source_url from the JSON sidecar, or a best-effort fallback."""
    if sidecar_path.exists():
        try:
            data = json.loads(sidecar_path.read_text(encoding="utf-8"))
            return str(data.get("source_url", _fallback_url(fallback_date)))
        except (json.JSONDecodeError, OSError):
            pass
    return _fallback_url(fallback_date)


def _fallback_url(d: date) -> str:
    # Mirror of ingestion.bnr._BNR_ARCHIVE_URL — kept here to avoid cross-layer import
    return f"https://www.bnr.ro/files/xml/years/nbrfxrates{d.year}.xml"


# ---------------------------------------------------------------------------
# Path helpers (mirror ingester paths)
# ---------------------------------------------------------------------------


def _xml_path(d: date) -> Path:
    return bnr_fx_xml_path(settings.landing_base_path, d)


def _sidecar_path(d: date) -> Path:
    return bnr_fx_sidecar_path(settings.landing_base_path, d)
