"""Silver table: exchange_rates.

Unified, reconciled, gap-filled FX reference table — the single source of
truth for currency conversion in Silver and Gold.  One row per
(date, base_currency, quote_currency); base is always the market's reporting
currency (EUR today).

Sources per market (from the MARKETS config registry)::

    fx_source=BNR        bronze/bnr_fx_rates  (authoritative Romanian rate;
                         ECB used as fallback for dates BNR is missing)
    fx_source=ECB        bronze/ecb_fx_rates  (EUR crosses, e.g. GBP)
    fx_source=SYNTHETIC  materialised rate=1.0 rows for EUR-denominated
                         markets so price tables join uniformly

Gap handling: weekends and holidays are FORWARD_FILLED from the last
published rate.  The fill is seeded with a ``_LOOKBACK_DAYS`` read *before*
the requested start, so a processing window that begins on a weekend still
inherits Friday's rate — a window-local fill previously nulled weekend EUR
prices whenever the weekend fell at the start of the rolling Gold window.

MERGE key: (date, base_currency, quote_currency).
Partition:  (base_currency, quote_currency).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from shiden.config.markets import MARKETS
from shiden.config.settings import settings
from shiden.dates import date_range
from shiden.processing.delta_io import deduplicate_latest, merge_upsert

logger = logging.getLogger(__name__)

_TABLE_PATH = "{base}/silver/exchange_rates"

# How far before `start` to look for a rate to seed the forward-fill.
# BNR/ECB gaps are at most ~5 consecutive days (Easter cluster); 14 is safe.
_LOOKBACK_DAYS = 14

_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=False),
        StructField("base_currency", StringType(), nullable=False),
        StructField("quote_currency", StringType(), nullable=False),
        StructField("rate", DoubleType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("processed_at", TimestampType(), nullable=False),
    ]
)


class FxRow(NamedTuple):
    date: date
    base_currency: str
    quote_currency: str
    rate: float
    source: str  # BNR | ECB | SYNTHETIC | FORWARD_FILLED
    processed_at: datetime  # naive UTC


class ExchangeRatesProcessor:
    """Builds silver/exchange_rates from Bronze FX tables + market config."""

    def __init__(self) -> None:
        self._table_path = _TABLE_PATH.format(base=settings.delta_base_path)
        self._bnr_path = f"{settings.delta_base_path}/bronze/bnr_fx_rates"
        self._ecb_path = f"{settings.delta_base_path}/bronze/ecb_fx_rates"

    def process(self, start: date, end: date, spark: SparkSession) -> None:
        """MERGE reconciled EUR/x rates for every configured market currency.

        Rows are written for [start, end]; Bronze is read from
        start − _LOOKBACK_DAYS so forward-fill is seeded across the window
        edge.
        """
        processed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        fill_start = start - timedelta(days=_LOOKBACK_DAYS)
        rows: list[FxRow] = []

        for quote_currency, fx_source in _required_pairs():
            if fx_source == "SYNTHETIC":
                rows.extend(
                    FxRow(d, "EUR", quote_currency, 1.0, "SYNTHETIC", processed_at)
                    for d in date_range(start, end)
                )
                continue

            primary = self._read_source_rates(
                spark, fx_source, quote_currency, fill_start, end
            )
            # BNR is authoritative for RON but publishes nothing on Romanian
            # holidays where ECB may still publish — use ECB as fallback.
            secondary: dict[date, float] = {}
            secondary_source = ""
            if fx_source == "BNR":
                secondary = self._read_source_rates(
                    spark, "ECB", quote_currency, fill_start, end
                )
                secondary_source = "ECB"

            daily = reconcile_and_fill(
                start,
                end,
                primary,
                fx_source,
                secondary,
                secondary_source,
                fill_start=fill_start,
            )
            rows.extend(
                FxRow(d, "EUR", quote_currency, rate, source, processed_at)
                for d, rate, source in daily
            )
            logger.info(
                "ExchangeRatesProcessor: EUR/%s — %d days resolved (%s primary)",
                quote_currency,
                len(daily),
                fx_source,
            )

        if not rows:
            logger.warning(
                "ExchangeRatesProcessor: no rates resolved for %s–%s", start, end
            )
            return

        incoming_df = spark.createDataFrame(rows, schema=_SCHEMA)
        merge_upsert(
            spark,
            incoming_df,
            self._table_path,
            key_cols=("date", "base_currency", "quote_currency"),
            partition_cols=("base_currency", "quote_currency"),
        )
        logger.info(
            "ExchangeRatesProcessor: merged %d rows into %s",
            len(rows),
            self._table_path,
        )

    # ------------------------------------------------------------------
    # Bronze readers
    # ------------------------------------------------------------------

    def _read_source_rates(
        self,
        spark: SparkSession,
        source: str,
        quote_currency: str,
        start: date,
        end: date,
    ) -> dict[date, float]:
        """Return {date: rate} for one source, latest observation per date.

        Returns {} (with a warning) when the Bronze table does not exist yet.
        """
        try:
            if source == "BNR":
                # BNR quotes RON per 1 unit of foreign currency; the EUR row
                # is therefore RON per 1 EUR — exactly the EUR/RON rate.
                df = (
                    spark.read.format("delta")
                    .load(self._bnr_path)
                    .filter(
                        (F.col("foreign_currency") == "EUR")
                        & (F.col("date") >= start.isoformat())
                        & (F.col("date") <= end.isoformat())
                    )
                )
                df = deduplicate_latest(df, ["date", "foreign_currency"])
                df = df.withColumn(
                    "rate", F.col("rate_ron") / F.col("multiplier")
                ).select("date", "rate")
            elif source == "ECB":
                df = (
                    spark.read.format("delta")
                    .load(self._ecb_path)
                    .filter(
                        (F.col("quote_currency") == quote_currency)
                        & (F.col("date") >= start.isoformat())
                        & (F.col("date") <= end.isoformat())
                    )
                )
                df = deduplicate_latest(df, ["date", "quote_currency"]).select(
                    "date", "rate"
                )
            else:
                raise ValueError(f"Unknown fx_source: {source!r}")

            return {r["date"]: r["rate"] for r in df.collect()}
        except Exception as exc:  # AnalysisException when table absent
            logger.warning(
                "ExchangeRatesProcessor: cannot read %s bronze (%s) — skipping",
                source,
                exc,
            )
            return {}


# ---------------------------------------------------------------------------
# Pure helpers (module-level, unit-testable without Spark)
# ---------------------------------------------------------------------------


def _required_pairs() -> list[tuple[str, str]]:
    """(quote_currency, fx_source) pairs needed by the configured markets.

    A market whose settlement currency equals its reporting currency needs a
    SYNTHETIC rate=1.0 row set regardless of its declared fx_source.
    """
    pairs: list[tuple[str, str]] = []
    for cfg in MARKETS.values():
        if cfg.currency == cfg.reporting_currency:
            pair = (cfg.currency, "SYNTHETIC")
        else:
            pair = (cfg.currency, cfg.fx_source)
        if pair not in pairs:
            pairs.append(pair)
    return pairs


def reconcile_and_fill(
    start: date,
    end: date,
    primary: dict[date, float],
    primary_source: str,
    secondary: dict[date, float] | None = None,
    secondary_source: str = "",
    *,
    fill_start: date | None = None,
) -> list[tuple[date, float, str]]:
    """Merge two rate sources and forward-fill gaps.

    Per date: primary wins; else secondary; else the last published rate is
    carried forward as FORWARD_FILLED.  The walk starts at ``fill_start``
    (default: ``start``) so the fill can be seeded by observations before the
    emitted window; only dates in [start, end] are returned.  Dates before
    the first published rate are omitted (no back-fill).
    """
    secondary = secondary or {}
    walk_from = fill_start if fill_start is not None else start

    out: list[tuple[date, float, str]] = []
    last_rate: float | None = None
    for d in date_range(walk_from, end):
        if d in primary:
            rate, source = primary[d], primary_source
            last_rate = rate
        elif d in secondary:
            rate, source = secondary[d], secondary_source
            last_rate = rate
        elif last_rate is not None:
            rate, source = last_rate, "FORWARD_FILLED"
        else:
            continue  # before first observation — nothing to fill from
        if d >= start:
            out.append((d, rate, source))
    return out
