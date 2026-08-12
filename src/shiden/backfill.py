"""Historical backfill across the medallion layers.

    python -m shiden.backfill --from 2025-10-01 --to 2026-08-10
    python -m shiden.backfill --from 2026-07-01 --to 2026-08-10 --stages silver,gold
    python -m shiden.backfill --from 2025-10-01 --to 2026-08-10 --dry-run

Why this exists
---------------
Until now the only way to load data was the scheduler, and every job fetched
a fixed window relative to ``today``. That makes history unreachable: a day
the scheduler did not run is a day that stays missing forever. Price coverage
degraded to four isolated days that way.

Design notes
------------
*Resumable by default.* Landing files already on disk are skipped, so a run
interrupted after 200 of 315 days picks up at 201 rather than refetching.
Pass ``--refetch`` to force.

*Staged.* ``--stages`` lets you re-run Silver and Gold without touching the
network, which is what you want after changing a transform.

*Chunked.* Silver and Gold run a month at a time. One Spark job over 300+
days would work but gives no progress signal and no partial credit if it
fails at day 290.

*Guarded at the source.* Romania moved to 15-minute MTUs on 2025-10-01;
earlier OPCOM exports are hourly and would corrupt the price series. The
default start date is the transition, and earlier dates are refused here
rather than being silently mangled downstream.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta

from shiden.config.markets import MARKETS
from shiden.processing.bronze.opcom import MTU_TRANSITION_DATE

logger = logging.getLogger("shiden.backfill")

ALL_STAGES = ("landing", "bronze", "silver", "gold")
ALL_SOURCES = ("opcom", "fx")


def _month_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into calendar-month slices."""
    chunks: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        if cursor.month == 12:
            next_month = date(cursor.year + 1, 1, 1)
        else:
            next_month = date(cursor.year, cursor.month + 1, 1)
        chunk_end = min(next_month - timedelta(days=1), end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------


def stage_landing(
    market_id: str, start: date, end: date, sources: set[str], refetch: bool
) -> int:
    """Fetch raw files into the landing zone. Returns the failure count."""
    failures = 0

    if "opcom" in sources:
        from shiden.ingestion.opcom import OpcomIngester

        logger.info("Landing: OPCOM %s → %s", start, end)
        report = OpcomIngester().ingest(
            market_id, start, end, skip_existing=not refetch
        )
        logger.info("Landing: OPCOM %s", report.summary())
        for failed_date, reason in report.failed:
            logger.warning("  OPCOM gap %s — %s", failed_date, reason)
        failures += len(report.failed)

    if "fx" in sources:
        from shiden.ingestion.bnr import BnrIngester
        from shiden.ingestion.ecb import EcbIngester

        # FX is not optional: fact_price joins silver/exchange_rates to
        # produce price_eur_mwh, so a price backfill without a matching FX
        # backfill yields a EUR column that is null for the whole period.
        for name, ingester in (("BNR", BnrIngester()), ("ECB", EcbIngester())):
            logger.info("Landing: %s %s → %s", name, start, end)
            try:
                ingester.ingest(start, end)
            except Exception as exc:  # noqa: BLE001 - reported, run continues
                logger.error("Landing: %s failed — %s", name, exc)
                failures += 1

    return failures


def stage_bronze(market_id: str, start: date, end: date, sources: set[str]) -> None:
    from shiden.processing.spark import get_spark

    spark = get_spark()

    if "opcom" in sources:
        from shiden.processing.bronze.opcom import OpcomBronzeWriter

        logger.info("Bronze: OPCOM %s → %s", start, end)
        OpcomBronzeWriter().process(market_id, start, end, spark)

    if "fx" in sources:
        from shiden.processing.bronze.bnr import BnrBronzeWriter
        from shiden.processing.bronze.ecb import EcbBronzeWriter

        logger.info("Bronze: FX %s → %s", start, end)
        BnrBronzeWriter().process(start, end, spark)
        EcbBronzeWriter().process(start, end, spark)


def stage_silver(market_id: str, start: date, end: date) -> None:
    from shiden.processing.silver.dimensions.dim_date import DimDateProcessor
    from shiden.processing.silver.dimensions.dim_datetime import (
        DimDateTimeProcessor,
    )
    from shiden.processing.silver.dimensions.dim_location import DimLocationProcessor
    from shiden.processing.silver.dimensions.dim_market import DimMarketProcessor
    from shiden.processing.silver.dimensions.dim_production_type import (
        DimProductionTypeProcessor,
    )
    from shiden.processing.silver.exchange_rates import ExchangeRatesProcessor
    from shiden.processing.silver.fact_generation import FactGenerationProcessor
    from shiden.processing.silver.fact_load import FactLoadProcessor
    from shiden.processing.silver.fact_price import FactPriceProcessor
    from shiden.processing.silver.fact_weather import FactWeatherProcessor
    from shiden.processing.spark import get_spark

    spark = get_spark()

    # Market-agnostic reference tables first: facts resolve foreign keys
    # against them, and dim_date in particular must span the whole window or
    # the fact joins silently drop every day outside it.
    DimMarketProcessor().process(spark)
    DimLocationProcessor().process(spark)
    DimProductionTypeProcessor().process(spark)

    for chunk_start, chunk_end in _month_chunks(start, end):
        logger.info("Silver: %s → %s", chunk_start, chunk_end)
        DimDateProcessor().process(chunk_start, chunk_end, spark)
        DimDateTimeProcessor().process(chunk_start, chunk_end, spark)
        ExchangeRatesProcessor().process(chunk_start, chunk_end, spark)
        FactPriceProcessor().process(market_id, chunk_start, chunk_end, spark)
        FactGenerationProcessor().process(market_id, chunk_start, chunk_end, spark)
        FactLoadProcessor().process(market_id, chunk_start, chunk_end, spark)
        FactWeatherProcessor().process(market_id, chunk_start, chunk_end, spark)


def stage_gold(market_id: str, start: date, end: date) -> None:
    from shiden.processing.gold.gold_bess_signals import GoldBessSignalsProcessor
    from shiden.processing.gold.gold_generation_hourly import (
        GoldGenerationHourlyProcessor,
    )
    from shiden.processing.gold.gold_price_hourly import GoldPriceHourlyProcessor
    from shiden.processing.spark import get_spark

    spark = get_spark()

    for chunk_start, chunk_end in _month_chunks(start, end):
        logger.info("Gold: %s → %s", chunk_start, chunk_end)
        GoldPriceHourlyProcessor().process(market_id, chunk_start, chunk_end, spark)
        GoldGenerationHourlyProcessor().process(
            market_id, chunk_start, chunk_end, spark
        )
        GoldBessSignalsProcessor().process(market_id, chunk_start, chunk_end, spark)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"expected YYYY-MM-DD, got {value!r}"
        ) from exc


def _csv_list(valid: tuple[str, ...]):
    def parse(value: str) -> set[str]:
        items = {v.strip().lower() for v in value.split(",") if v.strip()}
        unknown = items - set(valid)
        if unknown:
            raise argparse.ArgumentTypeError(
                f"unknown value(s) {sorted(unknown)}; valid: {list(valid)}"
            )
        return items

    return parse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shiden-backfill",
        description="Backfill historical data across Bronze, Silver and Gold.",
    )
    parser.add_argument(
        "--from",
        dest="start",
        type=_parse_date,
        default=MTU_TRANSITION_DATE,
        help=(
            f"start date (default {MTU_TRANSITION_DATE.isoformat()}, "
            "the 15-min MTU transition)"
        ),
    )
    parser.add_argument(
        "--to",
        dest="end",
        type=_parse_date,
        default=date.today() - timedelta(days=1),
        help="end date, inclusive (default: yesterday)",
    )
    parser.add_argument("--market", default="RO", help="market id (default RO)")
    parser.add_argument(
        "--sources",
        type=_csv_list(ALL_SOURCES),
        default=set(ALL_SOURCES),
        help=f"comma-separated: {','.join(ALL_SOURCES)}",
    )
    parser.add_argument(
        "--stages",
        type=_csv_list(ALL_STAGES),
        default=set(ALL_STAGES),
        help=f"comma-separated: {','.join(ALL_STAGES)}",
    )
    parser.add_argument(
        "--refetch",
        action="store_true",
        help="refetch landing files that already exist (default: skip, so runs resume)",
    )
    parser.add_argument(
        "--allow-pre-mtu",
        action="store_true",
        help=(
            "permit a start date before the 15-min transition. OPCOM's hourly "
            "exports are rejected per-day by the Bronze parser regardless; this "
            "only silences the up-front check."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="print the plan and exit"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    args = build_parser().parse_args(argv)

    if args.market not in MARKETS:
        print(
            f"error: unknown market {args.market!r}; known: {sorted(MARKETS)}",
            file=sys.stderr,
        )
        return 2

    if args.end < args.start:
        print("error: --to must be >= --from", file=sys.stderr)
        return 2

    if args.start < MTU_TRANSITION_DATE and not args.allow_pre_mtu:
        print(
            f"error: --from {args.start.isoformat()} precedes the 15-minute MTU "
            f"transition ({MTU_TRANSITION_DATE.isoformat()}). OPCOM published "
            "hourly intervals before that date and Bronze stores quarter-hourly "
            "only. Use --allow-pre-mtu to proceed anyway (those days will be "
            "rejected by the Bronze parser).",
            file=sys.stderr,
        )
        return 2

    days = (args.end - args.start).days + 1
    print(
        f"Backfill plan: market={args.market} "
        f"{args.start.isoformat()} → {args.end.isoformat()} ({days} days)\n"
        f"  sources: {','.join(sorted(args.sources))}\n"
        f"  stages:  {','.join(s for s in ALL_STAGES if s in args.stages)}\n"
        f"  resume:  {'no (--refetch)' if args.refetch else 'yes'}"
    )
    if args.dry_run:
        print("\nDry run — nothing fetched or written.")
        return 0

    failures = 0
    if "landing" in args.stages:
        failures += stage_landing(
            args.market, args.start, args.end, args.sources, args.refetch
        )
    if "bronze" in args.stages:
        stage_bronze(args.market, args.start, args.end, args.sources)
    if "silver" in args.stages:
        stage_silver(args.market, args.start, args.end)
    if "gold" in args.stages:
        stage_gold(args.market, args.start, args.end)

    if failures:
        print(
            f"\nCompleted with {failures} source failure(s) — see warnings above. "
            "Rerun the same command to retry only the missing days.",
            file=sys.stderr,
        )
        return 1

    print("\nBackfill complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
