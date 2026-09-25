"""Daily pipeline job functions.

Each ``run_*_daily`` function is a self-contained pipeline step designed to
be triggered by a scheduler (see ``shiden.scheduler.runner`` for the local
APScheduler wiring; Databricks Workflows calls the same functions in prod).

Ingestion jobs iterate over the MARKETS registry where the source supports
multiple markets (weather, ENTSO-E).  OPCOM is inherently Romania-only —
it is the Romanian market operator; a new market brings its own price
source and ingester (e.g. EPEX for DE).
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from functools import partial
from typing import Callable

logger = logging.getLogger(__name__)


def _run_safe(name: str, fn: Callable[[], None]) -> None:
    """Run fn(), log any exception as ERROR and continue.

    Gives per-processor error isolation so one failing processor doesn't
    abort the rest of a pipeline run.
    """
    try:
        fn()
    except Exception as exc:
        logger.error("Pipeline step %s FAILED: %s", name, exc, exc_info=True)


# Days of history refetched on every OPCOM run. Matches the weather job's
# window so both sources self-heal identically.
OPCOM_LOOKBACK_DAYS = 7


def run_opcom_pzu_daily() -> None:
    """
    Fetch OPCOM PZU (Day-Ahead) raw CSVs to landing, then process into Bronze.

    Run daily at 14:00 Romanian time (EET/EEST) — after PZU results are
    published at ~13:00.  The published results cover the *next* delivery day,
    so 'today' here is the delivery date that was auctioned yesterday.

    Fetches a rolling D-7 → D0 window rather than today alone. A single-day
    fetch makes every missed run a permanent hole: nothing revisits it, and
    the gap is invisible until someone queries that date. With a lookback,
    one week of downtime repairs itself on the next successful run. Days
    already in the landing zone are skipped, so the extra window costs
    nothing on a healthy day.

    OPCOM is Romania's market operator — this job is deliberately RO-only.
    """
    from shiden.ingestion.opcom import OpcomIngester
    from shiden.processing.bronze.opcom import OpcomBronzeWriter
    from shiden.processing.spark import get_spark

    today = date.today()
    start = today - timedelta(days=OPCOM_LOOKBACK_DAYS)
    logger.info("Starting daily OPCOM PZU ingest for %s–%s", start, today)

    def _ingest() -> None:
        report = OpcomIngester().ingest("RO", start, today, skip_existing=True)
        logger.info("OPCOM ingest: %s", report.summary())
        for failed_date, reason in report.failed:
            logger.warning("OPCOM gap remains for %s — %s", failed_date, reason)

    _run_safe("OpcomIngest", _ingest)

    spark = get_spark()
    _run_safe(
        "OpcomBronze", partial(OpcomBronzeWriter().process, "RO", start, today, spark)
    )

    logger.info("Daily OPCOM PZU pipeline complete for %s–%s", start, today)


def run_weather_daily() -> None:
    """
    Fetch weather for all configured markets to landing, then into Bronze.

    Run daily at 06:00 Romanian time (EET/EEST).

    Two windows are fetched per market on each run:
      REALIZED window  D-7 to D-1  — Open-Meteo archive; backfills recent
                                      actuals used for ML training.
      FORECAST window  D0  to D+2  — Open-Meteo forecast; tomorrow and
                                      day-after inputs for price prediction.

    The ingester auto-selects the endpoint (archive vs forecast) based on
    whether end < today.  Bronze append-with-revision-capture keys on
    (market, location, timestamp, data_type), so re-runs are cheap and a
    REALIZED ingest is never blocked by earlier FORECAST rows for the date.
    """
    from shiden.config.markets import MARKETS, get_market
    from shiden.ingestion.weather import WeatherIngester
    from shiden.processing.bronze.weather import WeatherBronzeWriter
    from shiden.processing.spark import get_spark

    today = date.today()
    realized_start = today - timedelta(days=7)
    realized_end = today - timedelta(days=1)
    forecast_start = today
    forecast_end = today + timedelta(days=2)

    ingester = WeatherIngester()
    writer = WeatherBronzeWriter()
    spark = get_spark()

    for market_id in MARKETS:
        location_names = [
            loc.name for loc in get_market(market_id).weather_locations
        ]
        if not location_names:
            logger.info(
                "Weather: market %s has no configured locations — skipping",
                market_id,
            )
            continue

        logger.info(
            "Starting daily weather ingest for %s: realized=%s–%s forecast=%s–%s",
            market_id,
            realized_start,
            realized_end,
            forecast_start,
            forecast_end,
        )
        _run_safe(
            f"WeatherIngest({market_id}, realized)",
            partial(ingester.ingest, market_id, realized_start, realized_end),
        )
        _run_safe(
            f"WeatherIngest({market_id}, forecast)",
            partial(ingester.ingest, market_id, forecast_start, forecast_end),
        )
        _run_safe(
            f"WeatherBronze({market_id})",
            partial(
                writer.process,
                market_id,
                location_names,
                realized_start,
                forecast_end,
                spark,
            ),
        )

    logger.info("Daily weather pipeline complete for all markets")


def run_fx_rates_daily() -> None:
    """
    Fetch BNR and ECB FX rates to landing, then process into Bronze.

    Run daily at 17:00 Romanian time (EET/EEST).
    - BNR publishes at ~13:00 Bucharest; safe to fetch after 16:00.
    - ECB publishes at ~16:00 CET; yesterday's confirmed rate is safe at 17:00.

    Fetches the last 7 days to backfill any missed business days.
    """
    from shiden.ingestion.bnr import BnrIngester
    from shiden.ingestion.ecb import EcbIngester
    from shiden.processing.bronze.bnr import BnrBronzeWriter
    from shiden.processing.bronze.ecb import EcbBronzeWriter
    from shiden.processing.spark import get_spark

    today = date.today()
    start = today - timedelta(days=7)

    spark = get_spark()
    logger.info("Starting daily FX rates ingest for %s–%s", start, today)

    _run_safe("BnrIngest", partial(BnrIngester().ingest, start, today))
    _run_safe("BnrBronze", partial(BnrBronzeWriter().process, start, today, spark))

    _run_safe("EcbIngest", partial(EcbIngester().ingest, start, today))
    _run_safe("EcbBronze", partial(EcbBronzeWriter().process, start, today, spark))

    logger.info("Daily FX rates pipeline complete for %s–%s", start, today)


def run_entsoe_daily() -> None:
    """
    Fetch ENTSO-E actual generation (A75) and load (A65) for all configured
    markets to landing, then process into Bronze.

    Run daily at 08:00 Romanian time (EET/EEST).
    ENTSO-E typically publishes D-1 actuals by 06:00 UTC, so 08:00 EET is safe.

    Fetches D-7 to D-1 on each run — landing files are re-written so revised
    actuals propagate; Bronze appends only changed observations.
    Requires ENTSOE_API_KEY in .env.
    """
    from shiden.config.markets import MARKETS
    from shiden.ingestion.entsoe import EntsoeIngester
    from shiden.processing.bronze.entsoe import EntsoeBronzeWriter
    from shiden.processing.spark import get_spark

    today = date.today()
    start = today - timedelta(days=7)
    end = today - timedelta(days=1)

    ingester = EntsoeIngester()
    writer = EntsoeBronzeWriter()
    spark = get_spark()

    for market_id in MARKETS:
        logger.info(
            "Starting daily ENTSO-E ingest for %s: %s–%s", market_id, start, end
        )
        _run_safe(
            f"EntsoeIngest({market_id})",
            partial(ingester.ingest, market_id, start, end),
        )
        _run_safe(
            f"EntsoeBronze({market_id})",
            partial(writer.process, market_id, start, end, spark),
        )

    logger.info("Daily ENTSO-E pipeline complete for all markets")


def run_silver_daily() -> None:
    """
    Build / refresh all Silver star schema tables for the last 7 days.

    Run order (enforced here):
      1. exchange_rates — needs BNR/ECB Bronze; facts join it for EUR prices.
      2. Dimensions — fact tables depend on them for FK lookups.
      3. Facts — iterated over all configured markets.

    Each processor is wrapped with _run_safe so one failure doesn't abort
    the entire run.

    Schedule: daily at 20:00 Romanian time (EET/EEST) — after all Bronze
    jobs have completed (Weather 06:00, ENTSO-E 08:00, OPCOM 14:00, FX 17:00).
    """
    from shiden.config.markets import MARKETS
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

    today = date.today()
    start = today - timedelta(days=7)
    end = today - timedelta(days=1)

    spark = get_spark()
    logger.info("Starting daily Silver pipeline for all markets %s–%s", start, end)

    # --- Reference data (market-agnostic — run once) ---
    _run_safe(
        "ExchangeRatesProcessor",
        partial(ExchangeRatesProcessor().process, start, end, spark),
    )
    _run_safe(
        "DimDateTimeProcessor",
        partial(DimDateTimeProcessor().process, start, end, spark),
    )
    _run_safe("DimMarketProcessor", partial(DimMarketProcessor().process, spark))
    _run_safe("DimLocationProcessor", partial(DimLocationProcessor().process, spark))
    _run_safe(
        "DimDateProcessor", partial(DimDateProcessor().process, start, end, spark)
    )
    _run_safe(
        "DimProductionTypeProcessor",
        partial(DimProductionTypeProcessor().process, spark),
    )

    # --- Facts (per-market) ---
    for market_id in MARKETS:
        logger.info("Silver fact processors starting for market=%s", market_id)
        _run_safe(
            f"FactPriceProcessor({market_id})",
            partial(FactPriceProcessor().process, market_id, start, end, spark),
        )
        _run_safe(
            f"FactGenerationProcessor({market_id})",
            partial(FactGenerationProcessor().process, market_id, start, end, spark),
        )
        _run_safe(
            f"FactLoadProcessor({market_id})",
            partial(FactLoadProcessor().process, market_id, start, end, spark),
        )
        _run_safe(
            f"FactWeatherProcessor({market_id})",
            partial(FactWeatherProcessor().process, market_id, start, end, spark),
        )

    logger.info("Daily Silver pipeline complete for all markets %s–%s", start, end)


def run_gold_daily() -> None:
    """
    Build / refresh all Gold tables for the last 7 days.

    Runs after run_silver_daily() — Gold reads from Silver.

    Schedule: daily at 21:00 Romanian time (EET/EEST) — after Silver (20:00).

    Gold tables produced:
      gold/price_hourly      : hourly prices (local + EUR via exchange_rates)
      gold/generation_hourly : wide generation pivot + renewable metrics
      gold/bess_signals      : 15-min charge/discharge signals per market-day
    """
    from shiden.config.markets import MARKETS
    from shiden.processing.gold.gold_bess_signals import GoldBessSignalsProcessor
    from shiden.processing.gold.gold_generation_hourly import (
        GoldGenerationHourlyProcessor,
    )
    from shiden.processing.gold.gold_price_hourly import GoldPriceHourlyProcessor
    from shiden.processing.spark import get_spark

    today = date.today()
    start = today - timedelta(days=7)
    end = today - timedelta(days=1)

    spark = get_spark()
    logger.info("Starting daily Gold pipeline for all markets %s–%s", start, end)

    for market_id in MARKETS:
        logger.info("Gold processors starting for market=%s", market_id)
        _run_safe(
            f"GoldPriceHourlyProcessor({market_id})",
            partial(
                GoldPriceHourlyProcessor().process, market_id, start, end, spark
            ),
        )
        _run_safe(
            f"GoldGenerationHourlyProcessor({market_id})",
            partial(
                GoldGenerationHourlyProcessor().process, market_id, start, end, spark
            ),
        )
        _run_safe(
            f"GoldBessSignalsProcessor({market_id})",
            partial(
                GoldBessSignalsProcessor().process, market_id, start, end, spark
            ),
        )

    logger.info("Daily Gold pipeline complete for all markets %s–%s", start, end)


def main(argv: list[str] | None = None) -> None:
    """CLI entry point.

    With no arguments, runs the full daily pipeline (ingest → bronze →
    silver → gold) once, in schedule order. ``--job <name>`` instead runs
    exactly that one job once and exits — a QA/ops affordance for exercising
    a single job in isolation.
    """
    jobs_by_name: dict[str, Callable[[], None]] = {
        "run_opcom_pzu_daily": run_opcom_pzu_daily,
        "run_weather_daily": run_weather_daily,
        "run_entsoe_daily": run_entsoe_daily,
        "run_fx_rates_daily": run_fx_rates_daily,
        "run_silver_daily": run_silver_daily,
        "run_gold_daily": run_gold_daily,
    }

    parser = argparse.ArgumentParser(description="Run daily pipeline jobs.")
    parser.add_argument(
        "--job",
        choices=sorted(jobs_by_name),
        help="Run exactly this named daily job once, then exit.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.job:
        jobs_by_name[args.job]()
        return

    # Manual full pipeline run (ingest → bronze → silver → gold).
    run_weather_daily()
    run_fx_rates_daily()
    run_entsoe_daily()
    run_opcom_pzu_daily()
    run_silver_daily()
    run_gold_daily()


if __name__ == "__main__":
    main()
