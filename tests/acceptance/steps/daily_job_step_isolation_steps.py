"""Step handlers for tests/acceptance/features/daily_job_step_isolation.feature.

Fault injection stubs every dependency a job calls (no real I/O, no
network, no Spark) and makes exactly one of them raise -- this exercises
"a pipeline step ... raises an exception" generically, without caring
which internal step it is or depending on live infrastructure the way the
specifier's black-box QA procedure does.
"""

from __future__ import annotations

import logging
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from shiden.scheduler import jobs as jobs_module
from tests.acceptance.runtime.engine import StepRegistry

registry = StepRegistry()

_JOB_LOGGER_NAME = "shiden.scheduler.jobs"


class AcceptanceInjectedFailure(RuntimeError):
    """Raised by a stubbed dependency to simulate a failing pipeline step."""


# (dotted class path, method called on it) for every dependency a job
# touches, in call order. The first entry is the injected fault point; the
# rest are stubbed to a harmless MagicMock so the isolation boundary is
# exercised in full without any real I/O.
_JOB_DEPENDENCIES: dict[str, list[tuple[str, str]]] = {
    "run_opcom_pzu_daily": [
        ("shiden.ingestion.opcom.OpcomIngester", "ingest"),
        ("shiden.processing.bronze.opcom.OpcomBronzeWriter", "process"),
    ],
    "run_weather_daily": [
        ("shiden.ingestion.weather.WeatherIngester", "ingest"),
        ("shiden.processing.bronze.weather.WeatherBronzeWriter", "process"),
    ],
    "run_entsoe_daily": [
        ("shiden.ingestion.entsoe.EntsoeIngester", "ingest"),
        ("shiden.processing.bronze.entsoe.EntsoeBronzeWriter", "process"),
    ],
    "run_fx_rates_daily": [
        ("shiden.ingestion.bnr.BnrIngester", "ingest"),
        ("shiden.ingestion.ecb.EcbIngester", "ingest"),
        ("shiden.processing.bronze.bnr.BnrBronzeWriter", "process"),
        ("shiden.processing.bronze.ecb.EcbBronzeWriter", "process"),
    ],
    "run_silver_daily": [
        (
            "shiden.processing.silver.exchange_rates.ExchangeRatesProcessor",
            "process",
        ),
        (
            "shiden.processing.silver.dimensions.dim_datetime.DimDateTimeProcessor",
            "process",
        ),
        (
            "shiden.processing.silver.dimensions.dim_market.DimMarketProcessor",
            "process",
        ),
        (
            "shiden.processing.silver.dimensions.dim_location.DimLocationProcessor",
            "process",
        ),
        (
            "shiden.processing.silver.dimensions.dim_date.DimDateProcessor",
            "process",
        ),
        (
            "shiden.processing.silver.dimensions.dim_production_type."
            "DimProductionTypeProcessor",
            "process",
        ),
        ("shiden.processing.silver.fact_price.FactPriceProcessor", "process"),
        (
            "shiden.processing.silver.fact_generation.FactGenerationProcessor",
            "process",
        ),
        ("shiden.processing.silver.fact_load.FactLoadProcessor", "process"),
        (
            "shiden.processing.silver.fact_weather.FactWeatherProcessor",
            "process",
        ),
    ],
    "run_gold_daily": [
        (
            "shiden.processing.gold.gold_price_hourly.GoldPriceHourlyProcessor",
            "process",
        ),
        (
            "shiden.processing.gold.gold_generation_hourly."
            "GoldGenerationHourlyProcessor",
            "process",
        ),
        (
            "shiden.processing.gold.gold_bess_signals.GoldBessSignalsProcessor",
            "process",
        ),
    ],
}


class _ListHandler(logging.Handler):
    def __init__(self, sink: list[logging.LogRecord]) -> None:
        super().__init__()
        self._sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        self._sink.append(record)


def _start_fault_injection(job: str) -> ExitStack:
    stack = ExitStack()
    stack.enter_context(
        patch("shiden.processing.spark.get_spark", return_value=MagicMock())
    )
    for index, (dotted_class, method_name) in enumerate(_JOB_DEPENDENCIES[job]):
        mock_class = stack.enter_context(patch(dotted_class))
        if index == 0:
            getattr(mock_class.return_value, method_name).side_effect = (
                AcceptanceInjectedFailure("acceptance-injected pipeline step failure")
            )
    return stack


@registry.step(
    r'^a pipeline step inside daily job "(?P<job>[^"]+)" raises an exception$'
)
def given_a_step_raises(context: dict, job: str) -> None:
    context["job"] = job
    context["fault_patches"] = _start_fault_injection(job)


@registry.step(r'^daily job "(?P<job>[^"]+)" runs$')
def when_job_runs(context: dict, job: str) -> None:
    records: list[logging.LogRecord] = []
    context["log_records"] = records

    job_logger = logging.getLogger(_JOB_LOGGER_NAME)
    list_handler = _ListHandler(records)
    previous_level = job_logger.level
    job_logger.addHandler(list_handler)
    job_logger.setLevel(logging.DEBUG)
    try:
        try:
            getattr(jobs_module, job)()
            context["raised"] = None
        except BaseException as exc:  # noqa: BLE001 - captured for Then/And to assert
            context["raised"] = exc
    finally:
        job_logger.removeHandler(list_handler)
        job_logger.setLevel(previous_level)
        context["fault_patches"].close()


@registry.step(r'^daily job "(?P<job>[^"]+)" logs an ERROR for the failing step$')
def then_logs_error(context: dict, job: str) -> None:
    errors = [r for r in context["log_records"] if r.levelno == logging.ERROR]
    assert errors, f"{job}: expected at least one ERROR log record, got none"


@registry.step(r'^daily job "(?P<job>[^"]+)" completes without raising$')
def and_completes_without_raising(context: dict, job: str) -> None:
    assert context["raised"] is None, f"{job} raised: {context['raised']!r}"
