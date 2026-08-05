"""Local APScheduler wiring for the daily pipeline jobs.

Runs the documented schedule (Europe/Bucharest local time):

    06:00  run_weather_daily
    08:00  run_entsoe_daily
    14:00  run_opcom_pzu_daily
    17:00  run_fx_rates_daily
    20:00  run_silver_daily
    21:00  run_gold_daily

Local/MVP only — on Databricks the same job functions are triggered by
Databricks Workflows instead (zero migration cost by design).

Start with:

    python -m shiden.scheduler.runner
"""

from __future__ import annotations

import logging

from shiden.scheduler import jobs

logger = logging.getLogger(__name__)

_TIMEZONE = "Europe/Bucharest"

# (job function, hour, minute)
_SCHEDULE = (
    (jobs.run_weather_daily, 6, 0),
    (jobs.run_entsoe_daily, 8, 0),
    (jobs.run_opcom_pzu_daily, 14, 0),
    (jobs.run_fx_rates_daily, 17, 0),
    (jobs.run_silver_daily, 20, 0),
    (jobs.run_gold_daily, 21, 0),
)


def main() -> None:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    scheduler = BlockingScheduler(timezone=_TIMEZONE)
    for fn, hour, minute in _SCHEDULE:
        scheduler.add_job(
            fn,
            CronTrigger(hour=hour, minute=minute, timezone=_TIMEZONE),
            id=fn.__name__,
            name=fn.__name__,
            misfire_grace_time=3600,  # run late rather than skip (1 h grace)
            coalesce=True,  # collapse a backlog of missed runs into one
        )
        logger.info("Scheduled %s at %02d:%02d %s", fn.__name__, hour, minute,
                    _TIMEZONE)

    logger.info("Scheduler started — %d jobs registered", len(_SCHEDULE))
    scheduler.start()


if __name__ == "__main__":
    main()
