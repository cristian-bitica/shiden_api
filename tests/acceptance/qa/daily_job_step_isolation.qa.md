# QA Procedure: Daily pipeline job step error isolation

Covers: `tests/acceptance/features/daily_job_step_isolation.feature`
(scenario `daily-job-step-isolation-1`).

## User interface under test

The daily jobs module's command-line entry point:

```bash
uv run python -m shiden.scheduler.jobs --job <job>
```

`--job <job>` is a QA-facing command-line affordance: it runs exactly one
named daily job once and exits, printing logs to stdout in the format set by
`logging.basicConfig` (`%(asctime)s %(levelname)s %(name)s %(message)s`).
`<job>` is one of the six function names in the Examples table below. This
flag is the only project-specific affordance QA needs; everything else
(forcing a step to fail, reading output, checking the exit code) uses
standard OS-level facilities, not a project API.

## Fault injection (user-facing, not an internal API)

Point the job at a landing directory QA has made unwritable, using the
project's documented environment-variable configuration (see
`docs/getting-started.md` / `.env.example`):

```bash
export LANDING_BASE_PATH=<a directory QA creates and chmods to read-only>
```

Writing to that path is the first filesystem write every job in the table
performs, so this reliably makes one pipeline step fail without touching
application code or internals.

## Procedure

For each `<job>` row in the Examples table:

1. Create a fresh temporary directory and remove write permission from it
   (e.g. `chmod 555`).
2. Set `LANDING_BASE_PATH` to that directory in the process environment.
3. Run `uv run python -m shiden.scheduler.jobs --job <job>` and capture
   stdout/stderr and the process exit code.
4. Restore/remove the temporary directory.

## Expected observable outcome (every row)

- The process exits 0 (success) — a failing step must not fail the job run.
- The captured output contains at least one `ERROR` line naming the failing
  step (job-specific ingest/processing step logs an ERROR, not a WARNING or
  silence).
- The captured output contains the job's own "complete" log line (each job
  logs a "... pipeline complete ..." message at the end of its function) —
  proof the job ran to completion instead of aborting partway through.

## Examples

| job                 |
|---------------------|
| run_opcom_pzu_daily |
| run_weather_daily   |
| run_entsoe_daily    |
| run_fx_rates_daily  |
| run_silver_daily    |
| run_gold_daily      |

## Notes for QA

- `run_weather_daily`, `run_entsoe_daily`, `run_fx_rates_daily`,
  `run_silver_daily`, and `run_gold_daily` are expected to already pass today
  (regression coverage — do not weaken these rows to make the suite green).
- `run_opcom_pzu_daily` originally failed this procedure (unhandled
  traceback, nonzero exit) until coder wrapped its ingest/bronze steps in
  `_run_safe` to match the other five jobs' error isolation. QA has
  verified all six rows now pass; do not adjust this procedure to accept
  a future regression on this row.
- `--job <job>` does not exist yet on `shiden.scheduler.jobs`; it is a
  required user-interface affordance for this QA suite. Add it (argparse,
  one job name → one function call, then exit) rather than importing job
  functions directly into the QA script.
- `_run_safe` logs a caught step failure with `exc_info=True` by design
  (asserted in `tests/unit/test_scheduler_jobs.py`, hardened by mutation
  testing), so a correctly isolated failure's captured output legitimately
  contains a `Traceback (most recent call last):` block. Do not treat that
  text as a failure signal; rely on exit code 0, the `ERROR` line, and the
  "... pipeline complete ..." line instead. Confirmed with operator
  2026-09-26.
