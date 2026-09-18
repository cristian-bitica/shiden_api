# Operations

## Scheduled pipeline

The daily pipeline is a set of plain functions in
{py:mod}`shiden.scheduler.jobs`. Locally they are driven by APScheduler
({py:mod}`shiden.scheduler.runner`); in production Databricks Workflows calls
the *same* functions. That symmetry is the point — there is no orchestration
code to port when moving to Databricks.

```bash
uv run python -m shiden.scheduler.runner
```

| Time (Europe/Bucharest) | Job | What it does |
|---|---|---|
| 06:00 | `run_weather_daily` | Open-Meteo → landing → Bronze |
| 08:00 | `run_entsoe_daily` | ENTSO-E generation + load → landing → Bronze |
| 14:00 | `run_opcom_pzu_daily` | OPCOM day-ahead prices → landing → Bronze |
| 17:00 | `run_fx_rates_daily` | BNR + ECB FX → landing → Bronze |
| 20:00 | `run_silver_daily` | Bronze → Silver star schema |
| 21:00 | `run_gold_daily` | Silver → Gold API tables |

Two behaviours worth knowing:

- **Per-step error isolation.** Each processor runs through `_run_safe`, which
  logs a failure as `ERROR` and continues. One broken source does not abort the
  rest of the run.
- **Self-healing lookback.** The OPCOM and weather jobs refetch the last
  `OPCOM_LOOKBACK_DAYS` (7) days on every run, so a transient upstream outage
  repairs itself without operator action.

Jobs are registered with `misfire_grace_time=3600` and `coalesce=True`: a run
missed by up to an hour executes late rather than being skipped, and a backlog
of missed runs collapses into one.

## Backfilling history

The scheduler only ever fetches a window relative to *today*, so history is
loaded explicitly with {py:mod}`shiden.backfill`.

```bash
# Full history from the 15-min MTU transition to yesterday
uv run python -m shiden.backfill

# Explicit range
uv run python -m shiden.backfill --from 2025-10-01 --to 2026-08-10

# Re-run transforms only — no network
uv run python -m shiden.backfill --from 2026-07-01 --to 2026-08-10 --stages silver,gold

# Show the plan and exit
uv run python -m shiden.backfill --dry-run
```

| Flag | Default | Meaning |
|---|---|---|
| `--from` | `2025-10-01` | Start date — the 15-min MTU transition |
| `--to` | yesterday | End date, inclusive |
| `--market` | `RO` | Market id |
| `--sources` | `opcom,fx` | Comma-separated source subset |
| `--stages` | `landing,bronze,silver,gold` | Comma-separated stage subset |
| `--refetch` | off | Refetch landing files that already exist |
| `--allow-pre-mtu` | off | Permit a start date before the MTU transition |
| `--dry-run` | off | Print the plan without touching anything |

**Runs are resumable.** Landing files already on disk are skipped, so an
interrupted backfill continues where it stopped; pass `--refetch` to force
re-download. Days OPCOM will not serve are collected and reported at the end
rather than aborting the run — rerunning the same command retries only those.

:::{warning}
**Do not backfill before 2025-10-01.** Romania switched to 15-minute Market
Time Units on that date. Earlier OPCOM exports contain 24 hourly intervals that
are *structurally identical* to the 96-interval quarter-hourly format —
ingesting them would map each hour onto a single 15-minute slot and silently
compress a day into six hours. The Bronze parser rejects them per-day, and the
backfill refuses earlier `--from` dates up front. `--allow-pre-mtu` only
silences the up-front check; the parser still rejects each day.
:::

## API key management

The key store is a SQLite file at `AUTH_DB_PATH` (default
`./data/shiden_auth.db`). `data/` is gitignored — in production this file holds
live credentials, so put it on durable, backed-up storage or migrate the schema
to Postgres.

```bash
uv run python -m shiden.api.auth.cli issue --name "Acme Energy" --markets RO --rate-limit 120
uv run python -m shiden.api.auth.cli issue --name "Demo"          # all markets
uv run python -m shiden.api.auth.cli list
uv run python -m shiden.api.auth.cli revoke shiden_live_a1b2c3d4
uv run python -m shiden.api.auth.cli usage --days 7
```

Full flag reference: {doc}`reference/cli`.

## Testing

```bash
uv run pytest tests/unit          # no network, no Spark cluster
uv run pytest -m integration      # live network + local Delta snapshot
```

Integration tests are excluded by default (`addopts = "-m 'not integration'"`)
because they need live upstream access and a local Bronze snapshot that `data/`
does not carry into a fresh checkout.

## Linting and type-checking

```bash
uv run ruff check src tests
uv run mypy src
```

`mypy` runs in `strict` mode over `src`. Third-party packages without stubs
(`pyspark`, `delta`, `entsoe`, `apscheduler`, `pandas`) are exempted from
missing-import errors, and the test suite relaxes the untyped-def rules.

`ruff` enforces an 88-character line limit, with per-file exemptions for three
modules whose aligned table-style literals read better wide than wrapped.

## CI

`.github/workflows/ci.yml` runs ruff, mypy and the unit tests on every push and
PR to `main`, on Python from `pyproject.toml` with Temurin JDK 17 for PySpark.
Integration tests are deliberately excluded.

To add a docs build to CI:

```yaml
      - name: Build docs
        run: uv run --extra docs sphinx-build -b html -W docs docs/_build/html
```

`-W` turns warnings into errors, which catches a broken cross-reference or a
page dropped from the toctree at PR time rather than in production.

## Schema migrations

Silver fact schemas and merge keys changed when `dim_time` was replaced by
`dim_datetime`. Silver and Gold must be rebuilt; Bronze is unaffected, since it
is append-only raw capture and carries no derived time columns.

```bash
rm -rf data/delta/silver data/delta/gold
uv run python -m shiden.backfill --stages silver,gold
```

The obsolete `silver/dim_time` table should be deleted. See {doc}`time-model`
for why the change was necessary.
