# Getting started

## Prerequisites

- **Python ≥ 3.10** (the dev container ships 3.13)
- **Java 11+** — required by PySpark. Only the processing layer needs it; the
  API reads Delta through `delta-rs` and runs without a JVM.
- **[uv](https://github.com/astral-sh/uv)** for dependency management

The supplied VS Code dev container (`.devcontainer/`) has Python, Java and uv
preinstalled and is the fastest path to a working environment.

## Install

```bash
uv sync --extra dev          # runtime + test/lint tooling
uv sync --extra docs         # add this to build these docs
```

## Configure

```bash
cp .env.example .env
```

Settings are read by {py:class}`shiden.config.settings.Settings` (pydantic-settings),
so every field below can be set in `.env` or as an environment variable of the
same name, upper-cased.

| Setting | Default | Purpose |
|---|---|---|
| `ENVIRONMENT` | `local` | `local` \| `databricks` — selects the Spark session builder |
| `DELTA_BASE_PATH` | `./data/delta` | Root of the Delta lakehouse |
| `LANDING_BASE_PATH` | `./data` | Root of the immutable landing zone |
| `ENTSOE_API_KEY` | *(empty)* | Required for generation/load ingestion — register at [transparency.entsoe.eu](https://transparency.entsoe.eu) |
| `OPCOM_RATE_LIMIT_SLEEP` | `1.0` | Seconds between OPCOM requests for different dates |
| `API_HOST` / `API_PORT` | `0.0.0.0` / `8000` | Uvicorn bind address |
| `API_AUTH_ENABLED` | `true` | Set `false` **only** for local work against throwaway data |
| `AUTH_DB_PATH` | `./data/shiden_auth.db` | SQLite key store and usage ledger |
| `SPARK_MASTER` | `local[*]` | Spark master URL in local mode |
| `SPARK_APP_NAME` | `shiden` | Spark application name |

At minimum, set `ENTSOE_API_KEY`. OPCOM, Open-Meteo, BNR and ECB need no
credentials.

:::{warning}
`data/` is gitignored and holds both the lakehouse and the credential store.
In production, put `AUTH_DB_PATH` on durable, backed-up storage — or migrate
the schema to Postgres.
:::

## Run the API

```bash
uv run uvicorn shiden.api.main:app --reload --port 8000
```

- Interactive OpenAPI docs: <http://localhost:8000/docs>
- Liveness probe (unauthenticated): <http://localhost:8000/health>

Every `/v1` endpoint requires an API key. Issue one first:

```bash
uv run python -m shiden.api.auth.cli issue --name "Local dev"
```

The secret is printed **once** — only its SHA-256 hash is stored. See
{doc}`api-guide` for the full authentication model.

## Run the scheduler

```bash
uv run python -m shiden.scheduler.runner
```

This starts a blocking APScheduler process running the daily pipeline on
Europe/Bucharest local time:

| Time | Job |
|---|---|
| 06:00 | {py:func}`~shiden.scheduler.jobs.run_weather_daily` |
| 08:00 | {py:func}`~shiden.scheduler.jobs.run_entsoe_daily` |
| 14:00 | {py:func}`~shiden.scheduler.jobs.run_opcom_pzu_daily` |
| 17:00 | {py:func}`~shiden.scheduler.jobs.run_fx_rates_daily` |
| 20:00 | {py:func}`~shiden.scheduler.jobs.run_silver_daily` |
| 21:00 | {py:func}`~shiden.scheduler.jobs.run_gold_daily` |

Jobs are registered with a one-hour misfire grace and `coalesce=True`, so a
missed window runs late rather than being skipped, and a backlog collapses into
a single run. On Databricks the same functions are invoked by Databricks
Workflows — the scheduler module is local/MVP wiring only.

## Load history

The scheduler only ever fetches a window relative to *today*. History must be
loaded explicitly:

```bash
uv run python -m shiden.backfill                       # 2025-10-01 → yesterday
uv run python -m shiden.backfill --dry-run             # show the plan only
```

See {doc}`operations` for the full backfill reference, including resuming an
interrupted run and re-running transforms without network access.

## First query

```bash
curl -H "X-API-Key: shiden_live_..." \
  "http://localhost:8000/v1/bess/RO/signals?date=2026-08-01"
```

## Build these docs

```bash
uv run --extra docs sphinx-build -b html docs docs/_build/html
# or, from docs/:
make html
```

Output lands in `docs/_build/html/index.html`. Add `-W` to turn warnings into
errors, which is what CI should do once the docs stabilise.
