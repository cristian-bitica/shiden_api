# Shiden

Energy market data and BESS (Battery Energy Storage System) intelligence API for operators and aggregators. Collects, processes, and serves energy price signals, weather data, and derived intelligence (arbitrage windows, charge/discharge signals, price forecasts).

**Initial market:** Romania. Architecture is multi-market from day one — adding a market is config-only.

## Tech stack

- **Processing:** PySpark 3.5 + Delta Lake (medallion architecture: Landing → Bronze → Silver → Gold)
- **API:** FastAPI + Uvicorn
- **Scheduling:** APScheduler (local) / Databricks Workflows (prod)
- **ML:** scikit-learn (MVP forecasting)
- **Package manager:** [uv](https://github.com/astral-sh/uv)
- **Production target:** Azure Databricks

See [docs/architecture.md](docs/architecture.md) for full architecture, data sources, and schema details, and [docs/diagrams.md](docs/diagrams.md) for the system, lineage, star-schema and request-path diagrams.

## Documentation

The full technical documentation is a Sphinx site: hand-written guides plus an
API reference generated from the source docstrings, and Mermaid architecture
diagrams that render both here on GitHub and in the built site.

```bash
uv sync --extra docs
uv run sphinx-build -b html docs docs/_build/html   # → docs/_build/html/index.html
# or, from docs/:  make html   (make strict = warnings as errors, as in CI)
```

| Page | Contents |
|---|---|
| `docs/getting-started.md` | Install, configure, run the API and scheduler |
| `docs/api-guide.md` | Auth, entitlements, rate limits, endpoints, errors |
| `docs/operations.md` | Schedule, backfills, key management, testing, CI |
| `docs/architecture.md` | Medallion pipeline, sources, deployment targets |
| `docs/diagrams.md` | Mermaid: context, lineage, star schema, request path |
| `docs/time-model.md` | DST rules and why the UTC instant is the only safe key |
| `docs/silver_schema.md` | Kimball star schema column reference |
| `docs/reference/` | Auto-generated reference for every `shiden` module |

The build also dumps the live FastAPI contract to
`docs/_build/html/_static/openapi.json`, so the machine-readable spec cannot
drift from the code.

## Prerequisites

- Python >= 3.10
- Java 11+ (required by PySpark)
- [uv](https://github.com/astral-sh/uv)

Alternatively, use the provided VSCode dev container (`.devcontainer/`), which includes Python 3.13, Java, and uv preinstalled.

## Setup

```bash
# Install dependencies (incl. dev tools)
uv sync --extra dev

# Configure environment
cp .env.example .env
# then edit .env — at minimum set ENTSOE_API_KEY (register at transparency.entsoe.eu)
```

## Running

```bash
# Start the API server
uv run uvicorn shiden.api.main:app --reload --port 8000
```

API docs (auto-generated): http://localhost:8000/docs
Health check: http://localhost:8000/health

```bash
# Start the local scheduler (runs ingestion/processing jobs on a schedule)
uv run python -m shiden.scheduler.runner
```

## Backfilling history

The scheduler only ever fetches a window relative to *today*, so history has to
be loaded explicitly:

```bash
# Full history from the 15-min MTU transition to yesterday (~315 days)
uv run python -m shiden.backfill

# Explicit range
uv run python -m shiden.backfill --from 2025-10-01 --to 2026-08-10

# Re-run transforms only, no network
uv run python -m shiden.backfill --from 2026-07-01 --to 2026-08-10 --stages silver,gold

# Show the plan without touching anything
uv run python -m shiden.backfill --dry-run
```

Runs are **resumable**: landing files already on disk are skipped, so an
interrupted backfill continues where it stopped. Pass `--refetch` to force.
Days OPCOM will not serve are collected and reported at the end rather than
aborting the run — rerun the same command to retry only those.

> **Romania switched to 15-minute Market Time Units on 2025-10-01.** Earlier
> OPCOM exports contain 24 hourly intervals, structurally identical to the
> 96-interval quarter-hourly format. Ingesting them would map each hour onto a
> single 15-minute slot and silently compress a day into six hours, so the
> Bronze parser rejects them and the backfill refuses `--from` dates before the
> transition. This is why the default start date is 2025-10-01.
>
> DST days are short or long: 2025-10-26 returns 100 intervals and 2026-03-29
> returns 92. `dim_time` has only 96 rows, so the four extra intervals on the
> autumn changeover day currently have no dimension row to join to.

## Time model

A delivery day is a **local** day, and on the two DST changeovers it is not 24
hours long: 23 hours (92 intervals) in spring, 25 hours (100 intervals) in
autumn. OPCOM numbers intervals by *elapsed slot within the local day*, which
its own summary block confirms — the 08:00–20:00 peak block is published as
intervals `33-80` normally, `29-76` on 2026-03-29 and `37-84` on 2025-10-26.

Consequences that shape the schema:

- **The UTC instant is the only safe key.** On the autumn changeover local
  03:00 occurs twice, so `(date_id, local_time_id)` is genuinely non-unique.
  Facts key on `timestamp_utc` (prices) or `hour_start_utc` (hourly facts);
  local columns are labels only.
- **`silver/dim_datetime`** replaces the old static 96-row `dim_time`. Grain is
  one row per settlement interval **per IANA timezone**, generated per date
  range like `dim_date`. It resolves an OPCOM interval number to an instant and
  a clock label — neither derivable arithmetically on a changeover day.
- **Keyed by timezone, not market.** The axis is a property of the zone, and
  European power markets routinely split one country into several bidding
  zones (Italy ~7, Sweden 4, Norway 5, Denmark 2) that would otherwise store an
  identical axis each. Note this does *not* merge countries with matching
  rules — Greece is `Europe/Athens`, Romania `Europe/Bucharest`, identical
  offsets but distinct tzdb entries. Collapsing those would mean keying on an
  offset signature, which breaks silently if tzdb diverges (e.g. if the EU
  abolishes seasonal clock changes and members choose differently).
- **Peak hours live on `dim_market`** (from `MarketConfig`), not on the shared
  axis — peak is a market convention, so bidding zones in one timezone may
  differ. `is_peak` is derived where it is used.
- **`spark.sql.session.timeZone` is pinned to UTC.** Left unset, the same Delta
  table reads back differently on a laptop in Bucharest and a cluster in UTC.

`shiden/timeaxis.py` holds the DST rules as pure stdlib code so they are
unit-tested in isolation; every processor derives its time columns from it.

> **Migration:** the Silver fact schemas and merge keys changed, so
> `data/delta/silver` and `data/delta/gold` must be rebuilt. Bronze is
> unaffected. The obsolete `silver/dim_time` table should be deleted.
>
> ```bash
> rm -rf data/delta/silver data/delta/gold
> uv run python -m shiden.backfill --stages silver,gold
> ```

## Testing

```bash
# Unit tests (no network, no live Spark cluster needed)
uv run pytest tests/unit

# Integration tests (live network + local Delta snapshot; excluded by default)
uv run pytest -m integration
```

## Linting & type-checking

```bash
uv run ruff check src tests
uv run mypy src
```

CI (`.github/workflows/ci.yml`) runs ruff, mypy, the unit tests, and a strict
docs build (`sphinx-build -W`) on every push/PR to `main`.

## API keys & usage metering

Every `/v1` endpoint requires an `X-API-Key` header. `/health` stays open so
uptime monitors can reach it.

```bash
# Issue a key scoped to one market
uv run python -m shiden.api.auth.cli issue --name "Acme Energy" --markets RO --rate-limit 120

# Issue a demo key with access to every market
uv run python -m shiden.api.auth.cli issue --name "Demo"

uv run python -m shiden.api.auth.cli list
uv run python -m shiden.api.auth.cli revoke shiden_live_a1b2c3d4
uv run python -m shiden.api.auth.cli usage --days 7
```

The secret is displayed **once**, at issuance — only a SHA-256 hash is stored,
so a leaked key store yields no usable credentials.

Keys are scoped to markets: requesting an unentitled market returns `403`.
Rate limits are per key, reported on every response via `X-RateLimit-Limit` /
`X-RateLimit-Remaining`, and return `429` with `Retry-After` when exceeded.

Every `/v1` request is recorded in `usage_events` (key, path, market, status,
latency) — including rejected ones, since sustained 429s and 403s identify
clients who have outgrown their plan.

For local work against throwaway data, set `API_AUTH_ENABLED=false` in `.env`.
Never do this on a reachable host.

> The key store is a SQLite file at `AUTH_DB_PATH` (default `./data/shiden_auth.db`).
> `data/` is gitignored. In production this file holds live credentials — put it
> on durable, backed-up storage, or migrate the schema to Postgres.

## API keys & usage metering

Every `/v1` endpoint requires an `X-API-Key` header. `/health` stays open so
uptime monitors can reach it.

```bash
# Issue a key scoped to one market
uv run python -m shiden.api.auth.cli issue --name "Acme Energy" --markets RO --rate-limit 120

# Issue a demo key with access to every market
uv run python -m shiden.api.auth.cli issue --name "Demo"

uv run python -m shiden.api.auth.cli list
uv run python -m shiden.api.auth.cli revoke shiden_live_a1b2c3d4
uv run python -m shiden.api.auth.cli usage --days 7
```

The secret is displayed **once**, at issuance — only a SHA-256 hash is stored,
so a leaked key store yields no usable credentials.

Keys are scoped to markets: requesting an unentitled market returns `403`.
Rate limits are per key, reported on every response via `X-RateLimit-Limit` /
`X-RateLimit-Remaining`, and return `429` with `Retry-After` when exceeded.

Every `/v1` request is recorded in `usage_events` (key, path, market, status,
latency) — including rejected ones, since sustained 429s and 403s identify
clients who have outgrown their plan.

For local work against throwaway data, set `API_AUTH_ENABLED=false` in `.env`.
Never do this on a reachable host.

> The key store is a SQLite file at `AUTH_DB_PATH` (default `./data/shiden_auth.db`).
> `data/` is gitignored. In production this file holds live credentials — put it
> on durable, backed-up storage, or migrate the schema to Postgres.

## API keys & usage metering

Every `/v1` endpoint requires an `X-API-Key` header. `/health` stays open so
uptime monitors can reach it.

```bash
# Issue a key scoped to one market
uv run python -m shiden.api.auth.cli issue --name "Acme Energy" --markets RO --rate-limit 120

# Issue a demo key with access to every market
uv run python -m shiden.api.auth.cli issue --name "Demo"

uv run python -m shiden.api.auth.cli list
uv run python -m shiden.api.auth.cli revoke shiden_live_a1b2c3d4
uv run python -m shiden.api.auth.cli usage --days 7
```

The secret is displayed **once**, at issuance — only a SHA-256 hash is stored,
so a leaked key store yields no usable credentials.

Keys are scoped to markets: requesting an unentitled market returns `403`.
Rate limits are per key, reported on every response via `X-RateLimit-Limit` /
`X-RateLimit-Remaining`, and return `429` with `Retry-After` when exceeded.

Every `/v1` request is recorded in `usage_events` (key, path, market, status,
latency) — including rejected ones, since sustained 429s and 403s identify
clients who have outgrown their plan.

For local work against throwaway data, set `API_AUTH_ENABLED=false` in `.env`.
Never do this on a reachable host.

> The key store is a SQLite file at `AUTH_DB_PATH` (default `./data/shiden_auth.db`).
> `data/` is gitignored. In production this file holds live credentials — put it
> on durable, backed-up storage, or migrate the schema to Postgres.

## API endpoints

All endpoints are versioned under `/v1/` and require an API key.

| Endpoint | Description | Status |
|---|---|---|
| `/v1/me` | Calling key's identity, entitlements and rate limit | Live |
| `/v1/prices/{market_id}` | Hourly prices (local + EUR + fx_rate) | Live |
| `/v1/generation/{market_id}` | Hourly generation mix | Live |
| `/v1/bess/{market_id}/arbitrage-windows` | Charge/discharge windows | Live |
| `/v1/bess/{market_id}/signals` | 15-min charge/idle/discharge signals | Live |
| `/v1/weather/{market_id}` | Weather data | Planned (501) |
| `/v1/bess/{market_id}/forecast` | Price forecast | Planned (501) |

## Repository structure

```
src/shiden/
├── api/            FastAPI app, routers, schemas
├── config/         Market registry + settings (env-based)
├── ingestion/       Source-specific ingesters (OPCOM, weather, BNR, ECB, ENTSO-E)
├── processing/
│   ├── bronze/     Landing → Bronze
│   ├── silver/     Bronze → Silver (Kimball star schema)
│   └── gold/       Silver → Gold (BESS signals, API-ready tables)
└── scheduler/      Job definitions + local APScheduler runner
docs/               Sphinx site: guides, architecture, diagrams, API reference
tests/
├── unit/           No network / no Spark required
└── integration/    Live network + local Delta snapshot
data/               Local Delta Lake storage (gitignored)
```

## Data sources

| Source | Data | Auth |
|---|---|---|
| OPCOM | Day-ahead market prices (Romania) | None |
| Open-Meteo | Hourly weather | None |
| BNR | EUR/RON exchange rates | None |
| ECB | EUR cross rates | None |
| ENTSO-E | Actual generation + load | API key required |

Details on schedules, endpoints, and Bronze table structure: [docs/architecture.md](docs/architecture.md#data-sources).
