# Command-line reference

Four entry points, all invoked as modules so they pick up the project
environment via `uv run`.

## `shiden.backfill`

Backfill historical data across Bronze, Silver and Gold.

```bash
uv run python -m shiden.backfill [options]
```

| Flag | Default | Description |
|---|---|---|
| `--from DATE` | `2025-10-01` | Start date. The default is the 15-min MTU transition |
| `--to DATE` | yesterday | End date, inclusive |
| `--market ID` | `RO` | Market id |
| `--sources LIST` | `opcom,fx` | Comma-separated subset of `opcom`, `fx` |
| `--stages LIST` | all | Comma-separated subset of `landing`, `bronze`, `silver`, `gold` |
| `--refetch` | off | Refetch landing files that already exist. Without it, runs resume |
| `--allow-pre-mtu` | off | Permit a start date before the MTU transition. The Bronze parser still rejects hourly exports per-day |
| `--dry-run` | off | Print the plan and exit |

See {doc}`../operations` for worked examples and the pre-MTU warning.

## `shiden.scheduler.runner`

Start the local APScheduler process running the daily pipeline on
Europe/Bucharest time. Takes no arguments; blocks until interrupted.

```bash
uv run python -m shiden.scheduler.runner
```

## `shiden.scheduler.jobs`

Run the daily pipeline once, outside the scheduler: the full sequence by
default, or a single named job with `--job`.

```bash
uv run python -m shiden.scheduler.jobs [--job NAME]
```

| Flag | Default | Description |
|---|---|---|
| `--job NAME` | none — runs the full pipeline | Run exactly one job once, then exit. One of `run_opcom_pzu_daily`, `run_weather_daily`, `run_entsoe_daily`, `run_fx_rates_daily`, `run_silver_daily`, `run_gold_daily` |

Each job logs a failing step as an `ERROR` and continues rather than
raising, so the process exits `0` even when a step fails partway through.

## `shiden.api.auth.cli`

Manage API keys and read usage.

```bash
uv run python -m shiden.api.auth.cli [--db PATH] <command>
```

`--db` overrides the key store path (default: `AUTH_DB_PATH`, `./data/shiden_auth.db`).

### `issue`

Mint a new API key. The secret is printed **once** — only its SHA-256 hash is
stored, and there is no recovery path.

| Flag | Default | Description |
|---|---|---|
| `--name` | *required* | Client name, e.g. `"Acme Energy"` |
| `--markets` | all markets | Comma-separated market entitlements |
| `--rate-limit` | `60` | Requests per minute |

```bash
uv run python -m shiden.api.auth.cli issue --name "Acme Energy" --markets RO --rate-limit 120
```

### `list`

List issued keys with their entitlements, rate limits and status.

### `revoke`

```bash
uv run python -m shiden.api.auth.cli revoke shiden_live_a1b2c3d4
```

Takes a key id or display prefix. Revocation is immediate — the store is
consulted per request.

### `usage`

Per-key usage summary from the `usage_events` ledger.

| Flag | Default | Description |
|---|---|---|
| `--days` | `30` | Lookback window |

Rejected requests are included: sustained `429`s and `403`s identify clients
who have outgrown their plan.
