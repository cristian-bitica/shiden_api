# Using the API

The HTTP API is a thin read layer over the Gold tables. Handlers are
deliberately synchronous (`def`, not `async def`) — `delta-rs` and pandas calls
block, and FastAPI runs sync handlers in its threadpool rather than stalling
the event loop. No Spark session is involved in serving a request.

Interactive OpenAPI documentation is generated from the same code at `/docs`.

The full machine-readable contract is regenerated from the FastAPI app on every
docs build and shipped alongside this page:
[`openapi.json`](_static/openapi.json). Point a client generator at it rather
than transcribing the tables below — they are prose for humans, and the spec is
the authority.

## Authentication

Every `/v1` endpoint requires an API key in the `X-API-Key` header. `/health`
stays open so uptime monitors can reach it.

```bash
curl -H "X-API-Key: shiden_live_..." \
  "https://api.shiden.io/v1/bess/RO/signals?date=2026-08-01"
```

Keys are minted with the CLI (see {doc}`reference/cli`):

```bash
uv run python -m shiden.api.auth.cli issue --name "Acme Energy" --markets RO --rate-limit 120
```

The secret is displayed **once, at issuance** — only a SHA-256 hash is
persisted, so a leaked key store yields no usable credentials. There is no
recovery path for a lost key; issue a new one and revoke the old.

`GET /v1/me` is the recommended first call when integrating. It confirms the
key works and reports what it is entitled to, so a client never has to guess:

```json
{
  "name": "Acme Energy",
  "key": "shiden_live_a1b2c3d4",
  "environment": "live",
  "markets": ["RO"],
  "rate_limit_per_min": 120
}
```

### Entitlements

Keys are scoped to markets. Entitlement is enforced once, as a router-level
dependency in {py:mod}`shiden.api.main`, rather than re-declared on each
handler — every `/v1` route carries `{market_id}`, so a per-handler check is
duplication waiting to be forgotten on the next endpoint.

Requesting a market outside the key's scope returns `403`.

### Rate limits

Limits are per key, per minute. Every response carries:

| Header | Meaning |
|---|---|
| `X-RateLimit-Limit` | The key's requests-per-minute ceiling |
| `X-RateLimit-Remaining` | Requests left in the current window |
| `Retry-After` | *(on `429` only)* seconds to wait before retrying |

Back off for `Retry-After` seconds rather than retrying immediately.

### Local development

Set `API_AUTH_ENABLED=false` in `.env` to bypass authentication entirely.

:::{danger}
Only do this against throwaway data on a host nobody else can reach. There is
no partial mode — disabling auth also disables entitlement checks and rate
limiting.
:::

## Usage metering

Every `/v1` request is recorded in `usage_events` (key, path, market, status,
latency) by {py:class}`shiden.api.middleware.UsageMeteringMiddleware`, including
rejected ones. Sustained `429`s and `403`s are the earliest signal that a client
has outgrown its plan — or that a key is being abused — which is exactly the
telemetry that is impossible to reconstruct after the fact.

Writes happen off the event loop via the Starlette threadpool: SQLite is fast,
but it is still blocking I/O, and metering must never add latency to the signal
the client actually came for.

```bash
uv run python -m shiden.api.auth.cli usage --days 7
```

## Endpoints

All data endpoints are versioned under `/v1/` and take a `market_id` path
parameter (`RO` today).

| Endpoint | Description | Status |
|---|---|---|
| `GET /health` | Liveness probe, unauthenticated | Live |
| `GET /v1/me` | Calling key's identity, entitlements and rate limit | Live |
| `GET /v1/prices/{market_id}` | Hourly day-ahead prices (local + EUR + `fx_rate`) | Live |
| `GET /v1/generation/{market_id}` | Hourly generation mix | Live |
| `GET /v1/bess/{market_id}/signals` | 15-min charge/idle/discharge signals | Live |
| `GET /v1/bess/{market_id}/arbitrage-windows` | Contiguous charge/discharge windows | Live |
| `GET /v1/weather/{market_id}` | Weather data | Planned (`501`) |
| `GET /v1/bess/{market_id}/forecast` | Weather-driven price forecast | Planned (`501`) |

### Prices

```
GET /v1/prices/{market_id}?start=YYYY-MM-DD&end=YYYY-MM-DD
```

Hourly day-ahead prices for an inclusive date range. EUR prices use the
reconciled, forward-filled rate from `silver/exchange_rates`; the rate actually
applied is returned on each interval as `fx_rate`, so a client can reproduce
the conversion rather than trust it.

If nothing has been ingested for the requested range, `intervals` is empty —
an empty range is not an error.

Each interval: `date_id`, `time_id`, `time_label`, `price_local_mwh`,
`fx_rate`, `price_eur_mwh`, `is_peak`. The response carries the settlement
`currency` (e.g. `RON`) that `price_local_mwh` is denominated in.

### Generation

```
GET /v1/generation/{market_id}?start=YYYY-MM-DD&end=YYYY-MM-DD
```

Hourly generation mix from ENTSO-E: `total_mw`, `renewable_mw`, `thermal_mw`,
`nuclear_mw_total`, `renewable_share_pct`, plus a `sources` map of per-production-type
values.

### BESS signals

```
GET /v1/bess/{market_id}/signals?date=YYYY-MM-DD
```

The full 96-slot (15-minute) signal list for one delivery day:

| `signal` | Meaning |
|---|---|
| `-1` | **charge** — one of the day's cheapest N intervals |
| `0` | **idle** |
| `+1` | **discharge** — one of the day's most expensive N intervals |

Signals are rule-based and deterministic (no ML): intervals are ranked by EUR
price within the calendar day, and the cheapest / most expensive N are marked.
Defaults are 8 charge and 8 discharge slots — two hours each. Each interval also
carries `price_rank_asc` / `price_rank_desc`, the day's min and max EUR price,
and the day-level `arbitrage_spread_eur_mwh`.

Intervals with no EUR price are never ranked. If a day has no EUR prices at all
— Silver `fact_price` missing, or the date precedes all FX history — every row
for that day returns `signal=0` with null ranks, rather than a fabricated
signal.

### Arbitrage windows

```
GET /v1/bess/{market_id}/arbitrage-windows?date=YYYY-MM-DD
```

The same signals, collapsed into contiguous runs. Each window reports its slot
range, clock labels, `avg_price_eur_mwh` and `n_slots`.

A single economic "optimal window" may come back as several non-contiguous
windows — the cheapest charge slots are not required to be adjacent, and
pretending otherwise would misstate what the battery can actually capture.

## Error semantics

| Status | Cause |
|---|---|
| `401` | Missing or unknown `X-API-Key` |
| `403` | Key is valid but not entitled to the requested market |
| `404` | Unknown `market_id` — the detail lists the available markets |
| `422` | Invalid parameters, including an inverted range (`end < start`) |
| `429` | Rate limit exceeded — honour `Retry-After` |
| `501` | Endpoint is planned but not implemented |

Absence of data is **not** an error: a valid request for a range with no
ingested data returns `200` with an empty `intervals` list.
