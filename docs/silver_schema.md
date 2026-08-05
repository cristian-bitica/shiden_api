# Silver Layer — Kimball Star Schema

## Overview

The Silver layer is modelled as a **Kimball star schema**: conformed dimensions shared across
fact tables, no snowflaking, no surrogate keys beyond what Spark/Delta needs.

**Primary consumers:** Gold layer processors (arbitrage windows, BESS signals, price forecast)
and ML model training pipelines. No BI tool queries Silver directly in the current architecture.

**Temporal spine:** 15-minute intervals (OPCOM native, 96 per day). ENTSO-E generation,
load, and weather are hourly — they reference the hour-start interval in `dim_time` and are
joined to price facts at hour grain in the Gold layer.

---

## Dimensions

### `silver/dim_date`

One row per calendar day. Purely calendar-derived — FX rates live in
`silver/exchange_rates` (see below), keeping this conformed dimension
market-agnostic.

| Column | Type | Nullable | Notes |
|---|---|:---:|---|
| `date_id` | INT | ✗ | PK — YYYYMMDD integer (e.g. 20240115) |
| `full_date` | DATE | ✗ | |
| `year` | INT | ✗ | |
| `month` | INT | ✗ | 1–12 |
| `day_of_month` | INT | ✗ | |
| `day_of_week` | INT | ✗ | 1 = Monday … 7 = Sunday |
| `week_of_year` | INT | ✗ | ISO week |
| `quarter` | INT | ✗ | 1–4 |
| `is_weekday` | BOOLEAN | ✗ | FALSE on Saturday/Sunday |
| `is_ro_holiday` | BOOLEAN | ✗ | Romanian public holiday (incl. Jan 6+7 from 2024, Law 52/2023) |
| `season` | STRING | ✗ | `Winter` \| `Spring` \| `Summer` \| `Autumn` |

**Partition:** none — table is tiny (~365 rows/year).
**Natural key / MERGE key:** `date_id`.

---

### `silver/exchange_rates`

Unified, reconciled, gap-filled FX reference — one row per
(date, base_currency, quote_currency). Built by `ExchangeRatesProcessor`
from Bronze BNR/ECB per market config (`fx_source`): BNR authoritative for
RON with ECB fallback; ECB for other crosses; SYNTHETIC rate=1.0 for
EUR-denominated markets. Weekends/holidays are `FORWARD_FILLED` from the
last published rate, seeded with a 14-day lookback so processing windows
that start on a weekend still resolve.

| Column | Type | Nullable | Notes |
|---|---|:---:|---|
| `date` | DATE | ✗ | |
| `base_currency` | STRING | ✗ | reporting currency (`EUR`) |
| `quote_currency` | STRING | ✗ | market settlement currency (`RON`, …) |
| `rate` | DOUBLE | ✗ | 1 base = rate quote |
| `source` | STRING | ✗ | `BNR` \| `ECB` \| `SYNTHETIC` \| `FORWARD_FILLED` |
| `processed_at` | TIMESTAMP | ✗ | |

**Partition:** `(base_currency, quote_currency)`.
**Natural key / MERGE key:** `(date, base_currency, quote_currency)`.

---

### `silver/dim_time`

96 static rows representing every 15-minute interval within a day. Created once; never updated.
All four fact tables share this dimension — hourly facts reference only the 24 hour-start rows
(`is_hour_start = TRUE`).

| Column | Type | Nullable | Notes |
|---|---|:---:|---|
| `time_id` | INT | ✗ | PK — 0..95 (interval index within day) |
| `hour` | INT | ✗ | 0–23 |
| `quarter_of_hour` | INT | ✗ | 0–3 |
| `minute_of_day` | INT | ✗ | 0, 15, 30, … 1425 |
| `time_label` | STRING | ✗ | `"00:00"`, `"00:15"`, … `"23:45"` |
| `is_hour_start` | BOOLEAN | ✗ | TRUE for `time_id` ∈ {0, 4, 8, … 92} |
| `is_peak` | BOOLEAN | ✗ | Romanian market peak window — see note below |

**Peak definition:** `is_peak = TRUE` for `time_id` 32–79 inclusive (08:00–19:45 local time),
matching OPCOM's official peak window of 08:00–20:00. The flag is purely time-based — it does
not filter for weekdays or exclude holidays. Weekday and holiday filtering is a Gold-layer
responsibility. The exact boundary should be confirmed against OPCOM's published tariff
schedule before using this flag in settlement calculations.

**Partition:** none — table is 96 rows.
**Natural key / MERGE key:** `time_id`.

---

### `silver/dim_market`

One row per market. Expandable to Germany (`10Y1001A1001A83F`), GB (`10YGB----------A`), etc.
with zero code changes (add a row; processing logic is already parameterised on `market_id`).

| Column | Type | Nullable | Notes |
|---|---|:---:|---|
| `market_id` | STRING | ✗ | PK — `"RO"`, `"DE"`, `"GB"`, … |
| `market_name` | STRING | ✗ | Human-readable name |
| `bidding_zone` | STRING | ✗ | ENTSO-E EIC code |
| `timezone` | STRING | ✗ | zoneinfo string, e.g. `"Europe/Bucharest"` |
| `currency` | STRING | ✗ | Local trading currency (`"RON"`, `"EUR"`, `"GBP"`) |
| `reporting_currency` | STRING | ✗ | Always `"EUR"` for MVP |

**Partition:** none — O(10) rows at full scale.
**Natural key / MERGE key:** `market_id`.

---

### `silver/dim_production_type`

Semi-static mapping from ENTSO-E production type strings to business-meaningful categories.
Updated only when ENTSO-E adds a new type (rare). The surrogate key insulates downstream tables
from ENTSO-E name changes.

| Column | Type | Nullable | Notes |
|---|---|:---:|---|
| `production_type_id` | INT | ✗ | PK — surrogate (sequential) |
| `production_type_name` | STRING | ✗ | ENTSO-E string, e.g. `"Wind Onshore"`, `"Hydro Pumped Storage\|Actual Aggregated"` |
| `energy_category` | STRING | ✗ | `Renewable` \| `Nuclear` \| `Thermal` \| `Storage` \| `Other` |
| `is_renewable` | BOOLEAN | ✗ | TRUE for wind, solar, run-of-river hydro |
| `is_variable` | BOOLEAN | ✗ | TRUE for wind and solar (intermittent, weather-driven) |
| `is_dispatchable` | BOOLEAN | ✗ | TRUE for gas, coal, nuclear, pumped storage |

**Key Romania types:**

| `production_type_name` | `energy_category` | `is_renewable` | `is_variable` | `is_dispatchable` |
|---|---|:---:|:---:|:---:|
| Hydro Run-of-river and Poundage | Renewable | TRUE | FALSE | FALSE |
| Wind Onshore | Renewable | TRUE | TRUE | FALSE |
| Nuclear | Nuclear | FALSE | FALSE | TRUE |
| Fossil Gas | Thermal | FALSE | FALSE | TRUE |
| Hydro Pumped Storage\|Actual Aggregated | Storage | FALSE | FALSE | TRUE |
| Solar | Renewable | TRUE | TRUE | FALSE |
| Fossil Hard coal | Thermal | FALSE | FALSE | TRUE |

**Partition:** none.
**Natural key / MERGE key:** `production_type_name`.

---

### `silver/dim_location`

Weather measurement points. Covers the six Romania locations defined in `config/markets.py`.

| Column | Type | Nullable | Notes |
|---|---|:---:|---|
| `location_id` | INT | ✗ | PK — surrogate |
| `location_name` | STRING | ✗ | e.g. `"Dobrogea"` |
| `market_id` | STRING | ✗ | FK → `dim_market` |
| `latitude` | DOUBLE | ✗ | |
| `longitude` | DOUBLE | ✗ | |
| `wind_weight` | DOUBLE | ✗ | Aggregation weight for national wind signal |
| `solar_weight` | DOUBLE | ✗ | Aggregation weight for national solar signal |
| `demand_weight` | DOUBLE | ✗ | Aggregation weight for national demand proxy |

**Partition:** none.
**Natural key / MERGE key:** `(market_id, location_name)`.

---

## Fact Tables

### `silver/fact_price`

Grain: one row per **15-minute delivery interval × market**.
~35,000 rows per market-year.

| Column | Type | Nullable | Notes |
|---|---|:---:|---|
| `date_id` | INT | ✗ | FK → `dim_date` |
| `time_id` | INT | ✗ | FK → `dim_time`, range 0–95 |
| `market_id` | STRING | ✗ | FK → `dim_market` |
| `price_local_mwh` | DOUBLE | ✓ | Settlement price in the market's local currency per MWh (currency in `dim_market`) |
| `fx_rate` | DOUBLE | ✓ | EUR→local rate applied, from `silver/exchange_rates` (already forward-filled) |
| `price_eur_mwh` | DOUBLE | ✓ | Derived: `price_local_mwh / fx_rate`; NULL only before FX history begins |

**Currency note:** Market-agnostic — the local price plus the exact rate used
are stored, so RON (or GBP, …) can be recomputed at query time and EUR
conversions are auditable. Because `silver/exchange_rates` is forward-filled
(with a lookback seed across processing-window edges), weekend/holiday EUR
prices are populated here, not deferred to Gold.

**Partition:** `(market_id, date_id)`.
**Natural key / MERGE key:** `(date_id, time_id, market_id)`.

---

### `silver/fact_generation`

Grain: one row per **1-hour interval × production_type × market**.
~131,000 rows per market-year (24 hours × 365 days × ~15 production types for Romania).
Long format matches the Bronze source; wide pivoted materialization is deferred to the Gold layer.

| Column | Type | Nullable | Notes |
|---|---|:---:|---|
| `date_id` | INT | ✗ | FK → `dim_date` |
| `time_id` | INT | ✗ | FK → `dim_time`; **always an hour-start value** (0, 4, 8, … 92) |
| `market_id` | STRING | ✗ | FK → `dim_market` |
| `production_type_id` | INT | ✗ | FK → `dim_production_type` |
| `actual_mw` | DOUBLE | ✓ | NULL when ENTSO-E reports no data for the interval |

**Partition:** `(market_id, date_id)`.
**Natural key / MERGE key:** `(date_id, time_id, market_id, production_type_id)`.

---

### `silver/fact_load`

Grain: one row per **1-hour interval × market**.
~8,760 rows per market-year.

| Column | Type | Nullable | Notes |
|---|---|:---:|---|
| `date_id` | INT | ✗ | FK → `dim_date` |
| `time_id` | INT | ✗ | FK → `dim_time`; hour-start values only |
| `market_id` | STRING | ✗ | FK → `dim_market` |
| `actual_load_mw` | DOUBLE | ✓ | NULL when ENTSO-E reports no data |

**Partition:** `(market_id, date_id)`.
**Natural key / MERGE key:** `(date_id, time_id, market_id)`.

---

### `silver/fact_weather`

Grain: one row per **1-hour interval × location × data_type**.
~52,560 rows per Romania-year for a single data_type (24 × 365 × 6 locations); up to doubled
during the REALIZED/FORECAST overlap window when both types coexist for the same interval.

| Column | Type | Nullable | Notes |
|---|---|:---:|---|
| `date_id` | INT | ✗ | FK → `dim_date` |
| `time_id` | INT | ✗ | FK → `dim_time`; hour-start values only |
| `location_id` | INT | ✗ | FK → `dim_location` |
| `data_type` | STRING | ✗ | `REALIZED` \| `FORECAST` — inherited from Bronze |
| `temperature_c` | DOUBLE | ✓ | 2 m air temperature |
| `wind_speed_ms` | DOUBLE | ✓ | 100 m wind speed (turbine hub height) |
| `solar_irradiance_wm2` | DOUBLE | ✓ | Direct normal irradiance |
| `cloud_cover_pct` | DOUBLE | ✓ | Total cloud cover 0–100 |

**Partition:** `(location_id, date_id)`.
**Natural key / MERGE key:** `(date_id, time_id, location_id, data_type)`.

---

## Bus Matrix

Shows which dimensions are shared (conformed) across fact tables.

| Dimension | fact_price | fact_generation | fact_load | fact_weather |
|---|:---:|:---:|:---:|:---:|
| `dim_date` | ✓ | ✓ | ✓ | ✓ |
| `dim_time` | ✓ (15-min) | ✓ (hour) | ✓ (hour) | ✓ (hour) |
| `dim_market` | ✓ | ✓ | ✓ | — |
| `dim_production_type` | — | ✓ | — | — |
| `dim_location` | — | — | — | ✓ |

`dim_date` and `dim_time` are fully conformed — every fact table uses them with the same
natural keys. `dim_market` is conformed across the three market-aware facts.

---

## Cross-Grain Join Pattern

The granularity mismatch (15-min prices vs. hourly generation/load/weather) resolves through
`dim_time`. The hour-start `time_id` for any 15-min interval is `floor(time_id / 4) * 4`.

Typical Gold join — average hourly price vs. renewable generation share:

```sql
SELECT
    d.full_date,
    t.hour,
    AVG(p.price_eur_mwh)                                        AS avg_price_eur_mwh,
    SUM(CASE WHEN pt.is_renewable THEN g.actual_mw ELSE 0 END)
        / NULLIF(SUM(g.actual_mw), 0)                           AS renewable_share,
    l.actual_load_mw
FROM silver.fact_price p
JOIN silver.dim_date d       ON p.date_id   = d.date_id
JOIN silver.dim_time t       ON p.time_id   = t.time_id
JOIN silver.fact_generation g
    ON  g.date_id   = p.date_id
    AND g.market_id = p.market_id
    AND g.time_id   = (t.hour * 4)          -- maps to hour-start time_id
JOIN silver.dim_production_type pt ON g.production_type_id = pt.production_type_id
LEFT JOIN silver.fact_load l
    ON  l.date_id   = p.date_id
    AND l.market_id = p.market_id
    AND l.time_id   = (t.hour * 4)
WHERE p.market_id = 'RO'
  AND t.is_hour_start = TRUE                -- aggregate at hourly grain
GROUP BY d.full_date, t.hour, l.actual_load_mw
```

---

## Physical Design Notes

**Delta Lake partitioning strategy**

| Table | Partition columns | Rationale |
|---|---|---|
| `fact_price` | `(market_id, date_id)` | Typical query filters on market + date range |
| `fact_generation` | `(market_id, date_id)` | Same access pattern |
| `fact_load` | `(market_id, date_id)` | Same |
| `fact_weather` | `(location_id, date_id)` | Weather queries filter by location + date |
| `dim_*` | none | All dimension tables are small enough to broadcast-join |

**MERGE idempotency**

Silver processors use `MERGE` (update-or-insert via
`shiden.processing.delta_io.merge_upsert`) on the natural keys listed above.
Re-running a Silver job for the same date range is safe, produces no
duplicates, and picks up any Bronze revisions (Bronze is append-only with
revision capture; Silver reads the latest observation per key by
`ingested_at`).

**FX forward-fill**

Handled inside `silver/exchange_rates`: weekends and holidays carry the last
published rate with `source = FORWARD_FILLED`, and the fill is seeded with a
14-day lookback before the processing window so a window starting on a
weekend still resolves. Fact tables and Gold consume filled rates — no layer
downstream needs its own fill logic.

---

## What Silver Does Not Do

The following belong to the **Gold layer**, not Silver:

- Pivoting generation from long to wide format (`fact_generation` stays long)
- Computing arbitrage spread between intervals
- Aggregating weather locations into a single national signal (weighted average)
- Forward-filling FX rate gaps for EUR conversion in derived metrics
- Classifying BESS charge/discharge windows
- Price forecasting

Silver's contract: clean, typed, deduplicated, conformed facts and dimensions. No business
logic beyond type casting, peak flag assignment, EUR price conversion, and dimension lookups.
