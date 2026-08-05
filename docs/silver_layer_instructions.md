# Task 2: Silver Layer — Agent Build Instructions

## Context

You are building the Silver layer for the **Shiden** energy data API.
Project: PySpark + Delta Lake (Medallion Bronze/Silver/Gold), FastAPI, targeting BESS operators in Romania.

Bronze is complete:
- `bronze/opcom_pzu_prices` — 96 x 15-min DA prices + volumes per delivery day, partitioned by `market_id / delivery_date`
- Weather Bronze does not yet exist (Task 2b — see below)

Silver's job: clean, standardise, enrich, and join the Bronze sources into a single
analyst-ready table. No business logic yet — that is Gold's job.

---

## Step 0 — Explore Bronze before writing any code

**Do this first.** Spin up a local SparkSession (use `get_spark()`) and run:

```python
from shiden.processing.spark import get_spark
from shiden.config.settings import settings

spark = get_spark()
df = spark.read.format("delta").load(f"{settings.delta_base_path}/bronze/opcom_pzu_prices")
df.printSchema()
df.show(5)
df.describe().show()

# Check for nulls
from pyspark.sql.functions import col, sum as spark_sum
df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

# Price distribution
df.select("price_lei_mwh").summary("min","25%","50%","75%","max").show()

# Confirm 96 intervals per day
from pyspark.sql.functions import count
df.groupBy("delivery_date").agg(count("*").alias("n")).orderBy("delivery_date").show(10)
```

Capture the actual schema, null counts, and price range before designing Silver.
Only proceed once you have real numbers.

---

## Task 2a — Silver: OPCOM PZU prices

### What Silver adds on top of Bronze

| Concern | Bronze | Silver |
|---|---|---|
| Price currency | Lei/MWh (raw) | Lei/MWh + EUR/MWh (converted) |
| Time representation | interval_15min (1–96) | interval_start_local (timestamp, Europe/Bucharest) |
| Period classification | none | is_peak, is_off_peak, hour_of_day, quarter_of_hour |
| Data quality | raw, may have nulls | nulls flagged, outliers annotated |
| Volume | raw MW | unchanged |

### Silver table: `silver/opcom_pzu_prices`

Partition by: `market_id`, `delivery_date`

**Schema:**

```
market_id              STRING       NOT NULL
delivery_date          DATE         NOT NULL
interval_15min         INTEGER      NOT NULL   -- 1–96, preserved from Bronze
interval_start_utc     TIMESTAMP    NOT NULL   -- start of 15-min slot in UTC
interval_start_local   TIMESTAMP    NOT NULL   -- start of 15-min slot in Europe/Bucharest
hour_of_day            INTEGER      NOT NULL   -- 0–23
quarter_of_hour        INTEGER      NOT NULL   -- 0–3 (within the hour)
is_peak                BOOLEAN      NOT NULL   -- intervals 33–80 (08:00–20:00 local)
price_lei_mwh          DOUBLE                 -- from Bronze, may be null
price_eur_mwh          DOUBLE                 -- price_lei_mwh / eur_ron_rate, may be null
volume_mw              DOUBLE
volume_buy_mw          DOUBLE
volume_sell_mw         DOUBLE
buy_sell_ratio         DOUBLE                 -- volume_buy_mw / volume_sell_mw, null-safe
is_price_outlier       BOOLEAN      NOT NULL   -- true if price > mean + 3*stddev for that date
bronze_ingested_at     TIMESTAMP              -- passthrough from Bronze for lineage
silver_processed_at    TIMESTAMP    NOT NULL   -- when Silver wrote this row
```

### EUR/RON conversion

Use a **static daily rate** for MVP — do not call a live FX API yet.
Add `eur_ron_rate: float = 4.97` to `Settings` in `config/settings.py`.
This can be replaced with a live feed in a later task.

### Peak definition

OPCOM defines:
- **Peak**: intervals 33–80 (08:00–20:00 local time, Mon–Fri)
- **Off-peak**: intervals 1–32 and 81–96

For MVP simplify to hour-of-day only (ignore weekday): `is_peak = (hour_of_day >= 8) AND (hour_of_day < 20)`.

### Interval → timestamp conversion

interval_15min runs 1–96, starting at midnight local time:
```
interval_start_local = midnight(delivery_date, Europe/Bucharest) + (interval_15min - 1) * 15 minutes
interval_start_utc   = interval_start_local converted to UTC
```

Use PySpark:
```python
from pyspark.sql.functions import expr, to_utc_timestamp, from_utc_timestamp

df = df.withColumn(
    "interval_start_local",
    expr("delivery_date + make_interval(0,0,0, (interval_15min-1)*15/60, (interval_15min-1)*15%60, 0)")
    # or: timestamp arithmetic via unix_timestamp
)
```

### Write pattern

Use Delta MERGE on `(market_id, delivery_date, interval_15min)` — same upsert pattern as Bronze.

---

## Task 2b — Bronze: Weather ingestion (prerequisite for Silver join)

Before building the weather Silver, you need weather Bronze. Build it now as part of this task.

### Ingester: `src/shiden/ingestion/weather.py` (already stubbed — implement it)

Open-Meteo endpoint: `https://archive-api.open-meteo.com/v1/archive` (historical)
or `https://api.open-meteo.com/v1/forecast` (future/current)

For Romania, fetch all three configured locations (from `markets.py`):
- Bucharest (44.4268, 26.1025)
- Dobrogea (44.5000, 28.8000)
- Oltenia (44.3000, 23.8000)

Hourly variables to request: `temperature_2m, wind_speed_10m, wind_speed_100m, shortwave_radiation, cloud_cover`

No API key needed. Timezone: `Europe/Bucharest`.

### Bronze table: `bronze/weather`

Partition by: `market_id`, `location_name`, `date`

```
market_id          STRING    NOT NULL
location_name      STRING    NOT NULL   -- "Bucharest", "Dobrogea", "Oltenia"
lat                DOUBLE    NOT NULL
lon                DOUBLE    NOT NULL
timestamp_local    TIMESTAMP NOT NULL   -- hourly, Europe/Bucharest
timestamp_utc      TIMESTAMP NOT NULL
temperature_2m     DOUBLE               -- °C
wind_speed_10m     DOUBLE               -- km/h
wind_speed_100m    DOUBLE               -- km/h
shortwave_radiation DOUBLE              -- W/m²
cloud_cover        DOUBLE               -- %
ingested_at        TIMESTAMP NOT NULL
source_url         STRING
```

---

## Task 2c — Silver: Joined prices + weather

Once both Bronze tables exist, build the joined Silver table.

### Silver table: `silver/prices_weather`

Join: `silver/opcom_pzu_prices` LEFT JOIN `bronze/weather`
On: `market_id`, `delivery_date`, `hour_of_day`
(weather is hourly; prices are 15-min — join on the hour bucket, not exact timestamp)

**Add these columns from weather (averaged across 3 RO locations):**

```
temp_avg_c             DOUBLE    -- avg temperature_2m across RO locations
wind_speed_10m_avg     DOUBLE    -- avg wind_speed_10m
wind_speed_100m_avg    DOUBLE    -- avg wind_speed_100m (better proxy for turbines)
solar_radiation_avg    DOUBLE    -- avg shortwave_radiation
cloud_cover_avg        DOUBLE    -- avg cloud_cover
```

Aggregate the 3 locations to a single RO value per hour before joining.
Also preserve individual location columns if needed for Gold.

### This table is the primary input for Gold

Gold (arbitrage windows, BESS signals, price forecast) reads from `silver/prices_weather`.
Do not write business logic in Silver — just clean, join, and expose.

---

## Files to Create

```
src/shiden/processing/silver/__init__.py     (exists, empty)
src/shiden/processing/silver/opcom_pzu.py   — Silver processor for OPCOM prices
src/shiden/processing/silver/weather.py     — Silver processor for weather (from Bronze)
src/shiden/processing/silver/joined.py      — Silver join: prices + weather
tests/unit/test_silver_opcom.py             — unit tests for interval→timestamp + peak flag
```

Update `src/shiden/scheduler/jobs.py` to add Silver steps after Bronze.

---

## Definition of Done

- [ ] `silver/opcom_pzu_prices` table exists with correct schema after running Silver processor
- [ ] `interval_start_utc` is correct for DST boundary dates (clocks change last Sunday of March/October in Romania)
- [ ] `price_eur_mwh` is null when `price_lei_mwh` is null, not zero
- [ ] `is_peak` is correct for intervals 33 and 80 (boundary check)
- [ ] `silver/prices_weather` join has no row explosion (LEFT JOIN, not INNER)
- [ ] All Silver processors use Delta MERGE — re-running is idempotent
- [ ] Unit tests cover: interval→timestamp, is_peak, EUR conversion, null handling
- [ ] `python -c "from shiden.processing.silver.opcom_pzu import OpcomPzuSilverProcessor"` works cleanly
