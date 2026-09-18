# System diagrams

Every diagram on this page is a Mermaid source block, rendered client-side by
Sphinx and also by GitHub when the Markdown is read in the repo browser. They
are generated from text, so they diff like code and cannot drift out of a
review unnoticed the way an exported image does.

Table and job names below are the real ones on disk — `gold/bess_signals`,
`silver/fact_price`, `run_silver_daily` — not illustrative placeholders.

## System context

Where the data comes from, what transforms it, and what serves it.

```mermaid
flowchart LR
    subgraph ext["External sources"]
        direction TB
        OPCOM["OPCOM<br/><i>day-ahead prices</i><br/>no auth"]
        OM["Open-Meteo<br/><i>hourly weather</i><br/>no auth"]
        BNR["BNR<br/><i>EUR/RON</i><br/>no auth"]
        ECB["ECB<br/><i>EUR cross rates</i><br/>no auth"]
        ENTSOE["ENTSO-E<br/><i>generation + load</i><br/>API key"]
    end

    subgraph ing["shiden.ingestion"]
        ING["Ingesters<br/><i>fetch only, no transform</i>"]
    end

    subgraph proc["shiden.processing — PySpark + Delta Lake"]
        direction LR
        LAND["Landing<br/><i>immutable raw files</i>"]
        BRZ["Bronze<br/><i>typed, append-only</i>"]
        SLV["Silver<br/><i>Kimball star schema</i>"]
        GLD["Gold<br/><i>API-shaped tables</i>"]
        LAND --> BRZ --> SLV --> GLD
    end

    subgraph api["shiden.api — FastAPI"]
        RD["Routers<br/><i>delta-rs, no JVM</i>"]
        AUTH["Auth + metering<br/><i>SQLite key store</i>"]
    end

    CLIENT["BESS operators<br/>and aggregators"]

    SCHED["shiden.scheduler<br/><i>APScheduler local /<br/>Databricks Workflows prod</i>"]

    OPCOM --> ING
    OM --> ING
    BNR --> ING
    ECB --> ING
    ENTSOE --> ING
    ING --> LAND
    GLD --> RD
    AUTH -.->|"gates every /v1"| RD
    RD -->|"HTTPS + X-API-Key"| CLIENT
    SCHED -.->|"triggers"| ING
    SCHED -.->|"triggers"| BRZ
    SCHED -.->|"triggers"| SLV
    SCHED -.->|"triggers"| GLD
```

The API never opens a Spark session. It reads Gold through `delta-rs`, so a
request costs a Parquet scan rather than a JVM start-up — which is what makes
sub-second responses possible from a lakehouse.

## Data lineage

Every table that exists on disk, and what feeds it.

```mermaid
flowchart TB
    classDef src fill:#f4f4f5,stroke:#71717a,color:#18181b
    classDef bronze fill:#fdf0e3,stroke:#b45309,color:#451a03
    classDef silver fill:#eef2f7,stroke:#475569,color:#0f172a
    classDef gold fill:#fdf6d8,stroke:#a16207,color:#422006
    classDef api fill:#e7f5ee,stroke:#0b6e4f,color:#052e1c

    OPCOM["OPCOM CSV"]:::src
    OM["Open-Meteo JSON"]:::src
    BNRX["BNR XML"]:::src
    ECBX["ECB XML"]:::src
    ENT["ENTSO-E XML"]:::src

    B1["bronze/opcom_pzu_prices"]:::bronze
    B2["bronze/weather"]:::bronze
    B3["bronze/bnr_fx_rates"]:::bronze
    B4["bronze/ecb_fx_rates"]:::bronze
    B5["bronze/entsoe_generation"]:::bronze
    B6["bronze/entsoe_load"]:::bronze

    FX["silver/exchange_rates<br/><i>reconciled + forward-filled</i>"]:::silver
    FP["silver/fact_price<br/><i>15-min × market</i>"]:::silver
    FG["silver/fact_generation<br/><i>hourly × type × market</i>"]:::silver
    FL["silver/fact_load<br/><i>hourly × market</i>"]:::silver
    FW["silver/fact_weather<br/><i>hourly × location</i>"]:::silver

    DD["silver/dim_date"]:::silver
    DT["silver/dim_datetime"]:::silver
    DM["silver/dim_market"]:::silver
    DP["silver/dim_production_type"]:::silver
    DL["silver/dim_location"]:::silver

    G1["gold/price_hourly"]:::gold
    G2["gold/generation_hourly"]:::gold
    G3["gold/bess_signals<br/><i>15-min granularity</i>"]:::gold

    E1["/v1/prices"]:::api
    E2["/v1/generation"]:::api
    E3["/v1/bess/signals"]:::api
    E4["/v1/bess/arbitrage-windows"]:::api

    OPCOM --> B1 --> FP
    OM --> B2 --> FW
    BNRX --> B3 --> FX
    ECBX --> B4 --> FX
    ENT --> B5 --> FG
    ENT --> B6 --> FL

    FX --> FP
    FP --> G1
    FP --> G3
    FG --> G2

    G1 --> E1
    G2 --> E2
    G3 --> E3
    G3 --> E4
```

Two things this diagram is deliberately explicit about:

- **`gold/bess_signals` reads `silver/fact_price`, not `gold/price_hourly`.**
  Going back to the 15-minute Silver fact keeps the signals at the natural
  granularity of OPCOM day-ahead settlement. Deriving them from the hourly Gold
  table would throw away three quarters of the resolution the battery can
  actually trade on.
- **There is no `gold/arbitrage_windows` table.** Windows are contiguous runs
  of the same signal, computed per request in
  {py:func}`shiden.api.routers.bess.get_arbitrage_windows`. Materialising them
  would add a table that carries no information `gold/bess_signals` does not
  already have.

## Silver star schema

Conformed dimensions shared across fact tables, no snowflaking. Full column
reference: {doc}`silver_schema`.

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_PRICE : "date_id"
    DIM_DATE ||--o{ FACT_GENERATION : "date_id"
    DIM_DATE ||--o{ FACT_LOAD : "date_id"
    DIM_DATE ||--o{ FACT_WEATHER : "date_id"

    DIM_DATETIME ||--o{ FACT_PRICE : "timestamp_utc"
    DIM_DATETIME ||--o{ FACT_GENERATION : "hour_start_utc"
    DIM_DATETIME ||--o{ FACT_LOAD : "hour_start_utc"
    DIM_DATETIME ||--o{ FACT_WEATHER : "hour_start_utc"

    DIM_MARKET ||--o{ FACT_PRICE : "market_id"
    DIM_MARKET ||--o{ FACT_GENERATION : "market_id"
    DIM_MARKET ||--o{ FACT_LOAD : "market_id"

    DIM_PRODUCTION_TYPE ||--o{ FACT_GENERATION : "production_type_id"
    DIM_LOCATION ||--o{ FACT_WEATHER : "location_id"
    EXCHANGE_RATES ||--o{ FACT_PRICE : "date_id + currency"

    DIM_DATE {
        int date_id PK "YYYYMMDD"
        date full_date
        bool is_holiday
    }
    DIM_DATETIME {
        timestamp timestamp_utc PK "the only safe key"
        string timezone PK "IANA zone, not market"
        int interval_of_day "1-based, matches OPCOM"
        int local_time_id "0-95, NOT unique on fall-back"
        bool is_repeated_hour
    }
    DIM_MARKET {
        string market_id PK
        string bidding_zone "ENTSO-E EIC"
        string currency "settlement"
        int peak_start_hour "market convention"
    }
    DIM_PRODUCTION_TYPE {
        int production_type_id PK "stable, never renumbered"
        string entsoe_name
        bool is_renewable
    }
    DIM_LOCATION {
        int location_id PK
        float lat
        float lon
        float wind_weight
    }
    EXCHANGE_RATES {
        int date_id PK
        string currency_pair PK
        float rate
        string source "BNR|ECB|FORWARD_FILLED|SYNTHETIC"
    }
    FACT_PRICE {
        timestamp timestamp_utc PK
        string market_id PK
        float price_local_mwh
        float fx_rate "the rate actually applied"
        float price_eur_mwh
    }
    FACT_GENERATION {
        timestamp hour_start_utc PK
        string market_id PK
        int production_type_id PK
        float generation_mw
    }
    FACT_LOAD {
        timestamp hour_start_utc PK
        string market_id PK
        float load_mw
    }
    FACT_WEATHER {
        timestamp hour_start_utc PK
        int location_id PK
        float temperature_c
        float wind_speed_ms
    }
```

`dim_datetime` is keyed by **IANA timezone, not by market**. The interval axis
is a property of the zone, and European power markets routinely split one
country into several bidding zones — Italy has roughly seven, Sweden four,
Norway five, Denmark two — which would otherwise each store an identical axis.
Note this does *not* merge countries that happen to share DST rules: Greece is
`Europe/Athens` and Romania `Europe/Bucharest`, identical offsets today but
distinct tzdb entries. Collapsing them would mean keying on an offset
signature, which breaks silently the moment tzdb diverges.

## API request path

What actually happens between the client's `curl` and the bytes coming back.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant M as UsageMeteringMiddleware
    participant A as require_api_key
    participant E as require_market_access
    participant R as Router handler
    participant D as delta-rs
    participant S as SQLite key store

    C->>M: GET /v1/bess/RO/signals + X-API-Key
    activate M
    M->>M: start latency timer
    M->>A: forward request
    activate A
    A->>S: look up SHA-256 of key
    alt key unknown or revoked
        S-->>A: no match
        A-->>M: 401
    else key valid
        S-->>A: AuthenticatedKey
        A->>A: check rate-limit bucket
        alt over limit
            A-->>M: 429 + Retry-After
        else within limit
            A->>E: entitlements
            activate E
            alt market not on plan
                E-->>M: 403
            else entitled
                E->>R: proceed
                activate R
                R->>R: require_market → 404 if unknown
                R->>D: scan gold/bess_signals partition
                D-->>R: pandas DataFrame
                R->>R: build response models
                R-->>M: 200 + payload
                deactivate R
            end
            deactivate E
        end
    end
    deactivate A
    M->>M: stamp X-RateLimit-* headers
    M-)S: record usage_event off the event loop
    M-->>C: response
    deactivate M
```

The metering write is fire-and-forget through Starlette's threadpool. SQLite is
fast, but it is still blocking I/O, and telemetry must never add latency to the
signal the client actually came for. Rejected requests are recorded too — a
sustained run of `429`s or `403`s is the earliest evidence that a client has
outgrown its plan, and it is impossible to reconstruct after the fact.

## Daily pipeline schedule

All times Europe/Bucharest. Ordering is not arbitrary: each stage waits for the
sources it consumes to have landed.

```mermaid
gantt
    title Daily pipeline — Europe/Bucharest
    dateFormat HH:mm
    axisFormat %H:%M

    section Ingestion
    run_weather_daily      :w, 06:00, 30m
    run_entsoe_daily       :e, 08:00, 30m
    run_opcom_pzu_daily    :o, 14:00, 30m
    run_fx_rates_daily     :f, 17:00, 30m

    section Transform
    run_silver_daily       :s, 20:00, 45m
    run_gold_daily         :g, 21:00, 45m
```

Jobs are registered with `misfire_grace_time=3600` and `coalesce=True`: a run
missed by up to an hour executes late rather than being skipped, and a backlog
of missed runs collapses into a single execution instead of stampeding.

Each processor runs through `_run_safe`, which logs a failure as `ERROR` and
continues, so one broken source cannot abort the rest of the run. The OPCOM and
weather jobs refetch the last seven days on every run, so a transient upstream
outage repairs itself without operator action.

## Backfill decision path

The scheduler only ever fetches a window relative to *today*, so history goes
through {py:mod}`shiden.backfill`. This is the per-day logic that makes a run
resumable.

```mermaid
flowchart TB
    START(["shiden.backfill --from --to"]) --> PRE{"start date before<br/>2025-10-01 MTU transition?"}
    PRE -->|"yes, no --allow-pre-mtu"| ABORT["refuse up front"]
    PRE -->|"no"| DAY["for each day in range"]

    DAY --> EXISTS{"landing file<br/>already on disk?"}
    EXISTS -->|"yes, no --refetch"| SKIP["skip fetch<br/><i>this is what makes<br/>runs resumable</i>"]
    EXISTS -->|"no, or --refetch"| FETCH["fetch from source"]

    FETCH --> OK{"source served<br/>the day?"}
    OK -->|"no"| COLLECT["collect into<br/>failed-days report"]
    OK -->|"yes"| WRITE["write landing file"]

    SKIP --> BRONZE
    WRITE --> BRONZE["Bronze parse"]
    BRONZE --> MTU{"24 hourly intervals<br/>instead of 96?"}
    MTU -->|"yes"| REJECT["reject the day"]
    MTU -->|"no"| NEXT["Silver → Gold"]

    COLLECT --> REPORT
    REJECT --> REPORT
    NEXT --> REPORT(["report at end —<br/>rerun retries only<br/>the failed days"])
```

The pre-MTU check exists because the failure it prevents is silent, not loud.
OPCOM's pre-2025-10-01 exports contain 24 hourly intervals that are
*structurally identical* to the 96-interval quarter-hourly format. Ingesting
them would map each hour onto a single 15-minute slot and compress a day into
six hours — producing a table that loads cleanly, queries cleanly, and is
wrong. The Bronze parser therefore rejects such days regardless of flags;
`--allow-pre-mtu` only silences the up-front range check.

## Deployment topology

The same job functions run in both environments. That symmetry is the design
goal — there is no orchestration code to port.

```mermaid
flowchart LR
    subgraph local["Local — dev and market validation"]
        direction TB
        L1["Dev container<br/><i>Python + Java</i>"]
        L2["SparkSession local[*]"]
        L3["Delta → ./data/delta"]
        L4["APScheduler<br/><i>embedded</i>"]
        L5["Manual backfill script"]
    end

    subgraph dbx["Azure Databricks — production"]
        direction TB
        D1["Databricks cluster"]
        D2["SparkSession getOrCreate"]
        D3["Delta → ADLS Gen2"]
        D4["Databricks Workflows"]
        D5["Auto Loader<br/><i>landing → Bronze</i>"]
    end

    JOBS["shiden.scheduler.jobs<br/><i>same functions, both sides</i>"]

    L4 --> JOBS
    D4 --> JOBS

    local -.->|"ENVIRONMENT=databricks"| dbx
```

`get_spark()` in {py:mod}`shiden.processing.spark` returns the appropriate
session for the `ENVIRONMENT` setting; nothing above it needs to know which one
it got.
