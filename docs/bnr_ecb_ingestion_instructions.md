# Task 3: BNR + ECB FX Rate Ingesters — Agent Build Instructions

## Context

You are building the exchange rate ingestion pipeline for **Shiden**, an energy market data API.
Stack: PySpark 3.5, Delta Lake, FastAPI, Python 3.10+.
Architecture: Medallion — Landing Zone → Bronze → Silver → Gold.

This task produces **four new files**:
- `src/shiden/ingestion/bnr.py` — BNR ingester (landing zone writer)
- `src/shiden/ingestion/ecb.py` — ECB ingester (landing zone writer)
- `src/shiden/processing/bronze/bnr.py` — BNR Bronze Delta writer
- `src/shiden/processing/bronze/ecb.py` — ECB Bronze Delta writer

And extends one existing file:
- `src/shiden/scheduler/jobs.py` — add `run_fx_rates_daily()`

---

## Why Two Sources

BNR (National Bank of Romania) and ECB (European Central Bank) serve different roles:

| Property | BNR | ECB |
|---|---|---|
| Primary use | EUR/RON for RO market prices | EUR crosses for future non-RON markets (GBP, USD, etc.) |
| Format | Daily XML, one Cube per file | Historical XML, all dates in one file (~5 MB) |
| Publish schedule | Business days ~13:00 Bucharest | Business days ~16:00 Frankfurt |
| Coverage | ~30 currencies vs RON | ~40 EUR crosses |
| Authority | Romanian contracts (legally authoritative) | International reference |

Silver will reconcile them into `silver/exchange_rates`. This task only builds Bronze landing writers — no Silver work here.

---

## Step 0 — Fetch and inspect both XML sources before writing any code

```python
import httpx

# BNR — today's rates
r = httpx.get("https://www.bnr.ro/nbrfxrates.xml", timeout=15)
print(r.text[:2000])

# BNR — archive (any past year)
r2 = httpx.get("https://www.bnr.ro/files/xml/years/nbrfxrates2024.xml", timeout=30)
print(r2.text[:2000])

# ECB — full history in one file
r3 = httpx.get("https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml", timeout=60)
print(r3.text[:3000])
print("ECB file size (bytes):", len(r3.content))
```

**Verify and note:**
- Exact XML namespace URIs for both
- Whether BNR uses a `multiplier` attribute on any `<Rate>` elements
- Whether ECB Cube nesting is `<Cube> → <Cube time="..."> → <Cube currency="..." rate="..."/>`
- How many days of history the ECB file contains
- Confirm BNR rate for EUR is `1 EUR = X RON` (not inverse)

If either URL returns an error or unexpected format, **stop and report** before proceeding.

---

## BNR Ingester

### File: `src/shiden/ingestion/bnr.py`

**Responsibility:** Fetch BNR XML to the landing zone. No parsing, no transformation.

#### URLs

```
Today (or most recent):  https://www.bnr.ro/nbrfxrates.xml
Archive by year:         https://www.bnr.ro/files/xml/years/nbrfxrates{YYYY}.xml
```

The archive URL contains all business days for a given year in one file — multiple `<Cube date="...">` elements. Fetching a full year is the backfill strategy.

#### Landing zone layout

```
{landing_base_path}/landing/bnr_fx_rates/
    {YYYY}/
        {YYYY-MM-DD}.xml       ← one file per business day, extracted from archive
        ...
    current.xml                ← the raw "today" file (convenience; not used by Bronze)
```

Write **one XML file per business day**. When fetching an archive year, split the multi-date XML into per-date files — store each `<Cube date="...">` block individually. This keeps the landing zone consistent with all other sources (one file per date).

If a landing file already exists for a date, skip the write (idempotent).

#### Class interface

```python
class BnrIngester(BaseIngester):
    def fetch(self, start: date, end: date) -> dict:
        """
        Returns {
            "source": "BNR",
            "dates": [{"date": "YYYY-MM-DD", "raw_xml": "<Cube ...>...</Cube>", "url": "..."}, ...]
        }
        Fetches archive year-files as needed; falls back to daily URL for today.
        """

    def ingest(self, start: date, end: date) -> None:
        """Fetch and write per-date XML files to landing zone. Skips existing files."""
```

**Implementation notes:**
- For a date range that spans multiple years, fetch each year's archive file once.
- If a date is `date.today()` or very recent (archive may not have it yet), fall back to `nbrfxrates.xml`.
- Use `httpx` with `timeout=30`. Add `time.sleep(0.5)` between year requests to be polite.
- Wrap HTTP calls in a retry helper (3 attempts, exponential backoff) — same pattern as `src/shiden/ingestion/opcom.py`.
- The per-date XML snippet to write should be a self-contained XML document (add `<?xml version="1.0"?>` header).

---

## BNR Bronze Writer

### File: `src/shiden/processing/bronze/bnr.py`

**Responsibility:** Parse landing XML files → append to `bronze/bnr_fx_rates` Delta table.

#### Bronze schema

```
date              DATE        NOT NULL    -- <Cube date="..."> attribute
foreign_currency  STRING      NOT NULL    -- e.g. EUR, USD, GBP
rate_ron          DOUBLE      NOT NULL    -- X RON per `multiplier` units of foreign_currency
multiplier        INT         NOT NULL    -- 1 for most currencies; 100 for JPY, HUF, etc.
source_url        STRING      NOT NULL    -- URL the XML was fetched from
ingested_at       TIMESTAMP   NOT NULL
```

Natural key: `(date, foreign_currency)`.
Partition by: `foreign_currency` (small cardinality, good for Silver joins).

#### Idempotency

Use **Delta MERGE** on `(date, foreign_currency)` — same pattern as `src/shiden/processing/bronze/opcom.py`.

```python
from delta.tables import DeltaTable

DeltaTable.createIfNotExists(spark) \
    .tableName(...) \
    .location(table_path) \
    .addColumns(_SCHEMA) \
    .execute()

delta_table.alias("t").merge(
    incoming_df.alias("s"),
    "t.date = s.date AND t.foreign_currency = s.foreign_currency"
).whenNotMatchedInsertAll() \
 .execute()
```

No `whenMatchedUpdate` — Bronze rows are immutable once written. If a re-fetch brings the same date, MERGE skips it.

#### Class interface

```python
class BnrBronzeWriter:
    def process(self, start: date, end: date, spark: SparkSession) -> None:
        """
        Parse landing XML files for [start, end] and MERGE into bronze/bnr_fx_rates.
        Skips dates with no landing file (warns).
        """
```

#### Parsing notes

- Parse with `xml.etree.ElementTree` (stdlib, no extra deps).
- Handle the `multiplier` attribute: `int(elem.get("multiplier", "1"))`.
- Skip non-numeric rates (BNR occasionally publishes "-" for illiquid currencies).
- The `source_url` comes from the landing file's XML comment or a sidecar; simplest is to embed the URL in the per-date XML filename's parent directory, or store it in a thin JSON sidecar `{YYYY-MM-DD}.json` next to the XML. **Decision: write a minimal JSON sidecar** `{YYYY-MM-DD}.json` alongside the XML:
  ```json
  {"date": "2024-01-15", "source_url": "https://www.bnr.ro/files/xml/years/nbrfxrates2024.xml", "fetched_at": "2024-01-15T14:30:00Z"}
  ```
  Bronze reads both.

---

## ECB Ingester

### File: `src/shiden/ingestion/ecb.py`

**Responsibility:** Fetch ECB historical XML, extract per-date rate slices, write to landing zone.

#### URL

```
Full history (single file, ~5 MB, ~3500+ business days):
https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml
```

ECB publishes all history in one file. Download it once per day during the daily run.

#### Landing zone layout

```
{landing_base_path}/landing/ecb_fx_rates/
    {YYYY-MM-DD}.json          ← per-date JSON slice extracted from the full XML
```

Download the full XML, parse it in memory, extract per-date rate dictionaries, write one JSON file per date:

```json
{
    "date": "2024-01-15",
    "rates": {"USD": 1.0921, "GBP": 0.8567, "RON": 4.9698, ...},
    "source_url": "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml",
    "fetched_at": "2024-01-15T18:00:00Z"
}
```

If a landing file already exists for a date, skip the write (idempotent).

#### Class interface

```python
class EcbIngester(BaseIngester):
    def fetch(self, start: date, end: date) -> dict:
        """
        Downloads the full ECB historical XML and parses all dates in [start, end].
        Returns {
            "source": "ECB",
            "dates": [{"date": "YYYY-MM-DD", "rates": {...}, "source_url": "..."}, ...]
        }
        """

    def ingest(self, start: date, end: date) -> None:
        """Write per-date JSON files to landing zone. Skips existing files."""
```

**Implementation notes:**
- Download with `httpx`, timeout=60 (file is ~5 MB).
- Parse namespace-aware: ECB uses `gesmes:` prefix. Use `xml.etree.ElementTree` with `{namespace}Cube` path.
- ECB does not publish on weekends or ECB holidays — gaps are normal; Silver forward-fills them.
- For historical backfill, a single `ingest(start=date(2020,1,1), end=date.today())` call fetches the full file once and writes all available dates.

---

## ECB Bronze Writer

### File: `src/shiden/processing/bronze/ecb.py`

**Responsibility:** Parse landing JSON files → append to `bronze/ecb_fx_rates` Delta table.

#### Bronze schema

```
date              DATE        NOT NULL    -- rate application date
quote_currency    STRING      NOT NULL    -- USD, GBP, RON, etc. (base is always EUR)
rate              DOUBLE      NOT NULL    -- 1 EUR = rate quote_currency
source_file_date  DATE        NOT NULL    -- date the full ECB XML was downloaded (lineage)
ingested_at       TIMESTAMP   NOT NULL
```

Natural key: `(date, quote_currency)`.
Partition by: `quote_currency`.

#### Idempotency

Same Delta MERGE pattern as BNR: merge on `(date, quote_currency)`, `whenNotMatchedInsertAll` only.

#### Class interface

```python
class EcbBronzeWriter:
    def process(self, start: date, end: date, spark: SparkSession) -> None:
        """Parse landing JSON files for [start, end] and MERGE into bronze/ecb_fx_rates."""
```

---

## Scheduler Extension

### File: `src/shiden/scheduler/jobs.py` — add to existing file

```python
def run_fx_rates_daily() -> None:
    """
    Fetch BNR and ECB FX rates to landing, then process into Bronze.

    Run daily at 17:00 Romanian time (EET/EEST).
    - BNR publishes at ~13:00 Bucharest; safe to fetch after 16:00.
    - ECB publishes at ~16:00 CET; 17:00 Bucharest (= 15:00 CET) is early
      for today's ECB rate but catches yesterday's confirmed rate.

    Fetches the last 7 days to backfill any missed days.
    """
    from datetime import timedelta
    from shiden.ingestion.bnr import BnrIngester
    from shiden.ingestion.ecb import EcbIngester
    from shiden.processing.bronze.bnr import BnrBronzeWriter
    from shiden.processing.bronze.ecb import EcbBronzeWriter
    from shiden.processing.spark import get_spark

    today = date.today()
    start = today - timedelta(days=7)

    spark = get_spark()
    logger.info("Starting daily FX rates ingest for %s–%s", start, today)

    BnrIngester().ingest(start, today)
    BnrBronzeWriter().process(start, today, spark)

    EcbIngester().ingest(start, today)
    EcbBronzeWriter().process(start, today, spark)

    logger.info("Daily FX rates pipeline complete for %s–%s", start, today)
```

---

## Tests

### Unit tests: `tests/unit/test_bnr_parser.py`

Test the XML parsing logic — no network, no Spark.

```python
BNR_XML_SAMPLE = """<?xml version="1.0" encoding="utf-8"?>
<DataSet xmlns="http://www.bnr.ro/xsd">
  <Header>
    <PublishingDate>2024-01-15</PublishingDate>
  </Header>
  <Body>
    <Cube date="2024-01-15">
      <Rate currency="EUR">4.9742</Rate>
      <Rate currency="USD">4.5123</Rate>
      <Rate currency="GBP">5.6789</Rate>
      <Rate currency="JPY" multiplier="100">3.4567</Rate>
    </Cube>
  </Body>
</DataSet>"""
```

Assert:
- `_parse_bnr_xml(BNR_XML_SAMPLE)` returns 4 rows
- EUR row: `date=2024-01-15, foreign_currency="EUR", rate_ron=4.9742, multiplier=1`
- JPY row: `multiplier=100, rate_ron=3.4567` (raw, NOT divided by 100 — Silver normalises)
- Non-numeric rates (if any) are skipped without raising

### Unit tests: `tests/unit/test_ecb_parser.py`

```python
ECB_XML_SAMPLE = """<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01"
                 xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
  <Cube>
    <Cube time="2024-01-15">
      <Cube currency="USD" rate="1.0921"/>
      <Cube currency="GBP" rate="0.8567"/>
      <Cube currency="RON" rate="4.9698"/>
    </Cube>
    <Cube time="2024-01-12">
      <Cube currency="USD" rate="1.0898"/>
      <Cube currency="GBP" rate="0.8551"/>
      <Cube currency="RON" rate="4.9712"/>
    </Cube>
  </Cube>
</gesmes:Envelope>"""
```

Assert:
- `_parse_ecb_xml(ECB_XML_SAMPLE)` returns 6 rows (3 currencies × 2 dates)
- USD row for 2024-01-15: `rate=1.0921, quote_currency="USD"`
- Date 2024-01-12 also parsed (weekend gap example — ECB published Friday rates)

### Integration tests: `tests/integration/test_bnr_ingester.py`

Mark with `pytestmark = pytest.mark.integration`. Make real HTTP requests.

```python
def test_fetch_recent_days():
    from shiden.ingestion.bnr import BnrIngester
    result = BnrIngester().fetch(date.today() - timedelta(days=5), date.today() - timedelta(days=1))
    assert len(result["dates"]) >= 1  # at least one business day in range
    day = result["dates"][0]
    assert "EUR" in {r["foreign_currency"] for r in day["rates"]}
    eur = next(r for r in day["rates"] if r["foreign_currency"] == "EUR")
    assert 4.0 < eur["rate_ron"] < 6.0  # sanity: EUR/RON reasonable range

def test_ingest_writes_landing_files(tmp_path):
    from shiden.config import settings as sm
    sm.settings.__dict__["landing_base_path"] = str(tmp_path)
    BnrIngester().ingest(date.today() - timedelta(days=3), date.today() - timedelta(days=1))
    files = list((tmp_path / "landing" / "bnr_fx_rates").rglob("*.xml"))
    assert len(files) >= 1
```

### Integration tests: `tests/integration/test_ecb_ingester.py`

Same structure — test that the ECB file downloads, parses, and EUR/RON and EUR/GBP rows are present.

---

## Code Style & Conventions

Follow the existing project conventions exactly:

- All imports at top of file; `from __future__ import annotations` on line 1 of every module
- Logging via `logger = logging.getLogger(__name__)` — `logger.info(...)` for normal flow, `logger.warning(...)` for missing files
- `settings` imported from `shiden.config.settings`
- Path construction via `Path(...)` — never string concatenation
- `httpx` for HTTP (already a project dependency) — not `requests`
- Pydantic is not used for internal data — use `NamedTuple` or plain dataclasses for row types
- Do not use `pandas` — Spark DataFrames only
- Retry pattern: copy `_fetch_with_retry()` pattern from `src/shiden/ingestion/opcom.py`

---

## Verification Checklist

Before marking this task done, verify:

- [ ] `python -m pytest tests/unit/test_bnr_parser.py -v` — all pass
- [ ] `python -m pytest tests/unit/test_ecb_parser.py -v` — all pass
- [ ] `python -c "from shiden.ingestion.bnr import BnrIngester; from shiden.ingestion.ecb import EcbIngester"` — no import errors
- [ ] `python -c "from shiden.processing.bronze.bnr import BnrBronzeWriter; from shiden.processing.bronze.ecb import EcbBronzeWriter"` — no import errors
- [ ] `python -c "from shiden.scheduler.jobs import run_fx_rates_daily"` — no import errors
- [ ] Run integration tests manually: `pytest -m integration tests/integration/test_bnr_ingester.py tests/integration/test_ecb_ingester.py -v`
- [ ] Confirm BNR landing files written: `ls data/landing/bnr_fx_rates/...`
- [ ] Confirm ECB landing files written: `ls data/landing/ecb_fx_rates/...`
- [ ] Confirm Bronze Delta tables created: `ls data/delta/bronze/bnr_fx_rates/` and `ecb_fx_rates/`
- [ ] Print Bronze schema for both tables to confirm all columns present
