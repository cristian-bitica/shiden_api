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

See [docs/architecture.md](docs/architecture.md) for full architecture, data sources, and schema details.

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

CI (`.github/workflows/ci.yml`) runs ruff, mypy, and unit tests on every push/PR to `main`.

## API endpoints

All endpoints are versioned under `/v1/`.

| Endpoint | Description | Status |
|---|---|---|
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
docs/               Architecture and pipeline instructions
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
