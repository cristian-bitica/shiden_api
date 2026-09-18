# Python API reference

Auto-generated from the source. Every module in the `shiden` package is
documented here, including private helpers where they carry design rationale.

```{eval-rst}
.. autosummary::
   :toctree: _autosummary
   :template: autosummary/module.rst
   :recursive:

   shiden
```

## Module map

| Package | Responsibility |
|---|---|
| {py:mod}`shiden.config` | Market registry and env-based settings |
| {py:mod}`shiden.timeaxis` | DST-aware delivery-day time axis (pure stdlib) |
| {py:mod}`shiden.dates` | Date helpers shared across layers |
| {py:mod}`shiden.landing_paths` | Landing-zone path conventions |
| {py:mod}`shiden.ingestion` | Source-specific ingesters → landing zone |
| {py:mod}`shiden.processing.bronze` | Landing → Bronze (typed, append-only) |
| {py:mod}`shiden.processing.silver` | Bronze → Silver (Kimball star schema) |
| {py:mod}`shiden.processing.gold` | Silver → Gold (API-shaped tables) |
| {py:mod}`shiden.processing.delta_io` | Delta read/write helpers |
| {py:mod}`shiden.processing.spark` | Session construction, local vs Databricks |
| {py:mod}`shiden.api` | FastAPI app, routers, schemas, auth |
| {py:mod}`shiden.scheduler` | Job definitions and the local APScheduler runner |
| {py:mod}`shiden.backfill` | Resumable historical load CLI |

## Reading the generated pages

- **Ingesters** (`shiden.ingestion.*`) fetch and write immutable landing files.
  They do not transform.
- **Processors** (`shiden.processing.*`) are classes with a `run()` entry point,
  documented with their grain, write strategy and partitioning in the module
  docstring — read that before the methods.
- **Routers** (`shiden.api.routers.*`) are sync by design; see
  {doc}`../api-guide`.
