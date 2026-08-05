from typing import Any

from fastapi import FastAPI

from shiden.api.routers import bess, generation, prices, weather

app = FastAPI(
    title="Shiden Energy API",
    description=(
        "Energy market data and BESS intelligence for operators and aggregators"
    ),
    version="0.1.0",
)

app.include_router(prices.router, prefix="/v1/prices", tags=["prices"])
app.include_router(generation.router, prefix="/v1/generation", tags=["generation"])
app.include_router(weather.router, prefix="/v1/weather", tags=["weather"])
app.include_router(bess.router, prefix="/v1/bess", tags=["bess"])


@app.get("/health", tags=["meta"])
def health() -> dict[str, Any]:
    return {"status": "ok", "version": "0.1.0"}
