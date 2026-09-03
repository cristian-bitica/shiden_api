from pathlib import Path
from typing import Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from shiden.api.auth.dependencies import get_store
from shiden.api.auth.ratelimit import limiter
from shiden.api.auth.store import KeyStore
from shiden.config.settings import settings


@pytest.fixture
def key_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[KeyStore, None, None]:
    """Isolated key store pointed at a temp SQLite file.

    ``get_store`` is lru_cached and is called directly by the metering
    middleware (not through Depends), so overriding the FastAPI dependency
    is not enough -- the cache has to be cleared around each test.
    """
    monkeypatch.setattr(settings, "auth_db_path", str(tmp_path / "auth.db"))
    monkeypatch.setattr(settings, "api_auth_enabled", True)
    get_store.cache_clear()
    limiter.reset()
    try:
        yield get_store()
    finally:
        get_store.cache_clear()
        limiter.reset()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Lightweight local SparkSession for tests.

    Delta JARs loaded via configure_spark_with_delta_pip.
    """
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("shiden-tests")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
