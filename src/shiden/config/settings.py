from enum import Enum

from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    LOCAL = "local"
    DATABRICKS = "databricks"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    environment: Environment = Environment.LOCAL

    # Storage
    delta_base_path: str = "./data/delta"
    landing_base_path: str = "./data"

    # ENTSO-E
    entsoe_api_key: str = ""

    # OPCOM
    opcom_rate_limit_sleep: float = 1.0  # seconds between requests for different dates

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Spark
    spark_master: str = "local[*]"
    spark_app_name: str = "shiden"


settings = Settings()
