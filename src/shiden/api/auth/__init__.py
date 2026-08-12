"""API authentication, entitlements and usage metering."""

from shiden.api.auth.dependencies import (
    AuthenticatedKey,
    get_store,
    require_api_key,
    require_market_access,
)
from shiden.api.auth.store import ALL_MARKETS, ApiKeyRecord, KeyStore

__all__ = [
    "ALL_MARKETS",
    "ApiKeyRecord",
    "AuthenticatedKey",
    "KeyStore",
    "get_store",
    "require_api_key",
    "require_market_access",
]
