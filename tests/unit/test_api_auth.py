"""API key authentication, entitlement, rate limiting and metering tests.

These run without a data lake: auth and entitlement are enforced before any
Delta read, so a 401/403/429 is reached without touching Gold.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from shiden.api.auth import keys as keyutil
from shiden.api.auth.ratelimit import SlidingWindowLimiter
from shiden.api.auth.store import ALL_MARKETS, KeyStore
from shiden.api.main import app

# Market ids are the short registry keys from config.markets, not EIC codes.
RO = "RO"
DE = "DE"  # not in MARKETS — entitlement is checked before existence, so 403 wins

PRICES = f"/v1/prices/{RO}?start=2026-06-01&end=2026-06-02"


@pytest.fixture
def client(key_store: KeyStore) -> TestClient:
    return TestClient(app)


class TestKeyGeneration:
    def test_secret_is_prefixed_and_high_entropy(self) -> None:
        generated = keyutil.generate_key()
        assert generated.secret.startswith("shiden_live_")
        assert len(generated.secret) > 40

    def test_keys_are_unique(self) -> None:
        secrets_seen = {keyutil.generate_key().secret for _ in range(100)}
        assert len(secrets_seen) == 100

    def test_hash_is_stable_and_secret_not_recoverable(self) -> None:
        generated = keyutil.generate_key()
        assert keyutil.hash_key(generated.secret) == generated.key_hash
        assert generated.secret not in generated.key_hash

    def test_display_prefix_reveals_little(self) -> None:
        generated = keyutil.generate_key()
        assert generated.secret.startswith(generated.display)
        assert len(generated.display) < len(generated.secret) / 2

    def test_verify_rejects_wrong_secret(self) -> None:
        a, b = keyutil.generate_key(), keyutil.generate_key()
        assert keyutil.verify(a.secret, a.key_hash)
        assert not keyutil.verify(b.secret, a.key_hash)

    @pytest.mark.parametrize(
        "junk", ["", "nonsense", "shiden_", "shiden_prod_abc", "shiden_live_"]
    )
    def test_malformed_keys_rejected(self, junk: str) -> None:
        assert not keyutil.looks_like_key(junk)

    def test_test_and_live_are_distinct(self) -> None:
        live = keyutil.generate_key("live")
        test = keyutil.generate_key("test")
        assert live.environment == "live"
        assert test.environment == "test"
        assert not keyutil.verify(test.secret, live.key_hash)


class TestKeyStore:
    def test_issue_then_lookup(self, key_store: KeyStore) -> None:
        secret, record = key_store.issue_key(name="Acme", markets=[RO])
        found = key_store.lookup(secret)
        assert found is not None
        assert found.id == record.id
        assert found.name == "Acme"
        assert found.markets == (RO,)

    def test_lookup_unknown_returns_none(self, key_store: KeyStore) -> None:
        assert key_store.lookup(keyutil.generate_key().secret) is None

    def test_revoke_marks_inactive(self, key_store: KeyStore) -> None:
        secret, record = key_store.issue_key(name="Acme")
        assert key_store.revoke(str(record.id)) is True
        found = key_store.lookup(secret)
        assert found is not None and not found.is_active

    def test_revoking_twice_is_a_no_op(self, key_store: KeyStore) -> None:
        _, record = key_store.issue_key(name="Acme")
        assert key_store.revoke(str(record.id)) is True
        assert key_store.revoke(str(record.id)) is False

    def test_entitlement_wildcard(self, key_store: KeyStore) -> None:
        _, record = key_store.issue_key(name="All", markets=[ALL_MARKETS])
        assert record.allows_market(RO)
        assert record.allows_market(DE)

    def test_entitlement_scoped(self, key_store: KeyStore) -> None:
        _, record = key_store.issue_key(name="RO only", markets=[RO])
        assert record.allows_market(RO)
        assert not record.allows_market(DE)

    def test_rejects_empty_name(self, key_store: KeyStore) -> None:
        with pytest.raises(ValueError):
            key_store.issue_key(name="   ")


class TestAuthentication:
    def test_missing_key_is_401(self, client: TestClient) -> None:
        r = client.get(PRICES)
        assert r.status_code == 401
        assert "X-API-Key" in r.json()["detail"]

    def test_invalid_key_is_401(self, client: TestClient) -> None:
        r = client.get(PRICES, headers={"X-API-Key": keyutil.generate_key().secret})
        assert r.status_code == 401

    def test_revoked_key_is_401(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        secret, record = key_store.issue_key(name="Acme")
        key_store.revoke(str(record.id))
        r = client.get(PRICES, headers={"X-API-Key": secret})
        assert r.status_code == 401
        assert "revoked" in r.json()["detail"].lower()

    def test_health_stays_open(self, client: TestClient) -> None:
        assert client.get("/health").status_code == 200

    def test_whoami_reports_entitlements(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        secret, _ = key_store.issue_key(name="Acme", markets=[RO], rate_limit_per_min=5)
        r = client.get("/v1/me", headers={"X-API-Key": secret})
        assert r.status_code == 200
        assert r.json() == {
            "name": "Acme",
            "key": r.json()["key"],
            "environment": "live",
            "markets": [RO],
            "rate_limit_per_min": 5,
        }


class TestEntitlement:
    def test_unentitled_market_is_403(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        secret, _ = key_store.issue_key(name="RO only", markets=[RO])
        r = client.get(
            f"/v1/prices/{DE}?start=2026-06-01&end=2026-06-02",
            headers={"X-API-Key": secret},
        )
        assert r.status_code == 403
        assert DE in r.json()["detail"]

    def test_entitled_market_passes_auth(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        secret, _ = key_store.issue_key(name="RO only", markets=[RO])
        r = client.get(PRICES, headers={"X-API-Key": secret})
        # Past auth and entitlement; 503 only if Gold is absent in this env.
        assert r.status_code in (200, 503)

    def test_unknown_market_still_404s_for_wildcard_key(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        secret, _ = key_store.issue_key(name="All", markets=[ALL_MARKETS])
        r = client.get(
            "/v1/prices/XX?start=2026-06-01&end=2026-06-02",
            headers={"X-API-Key": secret},
        )
        assert r.status_code == 404


class TestRateLimiting:
    def test_limiter_blocks_past_limit(self) -> None:
        limiter = SlidingWindowLimiter()
        results = [limiter.check(key_id=1, limit=3) for _ in range(4)]
        assert [r.allowed for r in results] == [True, True, True, False]
        assert results[-1].retry_after >= 1

    def test_limiter_is_per_key(self) -> None:
        limiter = SlidingWindowLimiter()
        for _ in range(3):
            limiter.check(key_id=1, limit=3)
        assert limiter.check(key_id=2, limit=3).allowed

    def test_window_expiry_restores_budget(self) -> None:
        limiter = SlidingWindowLimiter(window_seconds=0.05)
        assert limiter.check(key_id=1, limit=1).allowed
        assert not limiter.check(key_id=1, limit=1).allowed
        import time

        time.sleep(0.06)
        assert limiter.check(key_id=1, limit=1).allowed

    def test_api_returns_429_with_retry_after(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        secret, _ = key_store.issue_key(name="Tight", rate_limit_per_min=2)
        headers = {"X-API-Key": secret}
        client.get("/v1/me", headers=headers)
        client.get("/v1/me", headers=headers)
        r = client.get("/v1/me", headers=headers)
        assert r.status_code == 429
        assert int(r.headers["Retry-After"]) >= 1
        assert r.headers["X-RateLimit-Remaining"] == "0"

    def test_rate_limit_headers_present_on_success(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        secret, _ = key_store.issue_key(name="Acme", rate_limit_per_min=10)
        r = client.get(PRICES, headers={"X-API-Key": secret})
        assert r.headers["X-RateLimit-Limit"] == "10"
        assert r.headers["X-RateLimit-Remaining"] == "9"


class TestUsageMetering:
    def test_successful_request_is_recorded(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        secret, record = key_store.issue_key(name="Acme")
        client.get(PRICES, headers={"X-API-Key": secret})

        summary = key_store.usage_summary()
        assert len(summary) == 1
        assert summary[0]["key_display"] == record.display
        assert summary[0]["requests"] == 1

    def test_rejected_request_is_recorded_without_a_key(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        client.get(PRICES)
        summary = key_store.usage_summary()
        assert summary[0]["key_display"] == "<unauthenticated>"
        assert summary[0]["errors"] == 1

    def test_throttled_request_is_attributed_to_the_key(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        """429s must name the client -- that is the upsell signal."""
        secret, record = key_store.issue_key(name="Tight", rate_limit_per_min=1)
        headers = {"X-API-Key": secret}
        client.get("/v1/me", headers=headers)
        assert client.get("/v1/me", headers=headers).status_code == 429

        summary = key_store.usage_summary()
        assert len(summary) == 1, "throttled call leaked into <unauthenticated>"
        assert summary[0]["key_display"] == record.display
        assert summary[0]["requests"] == 2
        assert summary[0]["errors"] == 1

    def test_health_is_not_metered(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        client.get("/health")
        assert key_store.usage_summary() == []

    def test_market_is_captured_for_billing(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        secret, record = key_store.issue_key(name="Acme")
        client.get(PRICES, headers={"X-API-Key": secret})
        assert key_store.count_requests_since(record.id, "2000-01-01") == 1

    def test_last_used_is_tracked(
        self, client: TestClient, key_store: KeyStore
    ) -> None:
        secret, record = key_store.issue_key(name="Acme")
        assert record.last_used_at is None
        client.get(PRICES, headers={"X-API-Key": secret})
        refreshed = key_store.lookup(secret)
        assert refreshed is not None and refreshed.last_used_at is not None


class TestOpenApiContract:
    def test_security_scheme_is_published(self, client: TestClient) -> None:
        schema = client.get("/openapi.json").json()
        assert "APIKeyHeader" in schema["components"]["securitySchemes"]
