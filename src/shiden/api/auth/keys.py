"""API key generation, hashing and parsing.

Keys are shown to the client exactly once, at issuance.  Only a SHA-256
hash is persisted, so a leak of the key store does not leak usable
credentials.

Key format::

    shiden_live_<43 url-safe base64 chars>
    shiden_test_<43 url-safe base64 chars>

The environment segment is cosmetic (it helps clients keep staging and
production credentials apart) but it is preserved through hashing, so a
``test`` key can never be mistaken for a ``live`` one.

SHA-256 is used rather than a password KDF (bcrypt/argon2) deliberately:
API keys are 256 bits of machine-generated entropy, not user-chosen
passwords, so they are not brute-forceable and the KDF work factor would
only add latency to every single request.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass

KEY_PREFIX = "shiden"
LIVE = "live"
TEST = "test"

# 32 bytes -> 43 url-safe base64 characters.
_ENTROPY_BYTES = 32

# Number of leading characters retained in cleartext for display purposes
# (e.g. "shiden_live_a1b2c3d4"). Enough to identify a key in a list, far
# too little to reconstruct it.
_DISPLAY_CHARS = 8


class InvalidKeyFormat(ValueError):
    """Raised when a presented string is not shaped like a Shiden API key."""


@dataclass(frozen=True)
class GeneratedKey:
    """A freshly minted key. ``secret`` is only ever available here."""

    secret: str
    key_hash: str
    display: str
    environment: str


def generate_key(environment: str = LIVE) -> GeneratedKey:
    """Mint a new API key.

    The returned ``secret`` is the only copy that will ever exist in
    cleartext -- callers must surface it to the user immediately and then
    drop it.
    """
    if environment not in (LIVE, TEST):
        raise ValueError(
            f"environment must be {LIVE!r} or {TEST!r}, got {environment!r}"
        )

    token = secrets.token_urlsafe(_ENTROPY_BYTES)
    secret = f"{KEY_PREFIX}_{environment}_{token}"
    return GeneratedKey(
        secret=secret,
        key_hash=hash_key(secret),
        display=display_prefix(secret),
        environment=environment,
    )


def hash_key(secret: str) -> str:
    """Return the hex SHA-256 digest used as the stored key identity."""
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def display_prefix(secret: str) -> str:
    """Return the non-secret leading fragment shown in listings."""
    parse_environment(secret)  # validate shape before slicing
    head_len = len(KEY_PREFIX) + 1 + 4 + 1  # "shiden_" + "live" + "_"
    return secret[: head_len + _DISPLAY_CHARS]


def parse_environment(secret: str) -> str:
    """Return ``live``/``test`` for a well-formed key, else raise."""
    parts = secret.split("_", 2)
    if len(parts) != 3 or parts[0] != KEY_PREFIX or parts[1] not in (LIVE, TEST):
        raise InvalidKeyFormat("not a Shiden API key")
    if not parts[2]:
        raise InvalidKeyFormat("not a Shiden API key")
    return parts[1]


def looks_like_key(secret: str) -> bool:
    """Cheap shape check used to reject junk before touching the store."""
    try:
        parse_environment(secret)
    except InvalidKeyFormat:
        return False
    return True


def verify(secret: str, expected_hash: str) -> bool:
    """Constant-time comparison of a presented key against a stored hash."""
    return hmac.compare_digest(hash_key(secret), expected_hash)
