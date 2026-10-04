"""Sortable, collision-resistant identifiers and credential hashing.

IDs are ULIDs: lexicographically sortable by creation time (so range scans stay
local to a shard and hot writes never btree-bounce), 128-bit, and prefixable per
resource type for readable audit trails.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time

CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
TIMESTAMP_LEN = 10
RANDOM_LEN = 16
ULID_LEN = TIMESTAMP_LEN + RANDOM_LEN


def _encode(value: int, length: int) -> str:
    out = [""] * length
    for i in range(length - 1, -1, -1):
        out[i] = CROCKFORD[value & 0x1F]
        value >>= 5
    if value:
        raise ValueError("value too large for ULID component")
    return "".join(out)


def _decode(text: str) -> int:
    value = 0
    for ch in text.upper():
        idx = CROCKFORD.index(ch)
        value = (value << 5) | idx
    return value


def now_ms() -> int:
    return int(time.time() * 1000)


def ulid(ts_ms: int | None = None, random_bits: int | None = None) -> str:
    ts = now_ms() if ts_ms is None else ts_ms
    rand = secrets.randbits(80) if random_bits is None else random_bits
    return _encode(ts & ((1 << 48) - 1), TIMESTAMP_LEN) + _encode(rand & ((1 << 80) - 1), RANDOM_LEN)


def parse_ulid(value: str) -> tuple[int, int]:
    clean = value.rsplit("_", 1)[-1]
    if len(clean) != ULID_LEN:
        raise ValueError(f"not a ULID: {value!r}")
    return _decode(clean[:TIMESTAMP_LEN]), _decode(clean[TIMESTAMP_LEN:])


def created_ms(value: str) -> int:
    ts, _ = parse_ulid(value)
    return ts


def new_id(prefix: str) -> str:
    return f"{prefix}_{ulid()}"


def is_valid_id(prefix: str, value: str) -> bool:
    return value.startswith(f"{prefix}_") and len(value) == len(prefix) + 1 + ULID_LEN


def new_token(nbytes: int = 32) -> str:
    return f"plx_{secrets.token_urlsafe(nbytes)}"


def hash_token(token: str, *, pepper: str = "") -> str:
    """Deterministic digest for at-rest lookup of credentials.

    HMAC (not bare sha256) so a stolen database without the pepper cannot be
    rainbow-tabled against candidate keys.
    """
    return hmac.new(pepper.encode(), token.encode(), hashlib.sha256).hexdigest()


def constant_time_equals(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode(), b.encode())


def fingerprint(payload: str) -> str:
    return hashlib.sha256(payload.encode()).hexdigest()[:32]
