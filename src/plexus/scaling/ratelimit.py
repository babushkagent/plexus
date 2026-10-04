"""Admission control: token buckets and concurrency gates.

Scale-invariance requires that one tenant cannot convert its own burst into
everyone else's latency. Buckets are per-tenant and in-process by default; the
same interface accepts a shared (Redis/Postgres) implementation so a fleet can
enforce a global budget without changing callers.
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field

Clock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class BucketState:
    available: float
    capacity: float
    refill_rate: float
    retry_after_s: float


class TokenBucket:
    """Classic token bucket with continuous refill and no background task."""

    def __init__(self, *, capacity: float, refill_rate: float, clock: Clock = time.monotonic) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        if refill_rate < 0:
            raise ValueError("refill_rate must be >= 0")
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)
        self._clock = clock
        self._tokens = float(capacity)
        self._updated = clock()
        self._lock = threading.Lock()

    def _refill(self, now: float) -> None:
        elapsed = max(0.0, now - self._updated)
        if elapsed:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
            self._updated = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        return self.acquire(tokens) == 0.0

    def acquire(self, tokens: float = 1.0) -> float:
        """Consume `tokens`; returns seconds to wait (0.0 when admitted)."""
        amount = max(0.0, float(tokens))
        with self._lock:
            now = self._clock()
            self._refill(now)
            if self._tokens >= amount:
                self._tokens -= amount
                return 0.0
            missing = amount - self._tokens
            return missing / self.refill_rate if self.refill_rate > 0 else math.inf

    def retry_after_s(self, tokens: float = 1.0) -> float:
        with self._lock:
            self._refill(self._clock())
            missing = max(0.0, float(tokens) - self._tokens)
            if not missing:
                return 0.0
            return missing / self.refill_rate if self.refill_rate > 0 else math.inf

    def available(self) -> float:
        with self._lock:
            self._refill(self._clock())
            return self._tokens

    def state(self) -> BucketState:
        with self._lock:
            self._refill(self._clock())
            return BucketState(
                available=self._tokens,
                capacity=self.capacity,
                refill_rate=self.refill_rate,
                retry_after_s=(self.capacity - self._tokens) / self.refill_rate if self.refill_rate else 0.0,
            )


class ConcurrencyLimiter:
    """Bounding in-flight work per tenant keeps a flood from exhausting the pool."""

    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        self.limit = int(limit)
        self._active = 0
        self._condition = threading.Condition()

    def try_acquire(self) -> bool:
        with self._condition:
            if self._active >= self.limit:
                return False
            self._active += 1
            return True

    def release(self) -> None:
        with self._condition:
            if self._active > 0:
                self._active -= 1
            self._condition.notify()

    @property
    def active(self) -> int:
        with self._condition:
            return self._active

    @contextmanager
    def slot(self) -> Iterator[None]:
        if not self.try_acquire():
            raise RuntimeError("concurrency limit reached")
        try:
            yield
        finally:
            self.release()


class RateLimitRegistry:
    """Per-tenant buckets with plan-derived defaults and explicit overrides."""

    def __init__(
        self,
        *,
        default_rps: float = 50.0,
        default_burst: float = 100.0,
        max_concurrency: int = 32,
        clock: Clock = time.monotonic,
    ) -> None:
        self._default_rps = default_rps
        self._default_burst = default_burst
        self._max_concurrency = max(1, max_concurrency)
        self._clock = clock
        self._buckets: dict[str, tuple[tuple[float, float], TokenBucket]] = {}
        self._gates: dict[str, tuple[int, ConcurrencyLimiter]] = {}
        self._lock = threading.Lock()

    def bucket(
        self,
        tenant_id: str,
        *,
        rps: float | None = None,
        burst: float | None = None,
    ) -> TokenBucket:
        """Return the tenant's bucket, rebuilding it only when its limits change.

        Callers pass plan-derived limits on every request, so a limiter must be
        reused across calls with the same limits - recreating it would refill it
        to full and make per-plan throttling a no-op.
        """
        rate = self._default_rps if rps is None else max(0.001, float(rps))
        capacity = max(rate, self._default_burst if burst is None else float(burst))
        signature = (round(rate, 6), round(capacity, 6))
        with self._lock:
            cached = self._buckets.get(tenant_id)
            if cached is not None and cached[0] == signature:
                return cached[1]
            bucket = TokenBucket(capacity=capacity, refill_rate=rate, clock=self._clock)
            self._buckets[tenant_id] = (signature, bucket)
            return bucket

    def gate(self, tenant_id: str, *, limit: int | None = None) -> ConcurrencyLimiter:
        wanted = max(1, self._max_concurrency if limit is None else int(limit))
        with self._lock:
            cached = self._gates.get(tenant_id)
            if cached is not None and cached[0] == wanted:
                return cached[1]
            gate = ConcurrencyLimiter(wanted)
            self._gates[tenant_id] = (wanted, gate)
            return gate

    def check(self, tenant_id: str, *, tokens: float = 1.0, rps: float | None = None, burst: float | None = None) -> float:
        return self.bucket(tenant_id, rps=rps, burst=burst).acquire(tokens)

    def snapshot(self) -> dict[str, dict[str, float]]:
        with self._lock:
            buckets = {
                tenant: {"available": state.available, "rps": state.refill_rate, "capacity": state.capacity}
                for tenant, state in ((tenant, bucket.state()) for tenant, (_, bucket) in self._buckets.items())
            }
            gates = {tenant: {"active": float(gate.active), "limit": float(gate.limit)}
                     for tenant, (_, gate) in self._gates.items()}
        merged: dict[str, dict[str, float]] = {}
        for tenant in set(buckets) | set(gates):
            merged[tenant] = {**buckets.get(tenant, {}), **{f"_{k}": v for k, v in gates.get(tenant, {}).items()}}
        return merged

    def forget(self, tenant_id: str) -> None:
        with self._lock:
            self._buckets.pop(tenant_id, None)
            self._gates.pop(tenant_id, None)

    def in_flight(self) -> int:
        """Requests holding a concurrency slot right now, summed across tenants.

        Saturation of this number is the load signal the autoscaler wants: it is
        backpressure at the admission boundary rather than queue depth downstream.
        """
        with self._lock:
            return sum(gate.active for _, gate in self._gates.values())


class SlidingWindowCounter:
    """Fixed-count limiter for cheap hard ceilings (e.g. requests per minute)."""

    def __init__(self, *, limit: int, window_s: float, clock: Clock = time.monotonic) -> None:
        self.limit = max(1, int(limit))
        self.window_s = float(window_s)
        self._clock = clock
        self._hits: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str, *, cost: int = 1) -> bool:
        now = self._clock()
        cutoff = now - self.window_s
        with self._lock:
            hits = [hit for hit in self._hits.get(key, []) if hit > cutoff]
            if len(hits) + cost > self.limit:
                self._hits[key] = hits
                return False
            hits.extend([now] * cost)
            self._hits[key] = hits
            return True

    def remaining(self, key: str) -> int:
        with self._lock:
            cutoff = self._clock() - self.window_s
            return max(0, self.limit - len([hit for hit in self._hits.get(key, []) if hit > cutoff]))


def retry_headers(state: Mapping[str, float]) -> dict[str, str]:
    return {key: f"{value:.3f}".rstrip("0").rstrip(".") for key, value in state.items()}
