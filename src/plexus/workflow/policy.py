"""Retry and circuit-breaking policy: the two halves of "fail well".

Retries without a breaker turn one slow dependency into a stampede; a breaker
without retries turns a transient blip into a user-visible 500. Jitter is not
optional: synchronized retries from N replicas are how a degraded dependency
stays down.
"""

from __future__ import annotations

import random
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum

from ..errors import CircuitOpen, PlatformError


def _retry_if_retryable(exc: BaseException) -> bool:
    return isinstance(exc, PlatformError) and exc.retryable


class Jitter(str, Enum):
    FULL = "full"
    EQUAL = "equal"
    NONE = "none"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Exponential backoff with jitter and an explicit retryable-error contract."""

    max_attempts: int = 3
    base_delay_s: float = 0.5
    max_delay_s: float = 30.0
    multiplier: float = 2.0
    jitter: Jitter = Jitter.FULL
    retry_if: Callable[[BaseException], bool] = field(default=_retry_if_retryable)

    def delay_for(self, attempt: int, *, rng: random.Random | None = None) -> float:
        """Delay before retry `attempt` (1-based). Never negative, never above max."""
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        ceiling = min(self.max_delay_s, self.base_delay_s * (self.multiplier ** (attempt - 1)))
        randomizer = rng or random
        if self.jitter is Jitter.NONE:
            return ceiling
        if self.jitter is Jitter.FULL:
            return randomizer.uniform(0.0, ceiling)
        half = ceiling / 2.0
        return half + randomizer.uniform(0.0, half)

    def should_retry(self, exc: BaseException, attempt: int) -> bool:
        return attempt < self.max_attempts and bool(self.retry_if(exc))

    def execute(self, operation: Callable[[], object], *, sleep: Callable[[float], None] = time.sleep) -> object:
        """Run with retries; re-raises the final failure so callers see the real error."""
        attempt = 1
        while True:
            try:
                return operation()
            except BaseException as exc:
                if not self.should_retry(exc, attempt):
                    raise
                sleep(self.delay_for(attempt))
                attempt += 1


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True, slots=True)
class BreakerSnapshot:
    name: str
    state: BreakerState
    calls: int
    failures: int
    failure_ratio: float
    consecutive_failures: int
    trip_count: int
    retry_after_s: float


class CircuitBreaker:
    """Sliding-window breaker with a bounded half-open probe set.

    Trips on failure ratio over a time window once a minimum number of calls has been
    observed (so 1 failure out of 1 call cannot open it), then admits a few probes so
    recovery is proven before traffic floods back in.
    """

    def __init__(
        self,
        name: str = "default",
        *,
        failure_ratio: float = 0.5,
        min_calls: int = 10,
        window_s: float = 30.0,
        open_s: float = 20.0,
        half_open_max: int = 3,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        if not 0.0 < failure_ratio < 1.0:
            raise ValueError("failure_ratio must be between 0 and 1")
        self.name = name
        self.failure_ratio = failure_ratio
        self.min_calls = max(1, min_calls)
        self.window_s = window_s
        self.open_s = open_s
        self.half_open_max = max(1, half_open_max)
        self._time = time_fn
        self._lock = threading.RLock()
        self._outcomes: deque[tuple[float, bool]] = deque()
        self._state = BreakerState.CLOSED
        self._opened_at = 0.0
        self._probes_inflight = 0
        self._half_open_successes = 0
        self._consecutive_failures = 0
        self._trip_count = 0

    @property
    def state(self) -> BreakerState:
        with self._lock:
            self._maybe_half_open()
            return self._state

    def allow(self) -> bool:
        """Whether a call may go upstream right now."""
        with self._lock:
            self._maybe_half_open()
            if self._state is BreakerState.CLOSED:
                return True
            if self._state is BreakerState.HALF_OPEN and self._probes_inflight < self.half_open_max:
                self._probes_inflight += 1
                return True
            return False

    def record(self, *, success: bool) -> None:
        now = self._time()
        with self._lock:
            self._outcomes.append((now, success))
            self._prune(now)
            if self._state is BreakerState.HALF_OPEN and self._probes_inflight > 0:
                self._probes_inflight = max(0, self._probes_inflight - 1)
            if success:
                self._consecutive_failures = 0
                if self._state is BreakerState.HALF_OPEN:
                    self._half_open_successes += 1
                    if self._half_open_successes >= self.half_open_max:
                        self._close()
            else:
                self._consecutive_failures += 1
                if self._state is BreakerState.HALF_OPEN:
                    self._trip(now)
                elif self._should_trip(now):
                    self._trip(now)

    def retry_after_s(self) -> float:
        with self._lock:
            if self._state is not BreakerState.OPEN:
                return 0.0
            return max(0.0, self.open_s - (self._time() - self._opened_at))

    def snapshot(self) -> BreakerSnapshot:
        with self._lock:
            self._maybe_half_open()
            calls = len(self._outcomes)
            failures = sum(1 for _, ok in self._outcomes if not ok)
            return BreakerSnapshot(
                name=self.name,
                state=self._state,
                calls=calls,
                failures=failures,
                failure_ratio=(failures / calls) if calls else 0.0,
                consecutive_failures=self._consecutive_failures,
                trip_count=self._trip_count,
                retry_after_s=(max(0.0, self.open_s - (self._time() - self._opened_at)))
                if self._state is BreakerState.OPEN
                else 0.0,
            )

    @contextmanager
    def protect(self) -> Iterator[Callable[[bool], None]]:
        """`with breaker.protect() as done:` records the outcome when the block exits."""
        if not self.allow():
            snapshot = self.snapshot()
            raise CircuitOpen(
                f"circuit {self.name!r} is open",
                retry_after_s=snapshot.retry_after_s,
                details={"breaker": self.name, "state": snapshot.state.value},
            )
        recorded = False

        def done(success: bool) -> None:
            nonlocal recorded
            if not recorded:
                recorded = True
                self.record(success=success)

        try:
            yield done
        except BaseException:
            done(False)
            raise
        else:
            done(True)

    def force_state(self, state: BreakerState) -> None:
        """Test/ops hook: put the breaker in a known state without faking time."""
        with self._lock:
            self._state = state
            if state is BreakerState.OPEN:
                self._opened_at = self._time()
            if state is BreakerState.CLOSED:
                self._reset_half_open()
                self._outcomes.clear()

    def _maybe_half_open(self) -> None:
        if self._state is BreakerState.OPEN and (self._time() - self._opened_at) >= self.open_s:
            self._state = BreakerState.HALF_OPEN
            self._reset_half_open()

    def _should_trip(self, now: float) -> bool:
        self._prune(now)
        calls = len(self._outcomes)
        if calls < self.min_calls:
            return False
        failures = sum(1 for _, ok in self._outcomes if not ok)
        return (failures / calls) >= self.failure_ratio

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_s
        while self._outcomes and self._outcomes[0][0] < cutoff:
            self._outcomes.popleft()

    def _trip(self, now: float) -> None:
        self._state = BreakerState.OPEN
        self._opened_at = now
        self._trip_count += 1
        self._outcomes.clear()
        self._reset_half_open()

    def _close(self) -> None:
        self._state = BreakerState.CLOSED
        self._outcomes.clear()
        self._reset_half_open()

    def _reset_half_open(self) -> None:
        self._probes_inflight = 0
        self._half_open_successes = 0


class BreakerRegistry:
    """One breaker per upstream (provider, region, or model endpoint)."""

    def __init__(self, **defaults: object) -> None:
        self._defaults = dict(defaults)
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get(self, name: str) -> CircuitBreaker:
        existing = self._breakers.get(name)
        if existing is not None:
            return existing
        with self._lock:
            existing = self._breakers.get(name)
            if existing is None:
                existing = CircuitBreaker(name, **self._defaults)  # type: ignore[arg-type]
                self._breakers[name] = existing
            return existing

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._breakers))

    def snapshots(self) -> list[BreakerSnapshot]:
        return [breaker.snapshot() for breaker in list(self._breakers.values())]
