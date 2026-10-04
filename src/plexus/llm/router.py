"""Inference router: the only place that decides *which* upstream answers.

Three invariants make the gateway scale-invariant and fault tolerant:

1. Candidate order is derived from a consistent hash of ``(tenant, model)``, so
   a tenant keeps its provider affinity (better cache/keeps-alive reuse) while
   tenants spread across the pool, and adding a provider only moves a slice of
   keys instead of reshuffling everyone.
2. Every upstream call passes through its own circuit breaker, so one unhealthy
   provider cannot turn tail latency into a tenant-wide outage; the next
   candidate is tried in the same request.
3. Budget and usage accounting happen in the router, not in a handler, so
   background jobs and workflows cannot bypass spend limits.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Protocol

from ..config import Settings
from ..errors import (
    BudgetExhausted,
    CircuitOpen,
    Forbidden,
    NotFound,
    RateLimited,
    TenantIsolationViolation,
    UpstreamError,
    UpstreamUnavailable,
    ValidationFailed,
)
from ..scaling.hashring import HashRing
from ..scaling.ratelimit import RateLimitRegistry
from ..telemetry import METRICS
from ..workflow.policy import BreakerRegistry, BreakerState
from .provider import Completion, CompletionRequest, Provider, Usage

log = logging.getLogger(__name__)

# Tenant-level decisions are the same for every candidate, so retrying the next
# provider would only hide the real answer (and burn budget on pointless calls).
_PASS_THROUGH = (BudgetExhausted, RateLimited, ValidationFailed, Forbidden, TenantIsolationViolation)


@dataclass(frozen=True, slots=True)
class ProviderPrice:
    prompt_usd_per_1k: float = 0.0
    completion_usd_per_1k: float = 0.0

    def cost(self, usage: Usage) -> float:
        return (usage.prompt_tokens / 1000.0) * self.prompt_usd_per_1k + (
            usage.completion_tokens / 1000.0
        ) * self.completion_usd_per_1k


class Pricing:
    """Illustrative default prices; override per deployment via ``with_price``.

    A conservative projection is used *before* the call so a tenant that has
    already blown its budget cannot start an expensive generation, and the real
    usage returned by the provider is charged afterwards.
    """

    DEFAULTS: ClassVar[dict[str, ProviderPrice]] = {
        "echo": ProviderPrice(0.0, 0.0),
        "openai": ProviderPrice(0.15 / 1000, 0.60 / 1000),
        "ollama": ProviderPrice(0.0, 0.0),
    }

    def __init__(self, prices: dict[str, ProviderPrice] | None = None, *, fallback: ProviderPrice | None = None) -> None:
        self._prices = dict(prices or self.DEFAULTS)
        self._fallback = fallback or ProviderPrice(0.0, 0.0)

    def with_price(self, provider: str, price: ProviderPrice) -> Pricing:
        merged = dict(self._prices)
        merged[provider] = price
        return Pricing(merged, fallback=self._fallback)

    def for_provider(self, provider: str) -> ProviderPrice:
        return self._prices.get(provider, self._fallback)

    def projected(self, request: CompletionRequest, provider: str) -> float:
        price = self.for_provider(provider)
        expected_output = request.max_tokens if request.max_tokens is not None else 256
        return price.cost(Usage(request.estimated_prompt_tokens(), expected_output))

    def actual(self, completion: Completion) -> float:
        return self.for_provider(completion.provider).cost(completion.usage)


class UsageLedger(Protocol):
    """Spend accounting seam: the API wires this to a database transaction."""

    def assert_budget(self, tenant_id: str, projected_cost_usd: float) -> None: ...

    def charge(self, tenant_id: str, cost_usd: float) -> None: ...

    def record(self, tenant_id: str, completion: Completion, *, cost_usd: float, latency_ms: float,
               request_id: str | None = None) -> None: ...


class NullLedger:
    """Used by CLI/worker paths that intentionally do not bill."""

    def assert_budget(self, tenant_id: str, projected_cost_usd: float) -> None:
        return None

    def charge(self, tenant_id: str, cost_usd: float) -> None:
        return None

    def record(self, tenant_id: str, completion: Completion, *, cost_usd: float, latency_ms: float,
               request_id: str | None = None) -> None:
        return None


@dataclass(frozen=True, slots=True)
class Attempt:
    provider: str
    ok: bool
    latency_ms: float
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"provider": self.provider, "ok": self.ok, "latency_ms": round(self.latency_ms, 3),
                "error": self.error}


@dataclass(frozen=True, slots=True)
class RouteResult:
    completion: Completion
    attempts: tuple[Attempt, ...]
    cost_usd: float
    latency_ms: float

    @property
    def provider(self) -> str:
        return self.completion.provider

    @property
    def fallbacks(self) -> int:
        return sum(1 for attempt in self.attempts if not attempt.ok)

    def to_dict(self) -> dict[str, Any]:
        """OpenAI-compatible body plus the platform fields operators actually need.

        ``choices`` keeps strict OpenAI SDK clients working unchanged; ``text``,
        ``attempts`` and ``cost_usd`` are additive extensions that a typed client
        ignores. One representation for HTTP, CLI and worker output means a replayed
        response can never disagree with the one that was billed.
        """
        completion = self.completion
        return {
            "object": "chat.completion",
            "model": completion.model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": completion.text},
                    "finish_reason": completion.finish_reason,
                }
            ],
            "usage": completion.usage.to_dict(),
            "text": completion.text,
            "provider": completion.provider,
            "finish_reason": completion.finish_reason,
            "attempts": [attempt.to_dict() for attempt in self.attempts],
            "fallbacks": self.fallbacks,
            "cost_usd": round(self.cost_usd, 8),
            "latency_ms": round(self.latency_ms, 3),
        }


@dataclass(slots=True)
class StreamHandle:
    """Iterator over streamed text that also reports where the answer came from.

    Callers iterate first and then read ``result``; a provider swap is only
    allowed before the first byte reaches the client.
    """

    chunks: Iterator[str]
    tenant_id: str
    model: str
    provider: str | None = None
    text: str = ""
    attempts: tuple[Attempt, ...] = ()
    started_at: float = 0.0

    def __iter__(self) -> Iterator[str]:
        return self.chunks


class InferenceRouter:
    def __init__(
        self,
        providers: Sequence[Provider],
        *,
        settings: Settings | None = None,
        breakers: BreakerRegistry | None = None,
        rate_limiter: RateLimitRegistry | None = None,
        pricing: Pricing | None = None,
        ledger: UsageLedger | None = None,
        admission: bool = True,
    ) -> None:
        self._settings = settings or Settings()
        self._providers: dict[str, Provider] = {provider.name: provider for provider in providers}
        if not self._providers:
            raise ValueError("at least one provider is required")
        self._ring = HashRing.from_members(sorted(self._providers))
        self._breakers = breakers or BreakerRegistry(
            failure_ratio=self._settings.breaker_failure_ratio,
            min_calls=self._settings.breaker_min_calls,
            window_s=self._settings.breaker_window_s,
            open_s=self._settings.breaker_open_s,
            half_open_max=self._settings.breaker_half_open_max,
        )
        self._limits = rate_limiter or RateLimitRegistry(
            default_rps=self._settings.default_rps,
            default_burst=self._settings.default_burst,
            max_concurrency=self._settings.max_concurrent_per_tenant,
        )
        self._pricing = pricing or Pricing()
        self._ledger = ledger or NullLedger()
        # HTTP deployments admit at the edge; re-checking here would take a second token
        # from the same bucket and throttle every plan at 2x its advertised rate.
        self._admission = admission

    @property
    def providers(self) -> tuple[str, ...]:
        return tuple(sorted(self._providers))

    def provider(self, name: str) -> Provider:
        try:
            return self._providers[name]
        except KeyError:
            raise NotFound("unknown provider", details={"provider": name}) from None

    def candidates(self, tenant_id: str, model: str) -> tuple[str, ...]:
        """Hash-ordered candidate list: stable per tenant, spread across the pool."""
        return self._ring.nodes_for(f"{tenant_id}:{model}")

    def _require_model(self, model: str) -> None:
        allowed = self._settings.allowed_models
        if allowed and model not in allowed:
            # 400, not 404: the route exists, the *value in the body* is unacceptable,
            # and a client must never read an allow-list membership signal from a status code.
            raise ValidationFailed(
                "model is not enabled on this platform",
                details={"model": model, "allowed": list(allowed)},
            )

    def _admit(self, tenant_id: str, *, rps: float | None, burst: float | None) -> None:
        if not self._admission:
            return
        wait_s = self._limits.check(tenant_id, rps=rps, burst=burst)
        if wait_s > 0:
            METRICS.counter("plexus_rate_limited_total", labels={"scope": "tenant"})
            raise RateLimited("tenant rate limit exceeded", retry_after_s=max(0.05, wait_s))

    def route(
        self,
        tenant_id: str,
        request: CompletionRequest,
        *,
        rps: float | None = None,
        burst: float | None = None,
        ledger: UsageLedger | None = None,
        max_attempts: int | None = None,
        request_id: str | None = None,
    ) -> RouteResult:
        self._require_model(request.model)
        self._admit(tenant_id, rps=rps, burst=burst)
        ledger = ledger or self._ledger
        attempts: list[Attempt] = []
        started = time.perf_counter()
        for name in self._ordered_candidates(tenant_id, request.model)[: max(1, max_attempts or self._settings.provider_max_attempts)]:
            provider = self._providers[name]
            breaker = self._breakers.get(name)
            projected = self._pricing.projected(request, name)
            attempt_start = time.perf_counter()
            try:
                if not provider.healthy():
                    raise UpstreamError("provider reports unhealthy", details={"provider": name})
                ledger.assert_budget(tenant_id, projected)
                with breaker.protect():
                    completion = provider.complete(request)
            except _PASS_THROUGH:
                raise
            except Exception as exc:
                latency_ms = (time.perf_counter() - attempt_start) * 1000.0
                attempts.append(Attempt(provider=name, ok=False, latency_ms=latency_ms, error=_reason(exc)))
                log.warning("inference_attempt_failed", extra={"extra": {"provider": name, "model": request.model}})
                METRICS.counter("plexus_inference_attempts_total", labels={"provider": name, "outcome": "error"})
                if isinstance(exc, CircuitOpen):
                    continue
                continue
            latency_ms = (time.perf_counter() - attempt_start) * 1000.0
            cost = self._pricing.actual(completion)
            attempts.append(Attempt(provider=name, ok=True, latency_ms=latency_ms))
            ledger.charge(tenant_id, cost)
            ledger.record(tenant_id, completion, cost_usd=cost,
                          latency_ms=(time.perf_counter() - started) * 1000.0, request_id=request_id)
            METRICS.counter("plexus_inference_requests_total",
                            labels={"provider": name, "model": request.model, "outcome": "ok"})
            METRICS.observe("plexus_inference_latency_ms", latency_ms, labels={"provider": name})
            total_ms = (time.perf_counter() - started) * 1000.0
            return RouteResult(completion=completion, attempts=tuple(attempts), cost_usd=cost, latency_ms=total_ms)

        METRICS.counter("plexus_inference_requests_total", labels={"model": request.model, "outcome": "exhausted"})
        detail: dict[str, Any] = {"attempts": [attempt.to_dict() for attempt in attempts]}
        if attempts:
            detail["last_error"] = attempts[-1].error
        raise UpstreamUnavailable("no provider could serve the request", details=detail)

    def stream(
        self,
        tenant_id: str,
        request: CompletionRequest,
        *,
        rps: float | None = None,
        burst: float | None = None,
        ledger: UsageLedger | None = None,
        max_attempts: int | None = None,
        request_id: str | None = None,
    ) -> StreamHandle:
        """Stream tokens with failover that is safe up to the first byte.

        The first chunk is pulled synchronously while choosing a provider. That
        is what makes fallback honest: if no upstream can start, the caller gets
        a normal 5xx JSON error; once bytes are flowing the client has already
        been committed, so a later failure ends the stream instead of retrying.
        """
        self._require_model(request.model)
        self._admit(tenant_id, rps=rps, burst=burst)
        ledger = ledger or self._ledger
        attempts: list[Attempt] = []
        started = time.perf_counter()
        limit = max(1, max_attempts or self._settings.provider_max_attempts)
        for name in self._ordered_candidates(tenant_id, request.model)[:limit]:
            provider = self._providers[name]
            breaker = self._breakers.get(name)
            projected = self._pricing.projected(request, name)
            if not provider.healthy():
                attempts.append(Attempt(provider=name, ok=False, latency_ms=0.0, error="unhealthy"))
                continue
            try:
                ledger.assert_budget(tenant_id, projected)
            except _PASS_THROUGH:
                raise
            except Exception as exc:
                attempts.append(Attempt(provider=name, ok=False, latency_ms=0.0, error=_reason(exc)))
                continue
            probe = time.perf_counter()
            try:
                upstream = iter(provider.stream(request))
                first = next(upstream, None)
            except _PASS_THROUGH:
                raise
            except Exception as exc:
                breaker.record(success=False)
                attempts.append(
                    Attempt(provider=name, ok=False, latency_ms=(time.perf_counter() - probe) * 1000.0,
                            error=_reason(exc))
                )
                log.warning("stream_probe_failed", extra={"extra": {"provider": name, "model": request.model}})
                continue

            breaker.record(success=True)
            attempts.append(Attempt(provider=name, ok=True, latency_ms=(time.perf_counter() - probe) * 1000.0))
            METRICS.counter("plexus_inference_stream_total", labels={"provider": name, "outcome": "started"})
            return self._collect(tenant_id=tenant_id, request=request, provider=name, upstream=upstream,
                                 first=first, ledger=ledger, attempts=tuple(attempts), started=started,
                                 request_id=request_id)

        METRICS.counter("plexus_inference_stream_total", labels={"outcome": "exhausted"})
        raise UpstreamUnavailable(
            "no provider could stream the request",
            details={"attempts": [attempt.to_dict() for attempt in attempts]},
        )

    def _collect(
        self,
        *,
        tenant_id: str,
        request: CompletionRequest,
        provider: str,
        upstream: Iterator[str],
        first: str | None,
        ledger: UsageLedger,
        attempts: tuple[Attempt, ...],
        started: float,
        request_id: str | None,
    ) -> StreamHandle:
        """Wrap the upstream iterator so usage is billed exactly once, on close."""
        handle = StreamHandle(chunks=iter(()), tenant_id=tenant_id, model=request.model, provider=provider)

        def generate() -> Iterator[str]:
            pieces: list[str] = []
            completed = False
            try:
                if first is not None:
                    pieces.append(first)
                    yield first
                for piece in upstream:
                    pieces.append(piece)
                    yield piece
                completed = True
            except GeneratorExit:
                # Client hung up: the upstream behaved, so do not punish its breaker.
                completed = True
                raise
            except Exception:
                self._breakers.get(provider).record(success=False)
                raise
            else:
                self._breakers.get(provider).record(success=True)
            finally:
                text = "".join(pieces)
                latency_ms = (time.perf_counter() - started) * 1000.0
                usage = Usage(request.estimated_prompt_tokens(), len(text) // 4)
                completion = Completion(text=text, model=request.model, provider=provider, usage=usage,
                                        finish_reason="stop" if completed else "cancelled")
                cost = self._pricing.actual(completion)
                ledger.charge(tenant_id, cost)
                ledger.record(tenant_id, completion, cost_usd=cost, latency_ms=latency_ms, request_id=request_id)
                handle.text = text
                handle.attempts = attempts
                handle.started_at = started

        handle.chunks = generate()
        return handle

    def _ordered_candidates(self, tenant_id: str, model: str) -> tuple[str, ...]:
        ordered = self.candidates(tenant_id, model)
        # Prefer providers that are not tripped, but keep the hash order within each
        # group so behaviour stays reproducible for a given tenant.
        # Inspect state rather than calling allow(): allow() consumes a half-open
        # probe permit, which ordering must not steal from the real attempt.
        healthy = {name for name in ordered if self._breakers.get(name).snapshot().state is not BreakerState.OPEN}
        closed = [name for name in ordered if name in healthy]
        open_ = [name for name in ordered if name not in healthy]
        return tuple(closed + open_) or ordered

    def health(self) -> dict[str, Any]:
        return {
            "providers": {
                name: {
                    "healthy": provider.healthy(),
                    "breaker": self._breakers.get(name).snapshot().state.value,
                }
                for name, provider in sorted(self._providers.items())
            },
            "ring": list(self._ring.members),
        }


def _reason(exc: BaseException) -> str:
    code = getattr(exc, "code", None)
    message = getattr(exc, "message", None) or str(exc)
    return f"{code or type(exc).__name__}: {message}"[:300]


def echo_router(settings: Settings | None = None, *, providers: Iterable[Provider] | None = None) -> InferenceRouter:
    """Convenience constructor for tests, examples, and offline development."""
    from .provider import EchoProvider

    return InferenceRouter(list(providers) if providers is not None else [EchoProvider()], settings=settings)

