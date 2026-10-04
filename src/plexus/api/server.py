"""HTTP edge: identity, tenant scoping, admission, idempotency and error shape.

Every cross-cutting invariant is enforced in one pipeline so handlers stay pure
business logic and cannot accidentally skip a security control. The edge also owns
the composition root (store, gateway, route table) so the API, CLI and tests all
assemble an identical process.
"""

from __future__ import annotations

import json
import os
import re
import signal
import threading
import time
from contextlib import nullcontext
from dataclasses import dataclass, replace
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterator, Sequence
from urllib.parse import parse_qs, unquote, urlparse

from ..config import Settings
from ..errors import (
    Conflict,
    Forbidden,
    MethodNotAllowed,
    NotFound,
    PlatformError,
    RateLimited,
    Unauthorized,
)
from ..ids import fingerprint, new_id
from ..llm.provider import Provider, build_providers
from ..llm.router import InferenceRouter, Pricing
from ..scaling.ratelimit import ConcurrencyLimiter, RateLimitRegistry
from ..store.db import Database
from ..store.uow import EventSink, Tenant, UnitOfWork
from ..telemetry import METRICS, TRACER, TraceContext, bind, configure_logging
from ..tenancy.auth import ApiKeyRecord, Authenticator, CredentialLookup
from ..tenancy.context import DEFAULT_LIMITS, PlanLimits, TenantContext, use
from ..tenancy.rbac import authorize, authorize_platform
from .handlers import ROUTES, Handler, StoreLedger
from .messages import Request, Response, no_content, respond, stream_response

log = configure_logging()

UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
POLICY_TTL_S = 5.0


@dataclass(frozen=True, slots=True)
class Route:
    method: str
    pattern: str
    handler: Handler
    capability: str | None = None
    public: bool = False
    platform: bool = False
    idempotent: bool = True


@lru_cache(maxsize=None)
def _compile(pattern: str) -> re.Pattern[str]:
    return re.compile("^" + re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", pattern) + "$")


def match_route(routes: Sequence[Route], method: str, path: str) -> tuple[Route | None, dict[str, str], set[str]]:
    allowed: set[str] = set()
    for route in routes:
        found = _compile(route.pattern).match(path)
        if found is None:
            continue
        if route.method == method:
            return route, {key: unquote(value) for key, value in found.groupdict().items()}, allowed
        allowed.add(route.method)
    return None, {}, allowed


class StoreCredentials:
    """CredentialLookup over the durable store: keys and revocations are data, not memory."""

    def __init__(self, app: App) -> None:
        self._app = app

    def api_key_by_digest(self, digest: str) -> ApiKeyRecord | None:
        with self._app.uow() as uow:
            record = uow.keys.lookup_by_digest(digest)
            if record is not None:
                uow.keys.touch(record.id)
            return record

    def is_denied(self, jti: str) -> bool:
        row = self._app.db.query(
            "SELECT expires_at_ms FROM denied_tokens WHERE jti = ?",
            (jti,),
        )
        if not row:
            return False
        expires = row[0]["expires_at_ms"]
        return expires is None or int(expires) > int(time.time() * 1000)


@dataclass(frozen=True, slots=True)
class TenantPolicy:
    limits: PlanLimits
    status: str


class App:
    """Composition root: settings + store + inference gateway + route table."""

    def __init__(
        self,
        settings: Settings,
        *,
        db: Database | None = None,
        providers: Sequence[Provider] | None = None,
        pricing: Pricing | None = None,
        gateway: InferenceRouter | None = None,
    ) -> None:
        self.settings = settings
        self.db = db if db is not None else Database(settings)
        self.sink = EventSink()
        self.limits = RateLimitRegistry(
            default_rps=settings.default_rps,
            default_burst=settings.default_burst,
            max_concurrency=settings.max_concurrent_per_tenant,
        )
        # Billing hangs off the gateway, not the HTTP handler: a workflow task that
        # calls the router directly must not get free tokens.
        self.ledger = StoreLedger(self)
        # Admission already happened at the edge for every request; the gateway must not
        # take a second token from the same bucket or plans would be throttled at 2x.
        self.gateway = gateway or InferenceRouter(
            list(providers) if providers is not None else build_providers(settings),
            settings=settings,
            rate_limiter=self.limits,
            pricing=pricing,
            ledger=self.ledger,
            admission=False,
        )
        self.credentials: CredentialLookup = StoreCredentials(self)
        self.authenticator = Authenticator(settings, self.credentials)
        self.routes = tuple(Route(route.method, route.pattern, route.handler, route.capability, route.public,
                                  route.platform, route.idempotent) for route in ROUTES)
        self.started_at = time.time()
        self._policies: dict[str, tuple[float, TenantPolicy]] = {}
        self._policy_lock = threading.Lock()

    def init_schema(self) -> list[str]:
        return self.db.migrate()

    def uow(self, *, tenant_id: str | None = None, immediate: bool = False) -> UnitOfWork:
        return UnitOfWork(
            self.db,
            tenant_id=tenant_id,
            immediate=immediate,
            event_sink=self.sink,
            settings=self.settings,
        )

    def policy_for(self, tenant_id: str, fallback: PlanLimits) -> TenantPolicy:
        """Plan defaults overridden by explicit per-tenant knobs, briefly cached.

        Caching is a scale-invariance property, not an optimization: admission control
        must cost O(1) per request regardless of how many tenants exist or how fast a
        single tenant shouts.
        """
        now = time.monotonic()
        with self._policy_lock:
            hit = self._policies.get(tenant_id)
            if hit is not None and hit[0] > now:
                return hit[1]
        try:
            with self.uow() as uow:
                tenant = uow.tenants.get(tenant_id)
        except PlatformError:
            tenant = None
        policy = TenantPolicy(limits=_merge_limits(fallback, tenant), status=tenant.status if tenant else "active")
        with self._policy_lock:
            self._policies[tenant_id] = (now + POLICY_TTL_S, policy)
        return policy

    def invalidate_policy(self, tenant_id: str) -> None:
        with self._policy_lock:
            self._policies.pop(tenant_id, None)

    def handle(self, request: Request) -> Response:
        started = time.perf_counter()
        route, params, allowed = match_route(self.routes, request.method, request.path)
        with TRACER.start(
            "http.request",
            context=request.trace,
            **{"http.method": request.method, "http.target": request.path},
        ) as span:
            request.params = params
            try:
                response = self._pipeline(request, route, allowed, span)
            except PlatformError as exc:
                response = self._from_error(exc)
            except Exception as exc:  # noqa: BLE001 - never leak internals to callers
                log.exception("unhandled_request_failure")
                span.record_error(exc)
                response = self._from_error(PlatformError("internal error"))
            latency_ms = (time.perf_counter() - started) * 1000.0
            route_key = route.pattern if route is not None else "unmatched"
            span.set("http.status_code", response.status)
            span.set("tenant_id", request.ctx.tenant_id if request.ctx else "-")
            METRICS.counter(
                "plexus_http_requests_total",
                labels={"method": request.method, "route": route_key, "status": str(response.status)},
                help_text="Requests handled, labelled by route and status.",
            )
            METRICS.observe(
                "plexus_http_latency_ms",
                latency_ms,
                labels={"route": route_key},
                help_text="Handler latency in milliseconds.",
            )
        response.headers.setdefault("X-Request-Id", request.request_id)
        response.headers.setdefault("Traceparent", request.trace.to_header())
        return response

    def _pipeline(self, request: Request, route: Route | None, allowed: set[str], span: Any) -> Response:
        if route is None:
            if allowed:
                raise MethodNotAllowed(
                    "method not allowed for this path",
                    details={"allowed": sorted(allowed)},
                )
            raise NotFound("route not found", details={"path": request.path})
        span.set("http.route", route.pattern)
        if not route.public:
            self._authenticate(request)

        gate: ConcurrencyLimiter | None = None
        ctx = request.ctx
        if ctx is not None:
            policy = self.policy_for(ctx.tenant_id, ctx.limits)
            if policy.status != "active":
                raise Forbidden("tenant is not active", details={"status": policy.status})
            limits = policy.limits
            wait_s = self.limits.check(
                ctx.tenant_id,
                rps=limits.requests_per_second,
                burst=limits.burst,
            )
            if wait_s > 0:
                raise RateLimited("tenant rate limit exceeded", retry_after_s=max(0.05, wait_s))
            gate = self.limits.gate(ctx.tenant_id, limit=limits.max_concurrency)
            if not gate.try_acquire():
                raise RateLimited("tenant concurrency limit exceeded", retry_after_s=0.25)
        # Binding the context means a handler cannot reach another tenant's rows even
        # if it forgets to pass tenant_id down: the scope is already in the task.
        with use(ctx) if ctx is not None else nullcontext():
            try:
                return self._authorize_and_run(request, route)
            finally:
                if gate is not None:
                    gate.release()

    def _authenticate(self, request: Request) -> None:
        ctx = self.authenticator.authenticate(
            authorization=request.header("authorization"),
            api_key=request.header("x-api-key"),
        )
        request.ctx = replace(ctx, request_id=request.request_id)
        for key, value in ctx.log_fields().items():
            if key != "request_id":
                bind(**{key: value})

    def _authorize_and_run(self, request: Request, route: Route) -> Response:
        ctx = request.ctx
        if ctx is not None:
            if route.platform:
                authorize_platform(ctx)
            if route.capability is not None:
                authorize(ctx, route.capability)
        key = request.header("idempotency-key")
        if not key or request.method not in UNSAFE_METHODS or not route.idempotent or ctx is None:
            return route.handler(self, request)
        return self._with_idempotency(request, route, key)

    def _with_idempotency(self, request: Request, route: Route, key: str) -> Response:
        assert request.ctx is not None
        tenant_id = request.ctx.tenant_id
        token = fingerprint({"method": request.method, "path": request.path, "body": request.body.decode("utf-8", "replace")})
        with self.uow(tenant_id=tenant_id, immediate=True) as uow:
            outcome = uow.idempotency.check(tenant_id, key, token)
        if outcome.kind == "replay":
            METRICS.counter("plexus_idempotent_replays_total", help_text="Replayed idempotent requests.")
            return respond(outcome.body, status=outcome.status_code or 200, headers={"X-Plexus-Replayed": "true"})
        if outcome.kind == "in_progress":
            raise Conflict(
                "a request with this idempotency key is still running",
                details={"key": key},
            )
        try:
            response = route.handler(self, request)
        except Exception:
            with self.uow(tenant_id=tenant_id, immediate=True) as uow:
                uow.idempotency.release(tenant_id, key)
            raise
        if response.events is None and response.status < 500:
            with self.uow(tenant_id=tenant_id, immediate=True) as uow:
                uow.idempotency.complete(tenant_id, key, response.status, response.payload)
        return response

    def _from_error(self, exc: PlatformError) -> Response:
        envelope = exc.to_envelope()
        headers: dict[str, str] = {}
        retry_after = getattr(exc, "retry_after_s", None)
        if retry_after is not None:
            headers["Retry-After"] = str(max(1, int(round(retry_after))))
        return Response(status=exc.status, payload=envelope, headers=headers)


def _merge_limits(base: PlanLimits, tenant: Tenant | None) -> PlanLimits:
    if tenant is None:
        return base
    plan = DEFAULT_LIMITS.get(tenant.plan, base)
    return replace(
        plan,
        requests_per_second=tenant.rps_limit if tenant.rps_limit is not None else plan.requests_per_second,
        burst=tenant.burst if tenant.burst is not None else plan.burst,
        max_concurrency=tenant.max_concurrency if tenant.max_concurrency is not None else plan.max_concurrency,
        monthly_budget_usd=tenant.monthly_budget_usd
        if tenant.monthly_budget_usd is not None
        else plan.monthly_budget_usd,
    )


class _HTTPRequestHandler(BaseHTTPRequestHandler):
    server_version = "plexus/0.1"
    protocol_version = "HTTP/1.1"

    @property
    def app(self) -> App:
        return self.server.app  # type: ignore[attr-defined,no-any-return]

    def do_GET(self) -> None:  # noqa: N802
        self._exchange("GET")

    def do_HEAD(self) -> None:  # noqa: N802
        self._exchange("HEAD")

    def do_POST(self) -> None:  # noqa: N802
        self._exchange("POST")

    def do_PUT(self) -> None:  # noqa: N802
        self._exchange("PUT")

    def do_PATCH(self) -> None:  # noqa: N802
        self._exchange("PATCH")

    def do_DELETE(self) -> None:  # noqa: N802
        self._exchange("DELETE")

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._exchange("OPTIONS")

    def _exchange(self, method: str) -> None:
        parsed = urlparse(self.path)
        headers = {key.lower(): value for key, value in self.headers.items()}
        request_id = headers.get("x-request-id") or new_id("req")
        try:
            body = self._read_body(headers)
            request = Request(
                method=method,
                path=parsed.path or "/",
                headers=headers,
                body=body,
                request_id=request_id,
                trace=TraceContext.parse(headers.get("traceparent")) or TraceContext.new(),
                query={key: values[0] for key, values in parse_qs(parsed.query).items() if values},
            )
        except PlatformError as exc:
            self._write(Response(status=exc.status, payload=exc.to_envelope()))
            return
        with bind(request_id=request_id, method=method, path=request.path):
            response = self.app.handle(request)
            self._write(response)
            log.info(
                "http_access",
                extra={"extra": {"status": response.status, "bytes": len(body), "route": request.params}},
            )

    def _read_body(self, headers: dict[str, str]) -> bytes:
        raw_length = headers.get("content-length")
        if not raw_length:
            return b""
        try:
            length = int(raw_length)
        except ValueError as exc:
            from ..errors import ValidationFailed

            raise ValidationFailed("malformed content-length") from exc
        if length > self.app.settings.max_body_bytes:
            from ..errors import QuotaExceeded

            raise QuotaExceeded(
                "request body too large",
                details={"max_body_bytes": self.app.settings.max_body_bytes, "received": length},
            )
        return self.rfile.read(length)

    def _write(self, response: Response) -> None:
        try:
            if response.events is not None:
                self._write_sse(response)
                return
            body = (response.raw if response.raw is not None else json.dumps(response.payload)).encode("utf-8")
            self.send_response(response.status)
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(body)))
            for key, value in response.headers.items():
                self.send_header(key, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def _write_sse(self, response: Response) -> None:
        self.send_response(response.status)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        for key, value in response.headers.items():
            self.send_header(key, value)
        self.end_headers()
        try:
            for event in response.events or ():
                self.wfile.write(f"data: {json.dumps(event)}\n\n".encode("utf-8"))
                self.wfile.flush()
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            close = getattr(response.events, "close", None)
            if close is not None:
                close()
            self.close_connection = True

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """Structured access logging happens in _exchange; keep stderr quiet."""


class PlexusServer(ThreadingHTTPServer):
    """One thread per connection with daemon workers: a stuck request cannot block accepts."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, app: App, *, host: str | None = None, port: int | None = None) -> None:
        self.app = app
        self.bound_host = host or app.settings.host
        self.bound_port = app.settings.port if port is None else port
        super().__init__((self.bound_host, self.bound_port), _HTTPRequestHandler)

    @property
    def url(self) -> str:
        host, port = self.server_address[0], self.server_address[1]
        return f"http://{host}:{port}"

    def serve_background(self) -> threading.Thread:
        thread = threading.Thread(target=self.serve_forever, name="plexus-http", daemon=True)
        thread.start()
        return thread


def serve(app: App, *, host: str | None = None, port: int | None = None) -> None:
    """Blocking server with graceful shutdown on SIGTERM/SIGINT."""
    server = PlexusServer(app, host=host, port=port)
    stop = threading.Event()
    install_signal_handlers(stop)
    log.info(
        "server_listening",
        extra={"extra": {"url": server.url, "env": app.settings.env.value, "providers": app.gateway.providers}},
    )
    thread = server.serve_background()
    stop.wait()
    server.shutdown()
    server.server_close()
    thread.join(timeout=app.settings.shutdown_grace_s)
    log.info("server_stopped")


def install_signal_handlers(stop: threading.Event) -> None:
    """First SIGTERM/SIGINT drains, the next one exits immediately.

    Kubernetes sends SIGTERM then escalates after terminationGracePeriodSeconds; a
    stuck in-flight request must not turn a rolling restart into a crashloop.
    """

    def _handler(signum: int, _frame: Any) -> None:
        if stop.is_set():
            os._exit(128 + signum)
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _handler)
        except ValueError:
            return
