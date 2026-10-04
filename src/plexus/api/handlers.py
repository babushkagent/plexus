"""Business logic for every route, written as plain functions over the composition root.

Handlers deliberately contain no cross-cutting concerns: identity, tenant scoping,
admission control, RBAC, idempotency and error mapping already happened in the edge
pipeline (see `server.py`). A handler can assume `request.auth` exists, that it may
only touch its own tenant, and that raising a `PlatformError` is the correct way to
fail. That is what keeps ~40 routes auditable in one screen.
"""

from __future__ import annotations

import json
import time
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..errors import NotFound, PlatformError, ValidationFailed
from ..llm.provider import CompletionRequest, messages_from_raw
from ..registry.store import DeploymentRepository, ModelRepository, RunRepository
from ..scaling.autoscaler import Autoscaler, LoadSample
from ..telemetry import METRICS
from ..tenancy.auth import TokenIssuer, mint_api_key
from ..tenancy.context import Plan
from ..tenancy.rbac import (
    ROLE_AUDITOR,
    ROLE_ADMIN,
    ROLE_INFERENCE_USER,
    ROLE_ML_ENGINEER,
    ROLE_OWNER,
    ROLE_VIEWER,
    Capability,
)
from ..workflow.engine import TaskQueue
from .messages import Request, Response, created, no_content, respond, stream_response

if TYPE_CHECKING:  # pragma: no cover - imported for typing only, avoids a cycle
    from .server import App

Handler = Callable[["App", Request], Response]

ROLES_BY_NAME: dict[str, str] = {
    ROLE_OWNER: ROLE_OWNER,
    ROLE_ADMIN: ROLE_ADMIN,
    ROLE_ML_ENGINEER: ROLE_ML_ENGINEER,
    ROLE_INFERENCE_USER: ROLE_INFERENCE_USER,
    ROLE_VIEWER: ROLE_VIEWER,
    ROLE_AUDITOR: ROLE_AUDITOR,
}


@dataclass(frozen=True, slots=True)
class RouteSpec:
    """Declarative route entry: the capability is part of the routing table."""

    method: str
    pattern: str
    handler: Handler
    capability: str | None = None
    public: bool = False
    platform: bool = False
    idempotent: bool = True


class StoreLedger:
    """Usage ledger backed by the durable store.

    Three separate short transactions on purpose: a budget check must not hold a
    write lock while a 30s upstream call is in flight, and spend must be recorded
    even when the caller is about to fail for an unrelated reason.
    """

    def __init__(self, app: "App") -> None:
        self._app = app

    def assert_budget(self, tenant_id: str, projected_cost_usd: float) -> None:
        with self._app.uow(tenant_id=tenant_id) as uow:
            uow.tenants.assert_budget(uow.tenants.require(tenant_id), projected_cost_usd)

    def charge(self, tenant_id: str, cost_usd: float) -> None:
        if cost_usd <= 0:
            return
        with self._app.uow(tenant_id=tenant_id, immediate=True) as uow:
            uow.tenants.add_spend(tenant_id, cost_usd)

    def record(
        self,
        tenant_id: str,
        completion: Any,
        *,
        cost_usd: float,
        latency_ms: float,
        request_id: str | None = None,
    ) -> None:
        with self._app.uow(tenant_id=tenant_id, immediate=True) as uow:
            uow.usage.record(
                tenant_id=tenant_id,
                model=completion.model,
                provider=completion.provider,
                request_tokens=completion.usage.prompt_tokens,
                response_tokens=completion.usage.completion_tokens,
                cost_usd=cost_usd,
                latency_ms=latency_ms,
                status_code=200,
                request_id=request_id,
            )


# --------------------------------------------------------------------------- helpers


def _audit(
    app: "App",
    request: Request,
    action: str,
    resource: str = "",
    *,
    details: dict[str, Any] | None = None,
    result: str = "success",
) -> None:
    """Audit trail commits with the request, never inside a handler's own transaction.

    Keeping this out of the caller's transaction means a rolled-back write cannot
    erase the record that someone attempted it.
    """
    ctx = request.auth
    with app.uow(tenant_id=ctx.tenant_id, immediate=True) as uow:
        uow.audit.log(
            tenant_id=ctx.tenant_id,
            actor=ctx.subject,
            action=action,
            resource=resource,
            result=result,
            details=details or {},
            trace_id=request.trace.trace_id if request.trace else None,
        )


def _require_body(request: Request, *fields: str) -> dict[str, Any]:
    body = request.json
    missing = [name for name in fields if not body.get(name)]
    if missing:
        raise ValidationFailed("required field(s) missing", details={"fields": missing})
    return body


def _str(body: dict[str, Any], name: str, default: str | None = None) -> str | None:
    value = body.get(name, default)
    if value is None:
        return None
    if not isinstance(value, (str, int, float)):
        raise ValidationFailed(f"{name} must be a string", details={name: type(value).__name__})
    return str(value)


def _num(body: dict[str, Any], name: str, default: float | None = None) -> float | None:
    value = body.get(name, default)
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, (int, float)):
        raise ValidationFailed(f"{name} must be a number", details={name: value})
    return float(value)


def _int(body: dict[str, Any], name: str, default: int | None = None) -> int | None:
    value = body.get(name, default)
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, int):
        raise ValidationFailed(f"{name} must be an integer", details={name: value})
    return int(value)


def _plan(value: Any) -> Plan:
    try:
        return Plan(str(value))
    except ValueError:
        raise ValidationFailed(
            "unknown plan",
            details={"plan": value, "allowed": [plan.value for plan in Plan]},
        ) from None


def _since_ms(request: Request) -> int:
    days = request.int_arg("days", 30)
    return int((time.time() - days * 86_400) * 1000)


def _tasks(app: "App") -> TaskQueue:
    """One place that turns settings into queue lease/attempt policy.

    Every caller must use the same lease so a crashed worker's tasks become
    visible on a predictable deadline no matter which route enqueued them.
    """
    return TaskQueue(
        app.db,
        lease_s=app.settings.task_lease_s,
        heartbeat_s=app.settings.task_heartbeat_s,
        default_max_attempts=app.settings.task_max_attempts,
    )

# ------------------------------------------------------------------------ liveness


def healthz(app: "App", request: Request) -> Response:
    """Liveness: the process is up. Never touches dependencies."""
    return respond(
        {
            "status": "ok",
            "service": app.settings.service_name,
            "version": _version(),
            "uptime_s": round(time.time() - app.started_at, 3),
        }
    )


def readyz(app: "App", request: Request) -> Response:
    """Readiness: the store answers, so writes can succeed. Drains are still live."""
    try:
        database_up = app.db.ping()
    except Exception:  # noqa: BLE001 - readiness must never raise
        database_up = False
    body = {"status": "ready" if database_up else "not_ready", "database": database_up}
    return respond(body, status=200 if database_up else 503)


# Prometheus scrapes arrive far faster than the backlog changes, and both signals cost
# something (one query, one lock sweep). Replicas share a sample instead of every scrape
# hitting the store. Names here are load-bearing: `plexus_queue_depth` is exactly what
# `scaling.autoscaler.render_external_metric` and the KEDA ScaledObject ask the HPA for.
OPERATIONAL_SAMPLE_TTL_S = 5.0
_BREAKER_SCORES: dict[str, int] = {"closed": 0, "half_open": 1, "open": 2}
_sample_lock = threading.Lock()
_next_sample_at = 0.0


def metrics(app: "App", request: Request) -> Response:
    """Prometheus exposition. Platform-scoped: cardinality is itself a secret."""
    _sample_operational_gauges(app)
    return Response(status=200, raw=METRICS.render_prometheus(), content_type="text/plain; version=0.0.4")


def _sample_operational_gauges(app: "App") -> None:
    """Publish backlog depth and per-provider breaker state for autoscaling/alerting.

    Never raises: an exposition endpoint that 500s turns a partial outage into a blind
    one, and dashboards must keep rendering while the store is unhealthy.
    """
    global _next_sample_at
    now = time.monotonic()
    with _sample_lock:
        if now < _next_sample_at:
            return
        _next_sample_at = now + OPERATIONAL_SAMPLE_TTL_S
    try:
        METRICS.gauge(
            "plexus_queue_depth",
            float(_tasks(app).ready_count()),
            help_text="Tasks that are pending and past their run_after deadline",
        )
    except Exception:  # noqa: BLE001 - metrics must survive an unhealthy store
        pass
    try:
        for name, state in _breaker_states(app).items():
            METRICS.gauge(
                "plexus_circuit_breaker_state",
                float(_BREAKER_SCORES.get(state, 2)),
                labels={"provider": name},
                help_text="Provider breaker state: 0 closed, 1 half-open, 2 open",
            )
    except Exception:  # noqa: BLE001 - a missing gateway must not break the scrape
        pass


def _breaker_states(app: "App") -> dict[str, str]:
    providers = app.gateway.health().get("providers", {})
    return {str(name): str(state.get("breaker", "open")) for name, state in providers.items()}


# -------------------------------------------------------------------------- tenant


def get_tenant(app: "App", request: Request) -> Response:
    with app.uow() as uow:
        tenant = uow.tenants.require(request.tenant_id)
    limits = app.policy_for(tenant.id, request.auth.limits).limits
    return respond(
        {
            **_tenant_dict(tenant),
            "effective_limits": {
                "requests_per_second": limits.requests_per_second,
                "burst": limits.burst,
                "max_concurrency": limits.max_concurrency,
                "monthly_budget_usd": limits.monthly_budget_usd,
                "max_model_versions": limits.max_model_versions,
                "max_replicas": limits.max_replicas,
            },
        }
    )


def update_tenant(app: "App", request: Request) -> Response:
    """Self-service profile edits. Limits and plan are platform-only on purpose."""
    body = _require_body(request)
    name = _str(body, "name")
    settings = body.get("settings")
    if name is None and settings is None:
        raise ValidationFailed("nothing to update", details={"fields": ["name", "settings"]})
    if not isinstance(settings, dict) and settings is not None:
        raise ValidationFailed("settings must be an object")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        current = uow.tenants.require(request.tenant_id)
        merged = dict(current.settings)
        merged.update(settings or {})
        uow.tx.execute(
            "UPDATE tenants SET name = ?, settings_json = ?, updated_at_ms = ? WHERE id = ?",
            (
                name if name is not None else current.name,
                _dumps(merged),
                int(time.time() * 1000),
                request.tenant_id,
            ),
        )
        tenant = uow.tenants.require(request.tenant_id)
    _audit(app, request, "tenant.update", request.tenant_id, details={"fields": sorted(body)})
    return respond(_tenant_dict(tenant))


def create_tenant(app: "App", request: Request) -> Response:
    body = _require_body(request, "name")
    plan = _plan(body.get("plan", Plan.STANDARD.value))
    with app.uow(immediate=True) as uow:
        tenant = uow.tenants.create(
            name=str(body["name"]),
            plan=plan,
            tenant_id=_str(body, "tenant_id"),
            rps_limit=_num(body, "rps_limit"),
            burst=_num(body, "burst"),
            max_concurrency=_int(body, "max_concurrency"),
            monthly_budget_usd=_num(body, "monthly_budget_usd"),
            settings=body.get("settings") if isinstance(body.get("settings"), dict) else None,
        )
        uow.outbox.append(
            tenant_id=tenant.id,
            type="tenant.created",
            subject=tenant.id,
            payload={"plan": tenant.plan.value, "name": tenant.name},
        )
    _audit(app, request, "tenant.create", tenant.id, details={"plan": tenant.plan.value})
    return created(_tenant_dict(tenant))


def list_tenants(app: "App", request: Request) -> Response:
    with app.uow() as uow:
        return respond({"items": [_tenant_dict(t) for t in uow.tenants.list()]})


def get_platform_tenant(app: "App", request: Request) -> Response:
    tenant_id = request.param("tenant_id")
    with app.uow() as uow:
        return respond(_tenant_dict(uow.tenants.require(tenant_id)))


def update_platform_tenant(app: "App", request: Request) -> Response:
    """Plan, status and per-tenant limit overrides.

    Policy cache is invalidated after the commit so a raised rate limit takes effect
    in milliseconds while the hot path stays O(1) and cache-backed.
    """
    tenant_id = request.param("tenant_id")
    body = _require_body(request)
    status = _str(body, "status")
    if status is not None and status not in {"active", "suspended", "archived"}:
        raise ValidationFailed("unknown status", details={"status": status})
    with app.uow(tenant_id=tenant_id, immediate=True) as uow:
        current = uow.tenants.require(tenant_id)
        plan = _plan(body["plan"]) if "plan" in body else current.plan
        uow.tx.execute(
            """
            UPDATE tenants SET plan = ?, status = ?, rps_limit = ?, burst = ?, max_concurrency = ?,
                   monthly_budget_usd = ?, updated_at_ms = ?
             WHERE id = ?
            """,
            (
                plan.value,
                status if status is not None else current.status,
                _num(body, "rps_limit") if "rps_limit" in body else current.rps_limit,
                _num(body, "burst") if "burst" in body else current.burst,
                _int(body, "max_concurrency") if "max_concurrency" in body else current.max_concurrency,
                _num(body, "monthly_budget_usd") if "monthly_budget_usd" in body else current.monthly_budget_usd,
                int(time.time() * 1000),
                tenant_id,
            ),
        )
        if status is not None:
            uow.outbox.append(
                tenant_id=tenant_id,
                type="tenant.status_changed",
                subject=tenant_id,
                payload={"status": status},
            )
        tenant = uow.tenants.require(tenant_id)
    app.invalidate_policy(tenant_id)
    _audit(app, request, "tenant.update_platform", tenant_id, details={"fields": sorted(body)})
    return respond(_tenant_dict(tenant))


# --------------------------------------------------------------------------- keys


def create_key(app: "App", request: Request, *, tenant_id: str | None = None) -> Response:
    body = _require_body(request)
    owner = tenant_id or request.tenant_id
    roles = _roles(body.get("roles"))
    capabilities = body.get("capabilities")
    if not isinstance(capabilities, list):
        capabilities = []
    minted = mint_api_key(
        tenant_id=owner,
        subject=str(body.get("subject") or request.auth.subject),
        plan=_plan(body.get("plan", request.auth.plan.value)),
        roles=roles,
        capabilities=[str(cap) for cap in capabilities],
        label=str(body.get("label") or ""),
        pepper=app.settings.api_key_pepper,
    )
    with app.uow(tenant_id=owner, immediate=True) as uow:
        uow.keys.add(minted.record)
        for role in roles:
            uow.memberships.grant(owner, minted.record.subject, role)
        uow.outbox.append(
            tenant_id=owner,
            type="api_key.created",
            subject=minted.record.id,
            payload={"label": minted.record.label, "roles": list(roles)},
        )
    _audit(app, request, "key.create", minted.record.id, details={"roles": list(roles)})
    # The only time the secret exists anywhere: it is never recoverable afterwards.
    return created(
        {
            "id": minted.record.id,
            "secret": minted.secret,
            "tenant_id": owner,
            "subject": minted.record.subject,
            "roles": list(roles),
            "label": minted.record.label,
            "warning": "store this value now; only a salted hash is retained",
        }
    )


def list_keys(app: "App", request: Request, *, tenant_id: str | None = None) -> Response:
    owner = tenant_id or request.tenant_id
    with app.uow(tenant_id=owner) as uow:
        return respond({"items": [_key_dict(record) for record in uow.keys.list(owner)]})


def revoke_key(app: "App", request: Request, *, tenant_id: str | None = None) -> Response:
    owner = tenant_id or request.tenant_id
    key_id = request.param("key_id")
    with app.uow(tenant_id=owner, immediate=True) as uow:
        if uow.keys.get(owner, key_id) is None:
            raise NotFound("api key not found", details={"key_id": key_id})
        uow.keys.revoke(owner, key_id)
        uow.outbox.append(tenant_id=owner, type="api_key.revoked", subject=key_id)
    _audit(app, request, "key.revoke", key_id)
    return no_content()


def issue_token(app: "App", request: Request) -> Response:
    """Short-lived JWT for a human or workload inside the caller's tenant."""
    body = _require_body(request)
    ctx = request.auth
    token = TokenIssuer(app.settings).issue(
        tenant_id=ctx.tenant_id,
        subject=str(body.get("subject") or ctx.subject),
        roles=_roles(body.get("roles")) or list(ctx.roles),
        plan=ctx.plan,
        ttl_s=min(int(body.get("ttl_s", 900)), 86_400),
    )
    _audit(app, request, "token.issue", str(body.get("subject") or ctx.subject))
    return created({"access_token": token, "token_type": "Bearer", "scope": ctx.tenant_id})


# ------------------------------------------------------------------------ members


def list_members(app: "App", request: Request, *, tenant_id: str | None = None) -> Response:
    owner = tenant_id or request.tenant_id
    subject = request.arg("subject") or request.auth.subject
    with app.uow(tenant_id=owner) as uow:
        roles = list(uow.memberships.roles_for(owner, subject))
    return respond({"subject": subject, "roles": roles, "available": sorted(ROLES_BY_NAME)})


def grant_member(app: "App", request: Request, *, tenant_id: str | None = None) -> Response:
    owner = tenant_id or request.tenant_id
    subject = request.param("subject")
    body = _require_body(request, "role")
    role = _require_role(body["role"])
    with app.uow(tenant_id=owner, immediate=True) as uow:
        uow.memberships.grant(owner, subject, role)
        uow.outbox.append(
            tenant_id=owner, type="member.granted", subject=subject, payload={"role": role}
        )
    _audit(app, request, "member.grant", subject, details={"role": role})
    return created({"subject": subject, "role": role})


def revoke_member(app: "App", request: Request, *, tenant_id: str | None = None) -> Response:
    owner = tenant_id or request.tenant_id
    subject = request.param("subject")
    body = _require_body(request, "role")
    role = _require_role(body["role"])
    with app.uow(tenant_id=owner, immediate=True) as uow:
        uow.memberships.revoke(owner, subject, role)
        uow.outbox.append(
            tenant_id=owner, type="member.revoked", subject=subject, payload={"role": role}
        )
    _audit(app, request, "member.revoke", subject, details={"role": role})
    return no_content()


# ---------------------------------------------------------------------- inference


def create_completion(app: "App", request: Request) -> Response:
    """OpenAI-compatible chat completion with hashing, fallback, budget and billing.

    Streaming is decided before the first byte is sent: `gateway.stream` pulls one
    chunk synchronously, so an outage is still a normal 5xx JSON error rather than a
    half-written 200. Once bytes flow, the response is committed and a later failure
    ends the stream instead of silently retrying and duplicating spend.

    Completions are audited like every other state change because they spend money:
    the record carries who called, which provider answered, how many fallbacks it
    took and what it cost -- never the prompt or completion text, so the audit log
    cannot become a second store of customer content.
    """
    body = _require_body(request, "messages")
    completion_request = _completion_request(app, body)
    request_id = request.request_id

    if completion_request.stream:
        handle = app.gateway.stream(
            request.tenant_id,
            completion_request,
            request_id=request_id,
        )
        _audit(
            app,
            request,
            "inference.completion",
            completion_request.model,
            details={"provider": handle.provider, "stream": True},
        )
        return stream_response(_sse_events(handle, completion_request.model, request_id=request_id))

    try:
        result = app.gateway.route(
            request.tenant_id,
            completion_request,
            request_id=request_id,
        )
    except PlatformError as exc:
        # A failed route is still a security-relevant event: every candidate may be
        # down, or a tenant may have just tripped its budget. Record and re-raise so
        # the edge still maps the error to the caller's status code.
        _audit(
            app,
            request,
            "inference.completion",
            completion_request.model,
            details={"stream": False, "code": exc.code},
            result="failure",
        )
        raise
    METRICS.counter("plexus_completions_total", labels={"model": completion_request.model})
    _audit(
        app,
        request,
        "inference.completion",
        completion_request.model,
        details={
            "provider": result.provider,
            "stream": False,
            "fallbacks": result.fallbacks,
            "cost_usd": round(result.cost_usd, 8),
            "request_tokens": result.completion.usage.prompt_tokens,
            "response_tokens": result.completion.usage.completion_tokens,
        },
    )
    # Request identity is stamped here, not in the router: the same completion replayed
    # through a different request keeps its billing fields but adopts the new trace.
    return respond({"id": f"chatcmpl-{request_id}", "created": int(time.time()), **result.to_dict()})


def _completion_request(app: "App", body: dict[str, Any]) -> CompletionRequest:
    raw_messages = body.get("messages")
    if not isinstance(raw_messages, list) or not raw_messages:
        raise ValidationFailed("messages must be a non-empty array")
    for message in raw_messages:
        if not isinstance(message, dict) or "role" not in message or "content" not in message:
            raise ValidationFailed("each message requires role and content")
    completion_request = CompletionRequest(
        model=str(body.get("model") or app.settings.default_model),
        messages=messages_from_raw(raw_messages),
        max_tokens=_int(body, "max_tokens"),
        temperature=float(body.get("temperature", 0.0) or 0.0),
        stream=bool(body.get("stream", False)),
    )
    if completion_request.prompt_chars > app.settings.max_input_chars:
        raise ValidationFailed(
            "prompt exceeds the maximum input size",
            details={
                "prompt_chars": completion_request.prompt_chars,
                "max_input_chars": app.settings.max_input_chars,
            },
        )
    return completion_request


def _sse_events(handle: Any, model: str, *, request_id: str = "unknown") -> Iterator[dict[str, Any]]:
    """Server-Sent Events in the OpenAI chunk schema.

    Every event is a `chat.completion.chunk` so existing streaming clients parse it;
    provider, attempts and cost ride along under `plexus` and are ignored by them.
    """

    def chunk(delta: str | None, finish_reason: str | None, *, final: bool = False) -> dict[str, Any]:
        extension: dict[str, Any] = {"provider": handle.provider}
        if final:
            extension["attempts"] = [attempt.to_dict() for attempt in handle.attempts]
            extension["text"] = handle.text
        return {
            "id": f"chatcmpl-{request_id}",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {"index": 0, "delta": {"content": delta} if delta else {}, "finish_reason": finish_reason}
            ],
            "plexus": extension,
        }

    def generate() -> Iterator[dict[str, Any]]:
        for piece in handle:
            yield chunk(piece, None)
        yield chunk(None, "stop", final=True)

    return generate()


def list_models(app: "App", request: Request) -> Response:
    """Registry view for this tenant (never another tenant's models, by construction)."""
    with app.uow(tenant_id=request.tenant_id) as uow:
        versions = ModelRepository(uow.tx, outbox=uow.outbox).list(
            tenant_id=request.tenant_id,
            name=request.arg("name"),
            stage=request.arg("stage"),
            limit=request.int_arg("limit", 50),
        )
    return respond(
        {
            "items": [version.to_dict() for version in versions],
            "enabled_models": list(app.settings.allowed_models),
        }
    )


# ----------------------------------------------------------------------- registry


def register_model(app: "App", request: Request) -> Response:
    body = _require_body(request, "name", "digest")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        version, created_flag = ModelRepository(uow.tx, outbox=uow.outbox).register(
            tenant_id=request.tenant_id,
            name=str(body["name"]),
            digest=str(body["digest"]),
            created_by=str(body.get("created_by") or request.auth.subject),
            parent_id=_str(body, "parent_id"),
            size_bytes=int(body.get("size_bytes", 0) or 0),
            metadata=body.get("metadata") if isinstance(body.get("metadata"), dict) else None,
            uri=_str(body, "uri"),
            kind=str(body.get("kind", "weights")),
            max_versions=request.auth.limits.max_model_versions,
        )
    _audit(
        app,
        request,
        "model.register",
        version.id,
        details={"name": version.name, "version": version.version, "reused": not created_flag},
    )
    return created(version.to_dict()) if created_flag else respond(version.to_dict())


def get_model(app: "App", request: Request) -> Response:
    version_id = request.param("version_id")
    with app.uow(tenant_id=request.tenant_id) as uow:
        version = ModelRepository(uow.tx).require(request.tenant_id, version_id)
        artifacts = ModelRepository(uow.tx).artifacts(tenant_id=request.tenant_id, digest=version.digest)
    return respond({**version.to_dict(), "artifacts": [artifact.to_dict() for artifact in artifacts]})


def model_lineage(app: "App", request: Request) -> Response:
    version_id = request.param("version_id")
    with app.uow(tenant_id=request.tenant_id) as uow:
        chain = ModelRepository(uow.tx).lineage(tenant_id=request.tenant_id, version_id=version_id)
    return respond({"items": [version.to_dict() for version in chain]})


def record_model_eval(app: "App", request: Request) -> Response:
    body = _require_body(request, "passed")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        version = ModelRepository(uow.tx, outbox=uow.outbox).record_eval(
            tenant_id=request.tenant_id,
            version_id=request.param("version_id"),
            passed=bool(body["passed"]),
            metrics=body.get("metrics") if isinstance(body.get("metrics"), dict) else None,
        )
    _audit(app, request, "model.eval", version.id, details={"passed": version.eval_passed})
    return respond(version.to_dict())


def sign_model(app: "App", request: Request) -> Response:
    body = _require_body(request, "signature")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        version = ModelRepository(uow.tx).sign(
            tenant_id=request.tenant_id,
            version_id=request.param("version_id"),
            signature=str(body["signature"]),
        )
    _audit(app, request, "model.sign", version.id)
    return respond(version.to_dict())


def promote_model(app: "App", request: Request) -> Response:
    """Stage transitions. Traffic stages require eval + signature; the store enforces it."""
    body = _require_body(request, "stage")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        version = ModelRepository(uow.tx, outbox=uow.outbox).promote(
            tenant_id=request.tenant_id,
            version_id=request.param("version_id"),
            stage=str(body["stage"]),
            actor=request.auth.subject,
        )
    _audit(app, request, "model.promote", version.id, details={"stage": version.stage})
    return respond(version.to_dict())


def attach_artifact(app: "App", request: Request) -> Response:
    body = _require_body(request, "digest", "kind", "uri")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        artifact = ModelRepository(uow.tx).attach_artifact(
            tenant_id=request.tenant_id,
            digest=str(body["digest"]),
            kind=str(body["kind"]),
            uri=str(body["uri"]),
            size_bytes=int(body.get("size_bytes", 0) or 0),
            metadata=body.get("metadata") if isinstance(body.get("metadata"), dict) else None,
        )
    _audit(app, request, "model.artifact", artifact.id, details={"kind": artifact.kind})
    return created(artifact.to_dict())


def list_artifacts(app: "App", request: Request) -> Response:
    digest = request.require_arg("digest")
    with app.uow(tenant_id=request.tenant_id) as uow:
        artifacts = ModelRepository(uow.tx).artifacts(tenant_id=request.tenant_id, digest=digest)
    return respond({"items": [artifact.to_dict() for artifact in artifacts]})


# -------------------------------------------------------------------- deployments


def upsert_deployment(app: "App", request: Request) -> Response:
    body = _require_body(request, "name", "model_version_id")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        deployment = DeploymentRepository(uow.tx, outbox=uow.outbox).upsert(
            tenant_id=request.tenant_id,
            name=str(body["name"]),
            model_version_id=str(body["model_version_id"]),
            min_replicas=int(body.get("min_replicas", 1)),
            max_replicas=int(body.get("max_replicas", 3)),
            desired_replicas=_int(body, "desired_replicas"),
            traffic_percent=int(body.get("traffic_percent", 100)),
            max_replicas_allowed=request.auth.limits.max_replicas,
        )
    _audit(app, request, "deployment.upsert", deployment.id, details={"name": deployment.name})
    return created(deployment.to_dict())


def list_deployments(app: "App", request: Request) -> Response:
    with app.uow(tenant_id=request.tenant_id) as uow:
        items = DeploymentRepository(uow.tx).list(tenant_id=request.tenant_id, limit=request.int_arg("limit", 50))
    return respond({"items": [deployment.to_dict() for deployment in items]})


def get_deployment(app: "App", request: Request) -> Response:
    with app.uow(tenant_id=request.tenant_id) as uow:
        deployment = DeploymentRepository(uow.tx).require(request.tenant_id, request.param("deployment_id"))
    return respond(deployment.to_dict())


def scale_deployment(app: "App", request: Request) -> Response:
    body = _require_body(request, "replicas")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        deployment = DeploymentRepository(uow.tx, outbox=uow.outbox).scale(
            request.tenant_id, request.param("deployment_id"), int(body["replicas"])
        )
    _audit(app, request, "deployment.scale", deployment.id, details={"replicas": deployment.desired_replicas})
    return respond(deployment.to_dict())


def set_deployment_traffic(app: "App", request: Request) -> Response:
    body = _require_body(request, "traffic_percent")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        deployment = DeploymentRepository(uow.tx, outbox=uow.outbox).set_traffic(
            request.tenant_id, request.param("deployment_id"), int(body["traffic_percent"])
        )
    _audit(app, request, "deployment.traffic", deployment.id, details={"percent": deployment.traffic_percent})
    return respond(deployment.to_dict())


def rollback_deployment(app: "App", request: Request) -> Response:
    body = _require_body(request, "model_version_id")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        deployment = DeploymentRepository(uow.tx, outbox=uow.outbox).rollback(
            tenant_id=request.tenant_id,
            deployment_id=request.param("deployment_id"),
            model_version_id=str(body["model_version_id"]),
        )
    _audit(app, request, "deployment.rollback", deployment.id, details={"to": deployment.model_version_id})
    return respond(deployment.to_dict())


# --------------------------------------------------------------------------- runs


def start_run(app: "App", request: Request) -> Response:
    body = _require_body(request, "kind")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        run = RunRepository(uow.tx).start(
            tenant_id=request.tenant_id,
            kind=str(body["kind"]),
            model_version_id=_str(body, "model_version_id"),
            metrics=body.get("metrics") if isinstance(body.get("metrics"), dict) else None,
        )
    _audit(app, request, "run.start", run.id, details={"kind": run.kind})
    return created(run.to_dict())


def finish_run(app: "App", request: Request) -> Response:
    body = _require_body(request, "status")
    with app.uow(tenant_id=request.tenant_id, immediate=True) as uow:
        run = RunRepository(uow.tx).finish(
            tenant_id=request.tenant_id,
            run_id=request.param("run_id"),
            status=str(body["status"]),
            metrics=body.get("metrics") if isinstance(body.get("metrics"), dict) else None,
            error=_str(body, "error"),
        )
    _audit(app, request, "run.finish", run.id, details={"status": run.status})
    return respond(run.to_dict())


def list_runs(app: "App", request: Request) -> Response:
    with app.uow(tenant_id=request.tenant_id) as uow:
        runs = RunRepository(uow.tx).list(
            tenant_id=request.tenant_id,
            status=request.arg("status"),
            limit=request.int_arg("limit", 50),
        )
    return respond({"items": [run.to_dict() for run in runs]})


def get_run(app: "App", request: Request) -> Response:
    with app.uow(tenant_id=request.tenant_id) as uow:
        run = RunRepository(uow.tx).require(request.tenant_id, request.param("run_id"))
    return respond(run.to_dict())


# -------------------------------------------------------------------------- tasks


def enqueue_task(app: "App", request: Request) -> Response:
    body = _require_body(request, "type")
    task_id = _tasks(app).enqueue(
        tenant_id=request.tenant_id,
        type=str(body["type"]),
        payload=body.get("payload") if isinstance(body.get("payload"), dict) else None,
        dedupe_key=_str(body, "dedupe_key"),
        delay_s=float(body.get("delay_s", 0.0) or 0.0),
        max_attempts=_int(body, "max_attempts"),
    )
    if task_id is None:
        # Deduplication is a successful no-op: the caller's intent is already queued.
        return respond({"status": "deduplicated"}, status=200)
    _audit(app, request, "task.enqueue", task_id, details={"type": body["type"]})
    return created({"id": task_id, "status": "pending"})


def get_task(app: "App", request: Request) -> Response:
    task = _tasks(app).get(request.param("task_id"))
    if task is None or task.tenant_id != request.tenant_id:
        # A foreign task id must be indistinguishable from a missing one.
        raise NotFound("task not found", details={"task_id": request.param("task_id")})
    return respond(task.to_dict())


def cancel_task(app: "App", request: Request) -> Response:
    cancelled = _tasks(app).cancel(
        request.param("task_id"), tenant_id=request.tenant_id
    )
    if not cancelled:
        raise NotFound("task not found or already finished", details={"task_id": request.param("task_id")})
    _audit(app, request, "task.cancel", request.param("task_id"))
    return no_content()


def list_dead_letters(app: "App", request: Request) -> Response:
    tasks = _tasks(app).dead_letters(
        tenant_id=request.tenant_id, limit=request.int_arg("limit", 50)
    )
    return respond({"items": [task.to_dict() for task in tasks]})


def requeue_dead_letter(app: "App", request: Request) -> Response:
    requeued = _tasks(app).requeue_dead(
        request.param("task_id"), tenant_id=request.tenant_id
    )
    if not requeued:
        raise NotFound("dead letter not found", details={"task_id": request.param("task_id")})
    _audit(app, request, "task.requeue", request.param("task_id"))
    return respond({"id": request.param("task_id"), "status": "pending"})


# --------------------------------------------------------------- usage and audit


def get_usage(app: "App", request: Request) -> Response:
    with app.uow(tenant_id=request.tenant_id) as uow:
        summary = uow.usage.summarize(request.tenant_id, since_ms=_since_ms(request))
        tenant = uow.tenants.require(request.tenant_id)
    summary["cost_usd"] = round(float(summary.get("cost_usd", 0.0)), 6)
    summary["monthly_spend_usd"] = round(tenant.monthly_spend_usd, 6)
    summary["monthly_budget_usd"] = tenant.monthly_budget_usd
    return respond({"window_days": request.int_arg("days", 30), **summary})


def list_audit(app: "App", request: Request) -> Response:
    with app.uow(tenant_id=request.tenant_id) as uow:
        entries = uow.audit.list(request.tenant_id, limit=request.int_arg("limit", 50))
    return respond({"items": entries})


# ----------------------------------------------------------------- platform plane


def platform_health(app: "App", request: Request) -> Response:
    gateway = app.gateway.health()
    queue = _tasks(app).depth()
    return respond(
        {
            "status": "ok",
            "database": app.db.ping(),
            "uptime_s": round(time.time() - app.started_at, 3),
            "gateway": gateway,
            "queue": queue,
            "in_flight": app.limits.in_flight(),
        }
    )


def platform_scaling(app: "App", request: Request) -> Response:
    """Deterministic view of the autoscaler: same signals always give same decision."""
    queue = _tasks(app).depth()
    autoscaler = Autoscaler.from_settings(app.settings)
    sample = LoadSample.from_queue(queue, replicas=autoscaler.desired)
    decision = autoscaler.decide(sample)
    return respond(
        {
            "api": {
                "desired": decision.desired,
                "previous": decision.previous,
                "reason": decision.reason,
                "signals": decision.signals,
                "min_replicas": app.settings.api_min_replicas,
                "max_replicas": app.settings.api_max_replicas,
                "target_queue_depth": app.settings.target_queue_depth,
            },
            "queue": queue,
            "ring": list(app.gateway.health()["ring"]),
        }
    )


def platform_queue(app: "App", request: Request) -> Response:
    return respond({"depth": _tasks(app).depth()})


# ------------------------------------------------------------------------ routing


def _tenant_dict(tenant: Any) -> dict[str, Any]:
    return {
        "id": tenant.id,
        "name": tenant.name,
        "plan": tenant.plan.value,
        "status": tenant.status,
        "rps_limit": tenant.rps_limit,
        "burst": tenant.burst,
        "max_concurrency": tenant.max_concurrency,
        "monthly_budget_usd": tenant.monthly_budget_usd,
        "monthly_spend_usd": round(tenant.monthly_spend_usd, 6),
        "settings": tenant.settings,
        "created_at_ms": tenant.created_at_ms,
    }


def _key_dict(record: Any) -> dict[str, Any]:
    return {
        "id": record.id,
        "subject": record.subject,
        "label": record.label,
        "roles": list(record.roles),
        "capabilities": sorted(record.capabilities),
        "last4": record.last4,
        "use_count": record.use_count,
        "expires_at": record.expires_at,
        "revoked": record.revoked_at is not None,
    }


def _roles(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValidationFailed("roles must be an array")
    return tuple(_require_role(role) for role in value)


def _require_role(value: Any) -> str:
    role = str(value)
    if role not in ROLES_BY_NAME:
        raise ValidationFailed("unknown role", details={"role": role, "allowed": sorted(ROLES_BY_NAME)})
    return role


def _dumps(value: dict[str, Any]) -> str:
    return json.dumps(value or {}, separators=(",", ":"), sort_keys=True)


def _version() -> str:
    from .. import __version__

    return __version__


PLATFORM_TENANT_PATH = "/v1/platform/tenants/{tenant_id}"

ROUTES: tuple[RouteSpec, ...] = (
    # Unauthenticated: probes only. Everything else fails closed.
    RouteSpec("GET", "/healthz", healthz, public=True),
    RouteSpec("GET", "/readyz", readyz, public=True),
    RouteSpec("GET", "/metrics", metrics, platform=True),
    # Self-service tenant plane
    RouteSpec("GET", "/v1/tenant", get_tenant, Capability.TENANT_READ),
    RouteSpec("PATCH", "/v1/tenant", update_tenant, Capability.TENANT_WRITE),
    RouteSpec("POST", "/v1/keys", create_key, Capability.KEY_MANAGE),
    RouteSpec("GET", "/v1/keys", list_keys, Capability.KEY_MANAGE),
    RouteSpec("DELETE", "/v1/keys/{key_id}", revoke_key, Capability.KEY_MANAGE),
    RouteSpec("POST", "/v1/tokens", issue_token, Capability.KEY_MANAGE),
    RouteSpec("GET", "/v1/members", list_members, Capability.MEMBER_MANAGE),
    RouteSpec("PUT", "/v1/members/{subject}", grant_member, Capability.MEMBER_MANAGE),
    RouteSpec("DELETE", "/v1/members/{subject}", revoke_member, Capability.MEMBER_MANAGE),
    # Data plane
    RouteSpec("POST", "/v1/chat/completions", create_completion, Capability.INFERENCE_CALL, idempotent=False),
    RouteSpec("GET", "/v1/models", list_models, Capability.MODEL_READ),
    RouteSpec("POST", "/v1/models", register_model, Capability.MODEL_WRITE),
    RouteSpec("GET", "/v1/artifacts", list_artifacts, Capability.MODEL_READ),
    RouteSpec("POST", "/v1/artifacts", attach_artifact, Capability.MODEL_WRITE),
    RouteSpec("GET", "/v1/models/{version_id}", get_model, Capability.MODEL_READ),
    RouteSpec("GET", "/v1/models/{version_id}/lineage", model_lineage, Capability.MODEL_READ),
    RouteSpec("POST", "/v1/models/{version_id}/eval", record_model_eval, Capability.MODEL_WRITE),
    RouteSpec("POST", "/v1/models/{version_id}/sign", sign_model, Capability.MODEL_WRITE),
    RouteSpec("POST", "/v1/models/{version_id}/promote", promote_model, Capability.MODEL_PROMOTE),
    RouteSpec("POST", "/v1/deployments", upsert_deployment, Capability.DEPLOY_WRITE),
    RouteSpec("GET", "/v1/deployments", list_deployments, Capability.DEPLOY_READ),
    RouteSpec("GET", "/v1/deployments/{deployment_id}", get_deployment, Capability.DEPLOY_READ),
    RouteSpec("POST", "/v1/deployments/{deployment_id}/scale", scale_deployment, Capability.DEPLOY_WRITE),
    RouteSpec("POST", "/v1/deployments/{deployment_id}/traffic", set_deployment_traffic, Capability.DEPLOY_WRITE),
    RouteSpec("POST", "/v1/deployments/{deployment_id}/rollback", rollback_deployment, Capability.DEPLOY_WRITE),
    RouteSpec("POST", "/v1/runs", start_run, Capability.RUN_WRITE),
    RouteSpec("GET", "/v1/runs", list_runs, Capability.RUN_READ),
    RouteSpec("GET", "/v1/runs/{run_id}", get_run, Capability.RUN_READ),
    RouteSpec("POST", "/v1/runs/{run_id}/finish", finish_run, Capability.RUN_WRITE),
    RouteSpec("POST", "/v1/tasks", enqueue_task, Capability.RUN_WRITE),
    RouteSpec("GET", "/v1/dead-letters", list_dead_letters, Capability.RUN_READ),
    RouteSpec("POST", "/v1/dead-letters/{task_id}/requeue", requeue_dead_letter, Capability.RUN_WRITE),
    RouteSpec("GET", "/v1/tasks/{task_id}", get_task, Capability.RUN_READ),
    RouteSpec("POST", "/v1/tasks/{task_id}/cancel", cancel_task, Capability.RUN_WRITE),
    RouteSpec("GET", "/v1/usage", get_usage, Capability.USAGE_READ),
    RouteSpec("GET", "/v1/audit", list_audit, Capability.AUDIT_READ),
    # Platform plane: separate identity class, never reachable with a tenant token.
    RouteSpec("POST", "/v1/platform/tenants", create_tenant, platform=True),
    RouteSpec("GET", "/v1/platform/tenants", list_tenants, platform=True),
    RouteSpec("GET", PLATFORM_TENANT_PATH, get_platform_tenant, platform=True),
    RouteSpec("PATCH", PLATFORM_TENANT_PATH, update_platform_tenant, platform=True),
    RouteSpec("POST", f"{PLATFORM_TENANT_PATH}/keys", lambda app, request: create_key(app, request, tenant_id=request.param("tenant_id")), platform=True),
    RouteSpec("GET", f"{PLATFORM_TENANT_PATH}/keys", lambda app, request: list_keys(app, request, tenant_id=request.param("tenant_id")), platform=True),
    RouteSpec("DELETE", f"{PLATFORM_TENANT_PATH}/keys/{{key_id}}", lambda app, request: revoke_key(app, request, tenant_id=request.param("tenant_id")), platform=True),
    RouteSpec("GET", f"{PLATFORM_TENANT_PATH}/members", lambda app, request: list_members(app, request, tenant_id=request.param("tenant_id")), platform=True),
    RouteSpec("PUT", f"{PLATFORM_TENANT_PATH}/members/{{subject}}", lambda app, request: grant_member(app, request, tenant_id=request.param("tenant_id")), platform=True),
    RouteSpec("DELETE", f"{PLATFORM_TENANT_PATH}/members/{{subject}}", lambda app, request: revoke_member(app, request, tenant_id=request.param("tenant_id")), platform=True),
    RouteSpec("GET", "/v1/platform/health", platform_health, platform=True),
    RouteSpec("GET", "/v1/platform/scaling", platform_scaling, platform=True),
    RouteSpec("GET", "/v1/platform/queue", platform_queue, platform=True),
)
