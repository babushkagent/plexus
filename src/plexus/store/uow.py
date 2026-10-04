"""Unit of work: one transaction, one tenant scope, one outbox.

Repositories never commit; the UnitOfWork does. Domain events are written to the
outbox in the same transaction as the state change and published afterwards, so
side effects are at-least-once and never describe a rollback'd write.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any

from ..config import Settings
from ..errors import Conflict, IdempotencyConflict, NotFound, QuotaExceeded
from ..ids import new_id
from ..tenancy.auth import ApiKeyRecord
from ..tenancy.context import Plan
from .db import Database, Tx


def now_ms() -> int:
    return int(time.time() * 1000)


def _dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, separators=(",", ":"), sort_keys=True)


@dataclass(frozen=True, slots=True)
class Tenant:
    id: str
    name: str
    plan: Plan
    status: str
    rps_limit: float | None
    burst: float | None
    max_concurrency: int | None
    monthly_budget_usd: float | None
    monthly_spend_usd: float
    settings: dict[str, Any] = field(default_factory=dict)
    created_at_ms: int = 0
    updated_at_ms: int = 0

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Tenant:
        return cls(
            id=row["id"],
            name=row["name"],
            plan=Plan(row["plan"]),
            status=row["status"],
            rps_limit=row["rps_limit"],
            burst=row["burst"],
            max_concurrency=row["max_concurrency"],
            monthly_budget_usd=row["monthly_budget_usd"],
            monthly_spend_usd=float(row["monthly_spend_usd"] or 0.0),
            settings=json.loads(row["settings_json"] or "{}"),
            created_at_ms=int(row["created_at_ms"]),
            updated_at_ms=int(row["updated_at_ms"]),
        )


@dataclass(frozen=True, slots=True)
class IdempotencyOutcome:
    kind: str  # replay | conflict | in_progress | fresh
    status_code: int | None = None
    body: Any = None


class EventSink:
    """Post-commit fan-out. Replace with NATS/Kafka by implementing publish()."""

    def __init__(self) -> None:
        self._subscribers: list[tuple[str, Callable[[dict[str, Any]], None]]] = []
        self.published: list[dict[str, Any]] = []
        self.failures: list[dict[str, Any]] = []

    def subscribe(self, pattern: str, handler: Callable[[dict[str, Any]], None]) -> None:
        self._subscribers.append((pattern, handler))

    def publish(self, events: Sequence[dict[str, Any]]) -> None:
        for event in events:
            self.published.append(event)
            for pattern, handler in list(self._subscribers):
                if pattern != "*" and not event["type"].startswith(pattern):
                    continue
                try:
                    handler(event)
                except Exception:
                    # Delivery is at-least-once; a redelivery loop owns retries, not the writer.
                    self.failures.append(event)


class TenantRepository:
    def __init__(self, tx: Tx) -> None:
        self._tx = tx

    def create(
        self,
        *,
        name: str,
        plan: Plan = Plan.STANDARD,
        tenant_id: str | None = None,
        rps_limit: float | None = None,
        burst: float | None = None,
        max_concurrency: int | None = None,
        monthly_budget_usd: float | None = None,
        settings: dict[str, Any] | None = None,
    ) -> Tenant:
        now = now_ms()
        tenant = Tenant(
            id=tenant_id or new_id("tnt"),
            name=name,
            plan=plan,
            status="active",
            rps_limit=rps_limit,
            burst=burst,
            max_concurrency=max_concurrency,
            monthly_budget_usd=monthly_budget_usd,
            monthly_spend_usd=0.0,
            settings=settings or {},
            created_at_ms=now,
            updated_at_ms=now,
        )
        self._tx.execute(
            """
            INSERT INTO tenants (id, name, plan, status, rps_limit, burst, max_concurrency,
                monthly_budget_usd, monthly_spend_usd, settings_json, created_at_ms, updated_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tenant.id,
                tenant.name,
                tenant.plan.value,
                tenant.status,
                tenant.rps_limit,
                tenant.burst,
                tenant.max_concurrency,
                tenant.monthly_budget_usd,
                0.0,
                _dumps(tenant.settings),
                now,
                now,
            ),
        )
        return tenant

    def get(self, tenant_id: str) -> Tenant | None:
        row = self._tx.query_one("SELECT * FROM tenants WHERE id = ?", (tenant_id,))
        return Tenant.from_row(row) if row else None

    def require(self, tenant_id: str) -> Tenant:
        tenant = self.get(tenant_id)
        if tenant is None:
            raise NotFound("tenant not found", details={"tenant": tenant_id})
        return tenant

    def list(self) -> list[Tenant]:
        return [Tenant.from_row(row) for row in self._tx.query("SELECT * FROM tenants ORDER BY created_at_ms")]

    def set_status(self, tenant_id: str, status: str) -> Tenant:
        self._tx.execute(
            "UPDATE tenants SET status = ?, updated_at_ms = ? WHERE id = ?",
            (status, now_ms(), tenant_id),
        )
        return self.require(tenant_id)

    def add_spend(self, tenant_id: str, cost_usd: float) -> None:
        self._tx.execute(
            "UPDATE tenants SET monthly_spend_usd = monthly_spend_usd + ?, updated_at_ms = ? WHERE id = ?",
            (cost_usd, now_ms(), tenant_id),
        )

    def assert_budget(self, tenant: Tenant, projected_cost_usd: float = 0.0) -> None:
        if tenant.monthly_budget_usd is None:
            return
        if tenant.monthly_spend_usd + projected_cost_usd > tenant.monthly_budget_usd:
            raise QuotaExceeded(
                "monthly budget exhausted",
                details={
                    "spend_usd": round(tenant.monthly_spend_usd, 6),
                    "budget_usd": tenant.monthly_budget_usd,
                },
                retry_after_s=3600.0,
            )


class ApiKeyRepository:
    def __init__(self, tx: Tx, *, pepper: str = "") -> None:
        self._tx = tx
        self._pepper = pepper

    def add(self, record: ApiKeyRecord) -> None:
        self._tx.execute(
            """
            INSERT INTO api_keys (id, tenant_id, subject, digest, plan, roles_json, capabilities_json,
                label, last4, expires_at_ms, revoked_at_ms, use_count, created_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                record.id,
                record.tenant_id,
                record.subject,
                record.digest,
                record.plan.value,
                json.dumps(list(record.roles)),
                json.dumps(sorted(record.capabilities)),
                record.label,
                record.last4,
                _seconds_to_ms(record.expires_at),
                None,
                now_ms(),
            ),
        )

    def lookup_by_digest(self, digest: str) -> ApiKeyRecord | None:
        row = self._tx.query_one("SELECT * FROM api_keys WHERE digest = ?", (digest,))
        return _api_key_from_row(row) if row else None

    def get(self, tenant_id: str, key_id: str) -> ApiKeyRecord | None:
        row = self._tx.query_one("SELECT * FROM api_keys WHERE tenant_id = ? AND id = ?", (tenant_id, key_id))
        return _api_key_from_row(row) if row else None

    def list(self, tenant_id: str) -> list[ApiKeyRecord]:
        rows = self._tx.query(
            "SELECT * FROM api_keys WHERE tenant_id = ? ORDER BY created_at_ms DESC",
            (tenant_id,),
        )
        return [_api_key_from_row(row) for row in rows]

    def revoke(self, tenant_id: str, key_id: str) -> None:
        cursor = self._tx.execute(
            "UPDATE api_keys SET revoked_at_ms = ? WHERE tenant_id = ? AND id = ?",
            (now_ms(), tenant_id, key_id),
        )
        if cursor.rowcount == 0:
            raise NotFound("api key not found", details={"key_id": key_id})

    def touch(self, key_id: str) -> None:
        self._tx.execute(
            "UPDATE api_keys SET last_used_at_ms = ?, use_count = use_count + 1 WHERE id = ?",
            (now_ms(), key_id),
        )


class MembershipRepository:
    def __init__(self, tx: Tx) -> None:
        self._tx = tx

    def grant(self, tenant_id: str, subject: str, role: str) -> None:
        self._tx.execute(
            "INSERT INTO memberships (tenant_id, subject, role, created_at_ms) VALUES (?, ?, ?, ?) "
            "ON CONFLICT DO NOTHING",
            (tenant_id, subject, role, now_ms()),
        )

    def revoke(self, tenant_id: str, subject: str, role: str) -> None:
        self._tx.execute(
            "DELETE FROM memberships WHERE tenant_id = ? AND subject = ? AND role = ?",
            (tenant_id, subject, role),
        )

    def roles_for(self, tenant_id: str, subject: str) -> tuple[str, ...]:
        rows = self._tx.query(
            "SELECT role FROM memberships WHERE tenant_id = ? AND subject = ? ORDER BY role",
            (tenant_id, subject),
        )
        return tuple(str(row["role"]) for row in rows)


class UsageRepository:
    def __init__(self, tx: Tx) -> None:
        self._tx = tx

    def record(
        self,
        *,
        tenant_id: str,
        model: str,
        provider: str,
        request_tokens: int,
        response_tokens: int,
        cost_usd: float,
        latency_ms: float,
        status_code: int,
        credential_id: str | None = None,
        cache_hit: bool = False,
        request_id: str | None = None,
    ) -> str:
        record_id = new_id("use")
        self._tx.execute(
            """
            INSERT INTO usage_records (id, tenant_id, credential_id, model, provider, request_tokens,
                response_tokens, cost_usd, latency_ms, status_code, cache_hit, request_id, created_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record_id,
                tenant_id,
                credential_id,
                model,
                provider,
                request_tokens,
                response_tokens,
                cost_usd,
                latency_ms,
                status_code,
                1 if cache_hit else 0,
                request_id,
                now_ms(),
            ),
        )
        return record_id

    def summarize(self, tenant_id: str, *, since_ms: int) -> dict[str, Any]:
        row = self._tx.query_one(
            """
            SELECT COUNT(*) AS requests,
                   COALESCE(SUM(request_tokens), 0) AS request_tokens,
                   COALESCE(SUM(response_tokens), 0) AS response_tokens,
                   COALESCE(SUM(cost_usd), 0) AS cost_usd,
                   COALESCE(AVG(latency_ms), 0) AS avg_latency_ms,
                   COALESCE(SUM(cache_hit), 0) AS cache_hits
            FROM usage_records WHERE tenant_id = ? AND created_at_ms >= ?
            """,
            (tenant_id, since_ms),
        )
        return dict(row or {})


class AuditRepository:
    def __init__(self, tx: Tx) -> None:
        self._tx = tx

    def log(
        self,
        *,
        tenant_id: str,
        actor: str,
        action: str,
        resource: str = "",
        result: str = "success",
        details: dict[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> str:
        entry_id = new_id("aud")
        self._tx.execute(
            """
            INSERT INTO audit_log (id, tenant_id, actor, action, resource, result, trace_id, details_json, created_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (entry_id, tenant_id, actor, action, resource, result, trace_id, _dumps(details or {}), now_ms()),
        )
        return entry_id

    def list(self, tenant_id: str, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._tx.query(
            "SELECT * FROM audit_log WHERE tenant_id = ? ORDER BY created_at_ms DESC LIMIT ?",
            (tenant_id, limit),
        )
        for row in rows:
            row["details"] = json.loads(row.pop("details_json") or "{}")
        return rows


class OutboxRepository:
    def __init__(self, tx: Tx) -> None:
        self._tx = tx

    def append(self, *, tenant_id: str, type: str, subject: str = "", payload: dict[str, Any] | None = None) -> str:
        event_id = new_id("evt")
        self._tx.execute(
            """
            INSERT INTO outbox_events (id, tenant_id, type, subject, payload_json, created_at_ms)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_id, tenant_id, type, subject, _dumps(payload or {}), now_ms()),
        )
        self._tx.emit({"id": event_id, "tenant_id": tenant_id, "type": type, "subject": subject, "payload": payload or {}})
        return event_id

    def unpublished(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._tx.query(
            "SELECT * FROM outbox_events WHERE published_at_ms IS NULL ORDER BY created_at_ms LIMIT ?",
            (limit,),
        )
        for row in rows:
            row["payload"] = json.loads(row.pop("payload_json") or "{}")
        return rows

    def mark_published(self, event_ids: Iterable[str]) -> None:
        for event_id in event_ids:
            self._tx.execute(
                "UPDATE outbox_events SET published_at_ms = ? WHERE id = ? AND published_at_ms IS NULL",
                (now_ms(), event_id),
            )


class IdempotencyService:
    """Request-level exactly-once effect for non-idempotent verbs.

    The key is reserved in the caller's transaction, so a crash after the business
    write cannot leave a "we never saw it" hole, and a concurrent duplicate gets an
    explicit in_progress instead of a second side effect.
    """

    def __init__(self, tx: Tx, *, ttl_s: int = 86_400) -> None:
        self._tx = tx
        self._ttl_ms = ttl_s * 1000

    def check(self, tenant_id: str, key: str, fingerprint: str) -> IdempotencyOutcome:
        now = now_ms()
        self._tx.execute(
            "DELETE FROM idempotency_keys WHERE tenant_id = ? AND expires_at_ms < ?",
            (tenant_id, now),
        )
        row = self._tx.query_one(
            "SELECT * FROM idempotency_keys WHERE tenant_id = ? AND key = ?",
            (tenant_id, key),
        )
        if row is None:
            self._tx.execute(
                """
                INSERT INTO idempotency_keys (tenant_id, key, fingerprint, created_at_ms, expires_at_ms)
                VALUES (?, ?, ?, ?, ?)
                """,
                (tenant_id, key, fingerprint, now, now + self._ttl_ms),
            )
            return IdempotencyOutcome("fresh")
        if row["fingerprint"] != fingerprint:
            raise IdempotencyConflict(
                "idempotency key was already used with a different request",
                details={"key": key},
            )
        if row["completed_at_ms"] is not None:
            return IdempotencyOutcome(
                "replay",
                status_code=int(row["status_code"] or 200),
                body=json.loads(row["response_json"]) if row["response_json"] else None,
            )
        return IdempotencyOutcome("in_progress")

    def complete(self, tenant_id: str, key: str, status_code: int, body: Any) -> None:
        self._tx.execute(
            """
            UPDATE idempotency_keys
               SET status_code = ?, response_json = ?, completed_at_ms = ?
             WHERE tenant_id = ? AND key = ?
            """,
            (status_code, _dumps(body) if isinstance(body, (dict, list)) else json.dumps(body), now_ms(), tenant_id, key),
        )

    def release(self, tenant_id: str, key: str) -> None:
        """Drop a reservation so the client may safely retry after a failure."""
        self._tx.execute(
            "DELETE FROM idempotency_keys WHERE tenant_id = ? AND key = ? AND completed_at_ms IS NULL",
            (tenant_id, key),
        )


class UnitOfWork:
    """Transactional scope bound to exactly one tenant (or none for platform work)."""

    def __init__(
        self,
        db: Database,
        *,
        tenant_id: str | None = None,
        immediate: bool = False,
        event_sink: EventSink | None = None,
        settings: Settings | None = None,
    ) -> None:
        self._db = db
        self._tenant_id = tenant_id
        self._immediate = immediate
        self._sink = event_sink
        self._settings = settings
        self._tx: Tx | None = None
        self._ctx: Any = None

    def __enter__(self) -> UnitOfWork:
        self._ctx = self._db.transaction(immediate=self._immediate, tenant_id=self._tenant_id)
        self._tx = self._ctx.__enter__()
        pepper = self._settings.api_key_pepper if self._settings else ""
        self.tenants = TenantRepository(self._tx)
        self.keys = ApiKeyRepository(self._tx, pepper=pepper)
        self.memberships = MembershipRepository(self._tx)
        self.usage = UsageRepository(self._tx)
        self.audit = AuditRepository(self._tx)
        self.outbox = OutboxRepository(self._tx)
        self.idempotency = IdempotencyService(self._tx)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        assert self._ctx is not None
        result = bool(self._ctx.__exit__(exc_type, exc, tb))
        if exc_type is None:
            # Publish outside the transaction: an event describing a write that never
            # committed is worse than an event that arrives twice.
            self.publish_pending()
        return result

    @property
    def tx(self) -> Tx:
        if self._tx is None:
            raise Conflict("unit of work is not open")
        return self._tx

    def publish_pending(self) -> list[dict[str, Any]]:
        """Deliver outbox events queued in this transaction (post-commit)."""
        tx = self._tx
        if tx is None:
            return []
        events = list(tx.outbox)
        if not events:
            return []
        del tx.outbox[: len(events)]
        if events and self._sink is not None:
            self._sink.publish(events)
            with self._db.transaction(immediate=True, tenant_id=self._tenant_id) as tx:
                OutboxRepository(tx).mark_published([event["id"] for event in events])
        return events


def _api_key_from_row(row: dict[str, Any]) -> ApiKeyRecord:
    return ApiKeyRecord(
        id=row["id"],
        tenant_id=row["tenant_id"],
        subject=row["subject"],
        digest=row["digest"],
        plan=Plan(row["plan"]),
        roles=tuple(json.loads(row["roles_json"] or "[]")),
        capabilities=frozenset(json.loads(row["capabilities_json"] or "[]")),
        expires_at=_ms_to_seconds(row["expires_at_ms"]),
        revoked_at=_ms_to_seconds(row["revoked_at_ms"]),
        label=row["label"],
        last4=row["last4"],
    )


def _seconds_to_ms(value: float | None) -> int | None:
    return None if value is None else int(value * 1000)


def _ms_to_seconds(value: Any) -> float | None:
    return None if value is None else float(value) / 1000.0
