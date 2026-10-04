"""Tenant context: the single source of truth for "who is calling, and which
tenant owns their blast radius".

Every request, task, and provider call runs inside a TenantContext. Data access
helpers refuse to operate without one, so forgetting to scope a query becomes an
error rather than a cross-tenant leak.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterator

from ..errors import Forbidden, TenantIsolationViolation, Unauthorized


class Plan(str, Enum):
    FREE = "free"
    STANDARD = "standard"
    ENTERPRISE = "enterprise"


@dataclass(frozen=True, slots=True)
class PlanLimits:
    requests_per_second: float
    burst: float
    max_concurrency: int
    monthly_budget_usd: float | None
    max_model_versions: int
    max_replicas: int


DEFAULT_LIMITS: dict[Plan, PlanLimits] = {
    Plan.FREE: PlanLimits(5, 10, 2, 50.0, 25, 3),
    Plan.STANDARD: PlanLimits(50, 100, 32, 2_500.0, 500, 20),
    Plan.ENTERPRISE: PlanLimits(500, 1_000, 256, None, 10_000, 200),
}


@dataclass(frozen=True, slots=True)
class TenantContext:
    tenant_id: str
    subject: str
    capabilities: frozenset[str] = frozenset()
    plan: Plan = Plan.STANDARD
    auth_method: str = "jwt"
    credential_id: str | None = None
    is_platform: bool = False
    roles: tuple[str, ...] = ()
    request_id: str | None = None
    tags: dict[str, Any] = field(default_factory=dict)

    def can(self, capability: str) -> bool:
        return self.is_platform or capability in self.capabilities or "*" in self.capabilities

    def require(self, capability: str) -> None:
        if not self.can(capability):
            raise Forbidden(
                f"missing capability {capability!r}",
                details={"required": capability, "subject": self.subject},
            )

    def own_or_deny(self, resource_tenant_id: str) -> str:
        """Return the resource tenant iff it belongs to this caller's boundary."""
        if not self.is_platform and resource_tenant_id != self.tenant_id:
            raise TenantIsolationViolation(
                "resource belongs to another tenant",
                details={"tenant": resource_tenant_id},
            )
        return resource_tenant_id

    @property
    def limits(self) -> PlanLimits:
        return DEFAULT_LIMITS[self.plan]

    def log_fields(self) -> dict[str, str]:
        fields = {
            "tenant_id": self.tenant_id,
            "subject": self.subject,
            "auth_method": self.auth_method,
            "plan": self.plan.value,
        }
        if self.request_id:
            fields["request_id"] = self.request_id
        return fields


_context: ContextVar[TenantContext | None] = ContextVar("plexus.tenant_context", default=None)


def current() -> TenantContext:
    ctx = _context.get()
    if ctx is None:
        raise Unauthorized("no tenant context bound to this execution")
    return ctx


def try_current() -> TenantContext | None:
    return _context.get()


@contextmanager
def use(ctx: TenantContext) -> Iterator[TenantContext]:
    token: Token[TenantContext | None] = _context.set(ctx)
    try:
        yield ctx
    finally:
        _context.reset(token)


@contextmanager
def bypass(reason: str, *, actor: TenantContext) -> Iterator[TenantContext]:
    """Explicit break-glass for platform jobs; always requires an audit reason."""
    if not actor.is_platform:
        raise Forbidden("only platform identities may bypass tenancy")
    scoped = TenantContext(
        tenant_id=actor.tenant_id,
        subject=actor.subject,
        capabilities=frozenset({"*"}),
        plan=actor.plan,
        auth_method=actor.auth_method,
        is_platform=True,
        tags={"bypass_reason": reason},
    )
    with use(scoped):
        yield scoped
