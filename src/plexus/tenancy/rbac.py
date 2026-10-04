"""Capability-based RBAC.

Roles are convenient bundles; capabilities are the contract. Handlers declare the
single capability they need, which keeps authorization auditable and prevents
the "admin can do anything from this endpoint" class of bugs.
"""

from __future__ import annotations

from ..errors import Forbidden
from .context import TenantContext


class Capability:
    TENANT_READ = "tenant:read"
    TENANT_WRITE = "tenant:write"
    KEY_MANAGE = "key:manage"
    MEMBER_MANAGE = "member:manage"
    MODEL_READ = "model:read"
    MODEL_WRITE = "model:write"
    MODEL_PROMOTE = "model:promote"
    DEPLOY_READ = "deployment:read"
    DEPLOY_WRITE = "deployment:write"
    RUN_READ = "run:read"
    RUN_WRITE = "run:write"
    INFERENCE_CALL = "inference:call"
    USAGE_READ = "usage:read"
    AUDIT_READ = "audit:read"
    PLATFORM_ADMIN = "platform:admin"
    ALL = "*"


ROLE_OWNER = "owner"
ROLE_ADMIN = "admin"
ROLE_ML_ENGINEER = "ml_engineer"
ROLE_INFERENCE_USER = "inference_user"
ROLE_VIEWER = "viewer"
ROLE_AUDITOR = "auditor"

ROLE_CAPABILITIES: dict[str, frozenset[str]] = {
    ROLE_OWNER: frozenset(
        {
            Capability.TENANT_READ,
            Capability.TENANT_WRITE,
            Capability.KEY_MANAGE,
            Capability.MEMBER_MANAGE,
            Capability.MODEL_READ,
            Capability.MODEL_WRITE,
            Capability.MODEL_PROMOTE,
            Capability.DEPLOY_READ,
            Capability.DEPLOY_WRITE,
            Capability.RUN_READ,
            Capability.RUN_WRITE,
            Capability.INFERENCE_CALL,
            Capability.USAGE_READ,
            Capability.AUDIT_READ,
        }
    ),
    ROLE_ADMIN: frozenset(
        {
            Capability.TENANT_READ,
            Capability.KEY_MANAGE,
            Capability.MEMBER_MANAGE,
            Capability.MODEL_READ,
            Capability.MODEL_WRITE,
            Capability.MODEL_PROMOTE,
            Capability.DEPLOY_READ,
            Capability.DEPLOY_WRITE,
            Capability.RUN_READ,
            Capability.RUN_WRITE,
            Capability.INFERENCE_CALL,
            Capability.USAGE_READ,
            Capability.AUDIT_READ,
        }
    ),
    ROLE_ML_ENGINEER: frozenset(
        {
            Capability.TENANT_READ,
            Capability.MODEL_READ,
            Capability.MODEL_WRITE,
            Capability.DEPLOY_READ,
            Capability.DEPLOY_WRITE,
            Capability.RUN_READ,
            Capability.RUN_WRITE,
            Capability.INFERENCE_CALL,
            Capability.USAGE_READ,
        }
    ),
    ROLE_INFERENCE_USER: frozenset({Capability.INFERENCE_CALL, Capability.MODEL_READ, Capability.USAGE_READ}),
    ROLE_VIEWER: frozenset({Capability.TENANT_READ, Capability.MODEL_READ, Capability.DEPLOY_READ, Capability.RUN_READ}),
    ROLE_AUDITOR: frozenset({Capability.AUDIT_READ, Capability.USAGE_READ, Capability.MODEL_READ}),
}


def capabilities_for_roles(roles: tuple[str, ...] | list[str]) -> frozenset[str]:
    caps: set[str] = set()
    for role in roles:
        caps |= set(ROLE_CAPABILITIES.get(role, frozenset()))
    return frozenset(caps)


def authorize(
    ctx: TenantContext,
    capability: str,
    *,
    resource_tenant_id: str | None = None,
) -> None:
    """Raise unless the caller may perform `capability` in their own tenant.

    Cross-tenant access requires an explicit platform identity; a normal tenant
    identity can never reach another tenant's rows regardless of capability.
    """
    if resource_tenant_id is not None:
        ctx.own_or_deny(resource_tenant_id)
    if not ctx.can(capability):
        raise Forbidden(
            f"missing capability {capability!r}",
            details={"required": capability, "roles": list(ctx.roles)},
        )


def authorize_platform(ctx: TenantContext) -> None:
    if not ctx.is_platform:
        raise Forbidden("platform identity required", details={"subject": ctx.subject})
