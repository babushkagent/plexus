"""Authentication: short-lived JWTs for humans/services, hashed API keys for
machines.

Design notes:
* Only the configured algorithm is accepted (alg confusion / `none` downgrade is
  rejected before signature parsing).
* API keys are high-entropy and only their HMAC digest is persisted, so a readonly
  database leak cannot be replayed.
* Revocation is checked on every use; jti denylisting covers JWTs mid-lifetime.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from ..config import Settings
from ..errors import Unauthorized
from ..ids import hash_token, new_id, new_token
from .context import Plan, TenantContext
from .rbac import capabilities_for_roles

SUPPORTED_ALG = "HS256"
_ALG_MAP = {"HS256": hashlib.sha256}


def _b64u_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _b64u_decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(raw.encode() + padding.encode())


class CredentialLookup(Protocol):
    def api_key_by_digest(self, digest: str) -> ApiKeyRecord | None: ...

    def is_denied(self, jti: str) -> bool: ...


@dataclass(frozen=True, slots=True)
class ApiKeyRecord:
    id: str
    tenant_id: str
    subject: str
    digest: str
    plan: Plan = Plan.STANDARD
    roles: tuple[str, ...] = ()
    capabilities: frozenset[str] = frozenset()
    expires_at: float | None = None
    revoked_at: float | None = None
    label: str = ""
    last4: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_active(self, now: float) -> bool:
        if self.revoked_at is not None:
            return False
        return self.expires_at is None or self.expires_at > now


@dataclass(frozen=True, slots=True)
class MintedKey:
    record: ApiKeyRecord
    secret: str


def mint_api_key(
    *,
    tenant_id: str,
    subject: str,
    plan: Plan = Plan.STANDARD,
    roles: Sequence[str] = (),
    capabilities: Sequence[str] = (),
    label: str = "",
    expires_at: float | None = None,
    pepper: str = "",
) -> MintedKey:
    secret = new_token()
    record = ApiKeyRecord(
        id=new_id("key"),
        tenant_id=tenant_id,
        subject=subject,
        digest=hash_token(secret, pepper=pepper),
        plan=plan,
        roles=tuple(roles),
        capabilities=frozenset(capabilities),
        expires_at=expires_at,
        label=label,
        last4=secret[-4:],
    )
    return MintedKey(record=record, secret=secret)


class TokenIssuer:
    def __init__(self, settings: Settings, *, default_ttl_s: int = 900) -> None:
        self._settings = settings
        self._default_ttl = default_ttl_s

    def issue(
        self,
        *,
        tenant_id: str,
        subject: str,
        roles: Sequence[str] = (),
        capabilities: Sequence[str] = (),
        plan: Plan = Plan.STANDARD,
        platform: bool = False,
        ttl_s: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> str:
        ttl = self._default_ttl if ttl_s is None else ttl_s
        now = int(time.time())
        claims: dict[str, Any] = {
            "iss": self._settings.jwt_issuer,
            "aud": self._settings.jwt_audience,
            "sub": subject,
            "tid": tenant_id,
            "plan": plan.value,
            "roles": list(roles),
            "iat": now,
            "nbf": now,
            "exp": now + ttl,
            "jti": new_id("tok"),
        }
        if capabilities:
            claims["caps"] = list(capabilities)
        if platform:
            claims["scope"] = "platform"
        if extra:
            claims.update(extra)
        return sign_jwt(claims, secret=self._settings.jwt_secret)


def sign_jwt(claims: dict[str, Any], *, secret: str) -> str:
    header = {"alg": SUPPORTED_ALG, "typ": "JWT"}
    signing_input = _b64u_encode(_json(header)) + "." + _b64u_encode(_json(claims))
    signature = _sign(signing_input.encode(), secret=secret)
    return f"{signing_input}.{_b64u_encode(signature)}"


def verify_jwt(
    token: str,
    *,
    settings: Settings,
    lookup: CredentialLookup | None = None,
    now: float | None = None,
    secrets: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Verify signature + registered claims and return the claim set."""
    parts = token.strip().split(".")
    if len(parts) != 3:
        raise Unauthorized("malformed token")
    header_b64, payload_b64, signature_b64 = parts
    try:
        header = json.loads(_b64u_decode(header_b64))
    except (ValueError, json.JSONDecodeError) as exc:
        raise Unauthorized("malformed token header") from exc
    if not isinstance(header, dict) or header.get("alg") != SUPPORTED_ALG:
        raise Unauthorized("unsupported token algorithm", details={"alg": header.get("alg") if isinstance(header, dict) else None})
    if str(header.get("typ", "JWT")) != "JWT":
        raise Unauthorized("unexpected token type")

    provided = _b64u_decode(signature_b64)
    signing_input = f"{header_b64}.{payload_b64}".encode()
    for candidate in _candidate_secrets(settings, secrets):
        expected = _sign(signing_input, secret=candidate)
        if hmac.compare_digest(expected, provided):
            break
    else:
        raise Unauthorized("invalid token signature")

    try:
        claims = json.loads(_b64u_decode(payload_b64))
    except (ValueError, json.JSONDecodeError) as exc:
        raise Unauthorized("malformed token payload") from exc
    if not isinstance(claims, dict):
        raise Unauthorized("token payload must be an object")

    leeway = settings.jwt_leeway_s
    current = int(now if now is not None else time.time())
    exp = claims.get("exp")
    if not isinstance(exp, (int, float)):
        raise Unauthorized("token missing exp")
    if current > exp + leeway:
        raise Unauthorized("token expired", details={"exp": int(exp)})
    nbf = claims.get("nbf")
    if isinstance(nbf, (int, float)) and current < nbf - leeway:
        raise Unauthorized("token not yet valid")
    iat = claims.get("iat")
    if isinstance(iat, (int, float)) and current < iat - leeway:
        raise Unauthorized("token issued in the future")
    if claims.get("iss") != settings.jwt_issuer:
        raise Unauthorized("token issuer mismatch", details={"iss": claims.get("iss")})
    aud = claims.get("aud")
    expected_aud = {settings.jwt_audience} if isinstance(settings.jwt_audience, str) else set(settings.jwt_audience)
    claim_aud = {aud} if isinstance(aud, str) else set(aud or [])
    if not claim_aud & expected_aud:
        raise Unauthorized("token audience mismatch", details={"aud": sorted(claim_aud)})
    if not claims.get("sub"):
        raise Unauthorized("token missing sub")

    jti = claims.get("jti")
    if isinstance(jti, str) and lookup is not None and lookup.is_denied(jti):
        raise Unauthorized("token revoked", details={"jti": jti})
    return claims


class Authenticator:
    """Turn transport credentials into a TenantContext."""

    def __init__(self, settings: Settings, lookup: CredentialLookup) -> None:
        self._settings = settings
        self._lookup = lookup

    def authenticate(
        self,
        *,
        authorization: str | None,
        api_key: str | None = None,
        now: float | None = None,
    ) -> TenantContext:
        if api_key:
            return self._from_api_key(api_key, now=now)
        if authorization:
            scheme, _, value = authorization.partition(" ")
            if scheme.lower() != "bearer" or not value.strip():
                raise Unauthorized("authorization header must use the Bearer scheme")
            token = value.strip()
            if token.startswith("plx_"):
                return self._from_api_key(token, now=now)
            return self._from_jwt(token, now=now)
        raise Unauthorized("missing credentials")

    def _from_jwt(self, token: str, *, now: float | None) -> TenantContext:
        claims = verify_jwt(token, settings=self._settings, lookup=self._lookup, now=now)
        roles = tuple(str(r) for r in claims.get("roles", []) if isinstance(r, str))
        caps = set(capabilities_for_roles(roles)) | {str(c) for c in claims.get("caps", [])}
        plan = _plan(claims.get("plan"))
        return TenantContext(
            tenant_id=str(claims.get("tid") or "platform"),
            subject=str(claims["sub"]),
            capabilities=frozenset(caps),
            plan=plan,
            auth_method="jwt",
            credential_id=str(claims.get("jti")) if claims.get("jti") else None,
            is_platform=claims.get("scope") == "platform",
            roles=roles,
        )

    def _from_api_key(self, secret: str, *, now: float | None) -> TenantContext:
        current = time.time() if now is None else now
        record = self._lookup.api_key_by_digest(hash_token(secret, pepper=self._settings.api_key_pepper))
        if record is None:
            # Same error and timing shape as a wrong password: never reveal key existence.
            raise Unauthorized("invalid API key")
        if not record.is_active(current):
            raise Unauthorized("API key inactive", details={"key_id": record.id})
        roles = record.roles
        caps = set(capabilities_for_roles(roles)) | set(record.capabilities)
        return TenantContext(
            tenant_id=record.tenant_id,
            subject=record.subject,
            capabilities=frozenset(caps),
            plan=record.plan,
            auth_method="api_key",
            credential_id=record.id,
            roles=roles,
            tags={"key_label": record.label},
        )


def _candidate_secrets(settings: Settings, extra: Sequence[str] | None) -> list[str]:
    candidates = [settings.jwt_secret]
    if extra:
        candidates.extend(extra)
    return [c for c in candidates if c]


def _sign(payload: bytes, *, secret: str) -> bytes:
    if not secret:
        raise Unauthorized("signing secret is not configured")
    return hmac.new(secret.encode(), payload, _ALG_MAP[SUPPORTED_ALG]).digest()


def _json(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def _plan(raw: Any) -> Plan:
    try:
        return Plan(str(raw))
    except ValueError:
        return Plan.STANDARD
