"""Error taxonomy shared by every surface: API, workers, providers.

Errors carry a stable machine code, an HTTP status, and a retryability hint so
clients and retries can make the same decision everywhere in the system.
"""

from __future__ import annotations

from typing import Any


class PlatformError(Exception):
    code: str = "internal_error"
    status: int = 500
    retryable: bool = False

    def __init__(
        self,
        message: str | None = None,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.message = message or self.__class__.__doc__ or self.code
        self.details: dict[str, Any] = dict(details or {})
        super().__init__(self.message)

    def to_envelope(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "retryable": self.retryable,
                "details": self.details,
            }
        }


class ValidationFailed(PlatformError):
    """Request failed validation."""

    code = "validation_failed"
    status = 400


class Unauthorized(PlatformError):
    """Credentials are missing, malformed, or expired."""

    code = "unauthorized"
    status = 401


class Forbidden(PlatformError):
    """Caller is authenticated but lacks the capability."""

    code = "forbidden"
    status = 403


class TenantIsolationViolation(PlatformError):
    """A request tried to cross a tenant boundary."""

    code = "tenant_isolation_violation"
    status = 403


class NotFound(PlatformError):
    """Resource does not exist in the caller's tenant."""

    code = "not_found"
    status = 404


class MethodNotAllowed(PlatformError):
    """Path exists but the verb does not; lets the edge send Allow details."""

    code = "method_not_allowed"
    status = 405


class Conflict(PlatformError):
    """State machine or optimistic-concurrency conflict."""

    code = "conflict"
    status = 409


class IdempotencyConflict(PlatformError):
    """Idempotency key reused with a different request fingerprint."""

    code = "idempotency_conflict"
    status = 409


class _WithRetryAfter(PlatformError):
    """Base for errors that should tell the client when to come back."""

    def __init__(self, message: str | None = None, *, retry_after_s: float = 1.0, **kw: Any) -> None:
        super().__init__(message, **kw)
        self.retry_after_s = float(retry_after_s)


class QuotaExceeded(_WithRetryAfter):
    """Tenant hit a configured hard limit."""

    code = "quota_exceeded"
    status = 429
    retryable = True


class RateLimited(QuotaExceeded):
    """Too many requests for this tenant."""

    code = "rate_limited"
    status = 429


class BudgetExhausted(QuotaExceeded):
    """Tenant spend ceiling reached for the current period."""

    code = "budget_exhausted"


class UpstreamError(PlatformError):
    """A provider or dependency failed."""

    code = "upstream_error"
    status = 502
    retryable = True


class CircuitOpen(_WithRetryAfter):
    """Upstream is tripping the circuit breaker."""

    code = "circuit_open"
    status = 503
    retryable = True


class UpstreamUnavailable(PlatformError):
    """No healthy upstream remains after fallbacks."""

    code = "upstream_unavailable"
    status = 503
    retryable = True


class Timeout(PlatformError):
    """Deadline exceeded."""

    code = "timeout"
    status = 504
    retryable = True
