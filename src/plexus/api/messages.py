"""Transport-neutral request/response values shared by the edge and the handlers.

Handlers must be callable from tests, the CLI and a future ASGI adapter without an
HTTP server, so these types carry no dependency on ``http.server``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterator

from ..errors import Unauthorized, ValidationFailed
from ..telemetry import TraceContext
from ..tenancy.context import TenantContext


@dataclass(slots=True)
class Response:
    status: int = 200
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    content_type: str = "application/json"
    raw: str | None = None
    events: Iterator[dict[str, Any]] | None = None


def respond(payload: Any, status: int = 200, headers: dict[str, str] | None = None) -> Response:
    return Response(status=status, payload=payload, headers=dict(headers or {}))


def created(payload: Any, headers: dict[str, str] | None = None) -> Response:
    return respond(payload, status=201, headers=headers)


def no_content() -> Response:
    return Response(status=204, payload=None)


def stream_response(events: Iterator[dict[str, Any]]) -> Response:
    return Response(status=200, content_type="text/event-stream", events=events)


@dataclass(slots=True)
class Request:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes
    request_id: str
    trace: TraceContext
    query: dict[str, str] = field(default_factory=dict)
    params: dict[str, str] = field(default_factory=dict)
    ctx: TenantContext | None = None
    body_json: dict[str, Any] | None = None

    @property
    def json(self) -> dict[str, Any]:
        if self.body_json is None:
            if not self.body:
                self.body_json = {}
            else:
                try:
                    parsed = json.loads(self.body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValidationFailed("request body must be valid JSON") from exc
                if not isinstance(parsed, dict):
                    raise ValidationFailed("request body must be a JSON object")
                self.body_json = parsed
        return self.body_json

    @property
    def auth(self) -> TenantContext:
        if self.ctx is None:
            raise Unauthorized("request is not authenticated")
        return self.ctx

    @property
    def tenant_id(self) -> str:
        return self.auth.tenant_id

    def header(self, name: str) -> str | None:
        return self.headers.get(name.lower())

    def arg(self, name: str, default: str | None = None) -> str | None:
        return self.query.get(name, default)

    def int_arg(self, name: str, default: int) -> int:
        raw = self.query.get(name)
        if raw is None or raw == "":
            return default
        try:
            return max(1, min(500, int(raw)))
        except ValueError:
            raise ValidationFailed("query parameter must be an integer", details={name: raw}) from None

    def require_arg(self, name: str) -> str:
        raw = self.query.get(name)
        if not raw:
            raise ValidationFailed("query parameter is required", details={"parameter": name})
        return raw

    def param(self, name: str) -> str:
        value = self.params.get(name)
        if not value:
            raise ValidationFailed("path parameter is required", details={"parameter": name})
        return value
