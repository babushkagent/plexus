"""Shared fixtures: an isolated store and an in-process client per test.

Tests drive `App.handle` directly instead of going through a socket. That keeps the
full edge pipeline (auth, scoping, RBAC, admission, idempotency) under test while
staying hermetic; `test_http.py` covers the transport layer separately.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from typing import Any

import pytest

from plexus.api.server import App
from plexus.config import Settings, load_settings
from plexus.llm.provider import EchoProvider, Provider
from plexus.telemetry import TraceContext
from plexus.tenancy.auth import TokenIssuer, mint_api_key
from plexus.tenancy.context import Plan
from plexus.tenancy.rbac import ROLE_OWNER, capabilities_for_roles

PEPPER = "test-pepper"
JWT_SECRET = "unit-test-secret-value-that-is-long-enough-32"


@pytest.fixture(autouse=True)
def _quiet_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Never inherit a developer's real provider credentials into a test."""
    for name in list(os.environ):
        if name.startswith("PLEXUS_"):
            monkeypatch.delenv(name, raising=False)


def make_settings(tmp_path: Any, name: str = "t", **overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "database_url": f"sqlite:///{tmp_path}/{name}.sqlite3",
        "env": "test",
        "jwt_secret": JWT_SECRET,
        "api_key_pepper": PEPPER,
        "json_logs": False,
        "log_level": "CRITICAL",
        "task_lease_s": 5.0,
        "task_heartbeat_s": 1.0,
    }
    base.update(overrides)
    return load_settings({}, dotenv=None, **base)


@pytest.fixture
def settings(tmp_path: Any) -> Settings:
    return make_settings(tmp_path)


@pytest.fixture
def providers() -> list[Provider]:
    return [EchoProvider(name="echo"), EchoProvider(name="echo-b")]


@pytest.fixture
def app(settings: Settings, providers: list[Provider]) -> Iterator[App]:
    application = App(settings, providers=list(providers))
    application.init_schema()
    yield application


class Client:
    """Tiny request builder over the in-process pipeline."""

    def __init__(self, app: App, *, api_key: str | None = None, token: str | None = None) -> None:
        self.app = app
        self.api_key = api_key
        self.token = token

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        headers: dict[str, str] | None = None,
        api_key: str | None = ...,  # type: ignore[assignment]
        token: str | None = ...,  # type: ignore[assignment]
    ) -> Any:
        import json as _json

        from plexus.api.messages import Request

        merged = dict(headers or {})
        key = self.api_key if api_key is ... else api_key
        jwt = self.token if token is ... else token
        if key:
            merged["x-api-key"] = key
        if jwt:
            merged["authorization"] = f"Bearer {jwt}"
        request = Request(
            method=method.upper(),
            path=path,
            headers={k.lower(): v for k, v in merged.items()},
            body=_json.dumps(body).encode() if body is not None else b"",
            request_id="req_test",
            trace=TraceContext.new(),
        )
        return self.app.handle(request)

    def get(self, path: str, **kwargs: Any) -> Any:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, body: dict[str, Any] | None = None, **kwargs: Any) -> Any:
        return self.request("POST", path, body or {}, **kwargs)

    def put(self, path: str, body: dict[str, Any] | None = None, **kwargs: Any) -> Any:
        return self.request("PUT", path, body or {}, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> Any:
        return self.request("DELETE", path, **kwargs)

    def json_of(self, response: Any) -> dict[str, Any]:
        assert isinstance(response.payload, dict), f"expected object body, got {response.payload!r}"
        return response.payload


@pytest.fixture
def client(app: App) -> Client:
    return Client(app)


@pytest.fixture
def tenant(app: App) -> str:
    with app.uow(immediate=True) as uow:
        return uow.tenants.create(name="acme", plan=Plan.STANDARD).id


@pytest.fixture
def other_tenant(app: App) -> str:
    with app.uow(immediate=True) as uow:
        return uow.tenants.create(name="globex", plan=Plan.STANDARD).id


def mint_key(
    app: App,
    tenant_id: str,
    *,
    roles: Sequence[str] = (ROLE_OWNER,),
    plan: Plan = Plan.STANDARD,
    subject: str = "svc@test",
) -> str:
    minted = mint_api_key(
        tenant_id=tenant_id,
        subject=subject,
        plan=plan,
        roles=list(roles),
        capabilities=sorted(capabilities_for_roles(list(roles))),
        label="test",
        pepper=app.settings.api_key_pepper,
    )
    with app.uow(immediate=True) as uow:
        uow.keys.add(minted.record)
    return minted.secret


@pytest.fixture
def api_key(app: App, tenant: str) -> str:
    return mint_key(app, tenant)


@pytest.fixture
def scoped_client(app: App, tenant: str, api_key: str) -> Client:
    return Client(app, api_key=api_key)


def owner_client(app: App, tenant_id: str) -> Client:
    return Client(app, api_key=mint_key(app, tenant_id, roles=(ROLE_OWNER,)))


@pytest.fixture
def jwt(app: App, tenant: str) -> str:
    return TokenIssuer(app.settings).issue(tenant_id=tenant, subject="user@test", roles=[ROLE_OWNER])


@pytest.fixture
def platform_jwt(app: App) -> str:
    return TokenIssuer(app.settings).issue(tenant_id="platform", subject="ops@test", platform=True)
