"""The console is served unauthenticated, so its safety properties are tested as a boundary.

Two things have to stay true for a public control-plane UI to be safe: the page must be
inert (a shell plus a route manifest -- no data, no credential, no configuration before
the operator authenticates) and it must not be able to talk anywhere but its own origin.
The manifest is also checked against the routing table in both directions, so the UI
cannot drift from the API or invent a privileged path.
"""

from __future__ import annotations

import inspect
import json
import re
import shutil
import subprocess
import tempfile
from typing import Any

import pytest

from conftest import Client, make_settings, mint_key
from plexus.api import console as console_module
from plexus.api.console import _SCRIPT, CHAT_PATH, CONSOLE_PATHS, CSP, VIEWS, render_page
from plexus.api.handlers import ROUTES
from plexus.api.server import App

# Prometheus scrapes this; a browser console has no reason to.
NOT_IN_CONSOLE = {("GET", "/metrics")}
METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}


def _page(client: Client, path: str = "/console") -> str:
    response = client.get(path)
    assert response.status == 200
    text: str = response.raw or ""
    return text


def _embedded_manifest(page: str) -> dict[str, Any]:
    found = re.search(r'<script id="manifest"[^>]*>(.*?)</script>', page, re.DOTALL)
    assert found is not None, "manifest script element missing from the console page"
    payload: dict[str, Any] = json.loads(found.group(1))
    return payload


def _declared_calls() -> set[tuple[str, str]]:
    calls = {(call.method, call.path.split("?")[0]) for view in VIEWS for call in view.calls}
    calls |= {("POST", view.stream_path) for view in VIEWS if view.stream_path}
    return calls


@pytest.mark.parametrize("path", CONSOLE_PATHS)
def test_console_is_served_without_a_credential(app: App, client: Client, path: str) -> None:
    response = client.get(path)
    assert response.status == 200
    assert response.content_type == "text/html; charset=utf-8"
    assert "<!doctype html>" in (response.raw or "")


def test_console_page_declares_hardened_headers(client: Client) -> None:
    response = client.get("/console")
    # connect-src 'self' is the load-bearing directive: the page can only call its own API.
    assert response.headers["Content-Security-Policy"] == CSP
    assert "default-src 'none'" in CSP
    assert "connect-src 'self'" in CSP
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Referrer-Policy"] == "no-referrer"
    assert response.headers["Cache-Control"] == "no-store"


def test_page_leaks_no_identity_or_configuration(app: App, client: Client, tenant: str) -> None:
    secret = mint_key(app, tenant)
    page = _page(client)
    settings = app.settings
    for value in (settings.jwt_secret, settings.api_key_pepper, settings.database_url, secret, tenant):
        assert value not in page, f"{value!r} must never be embedded in the console shell"


def test_page_fetches_nothing_off_origin(client: Client) -> None:
    page = _page(client)
    assert "http://" not in page
    assert "https://" not in page


def test_script_never_interprets_api_output_as_markup(client: Client) -> None:
    # innerHTML is the only place a hostile response could turn into script, and CSP
    # cannot stop an inline handler once markup is parsed. Rendering stays DOM-only.
    assert "innerHTML" not in _page(client)


def test_script_literal_survives_python_escaping() -> None:
    """A non-raw literal eats `\\n` and `\\"`, and the browser answers with a blank page.

    The console's JavaScript is served verbatim, so no escape sequence in it may depend
    on how Python reads the source file that holds it.
    """
    assert "\\\n" not in _SCRIPT, "line continuation in the script asset: Python eats the newline"
    assert '\\"' not in _SCRIPT, "escaped quote in the script asset: quote with ' instead"
    assert '_SCRIPT = r"""' in inspect.getsource(console_module), "script asset must be a raw literal"


def test_service_name_cannot_inject_markup(tmp_path: Any, providers: list[Any]) -> None:
    settings = make_settings(tmp_path, "injection", service_name="<svg onload=alert(1)>")
    application = App(settings, providers=list(providers))
    page = render_page(application)
    assert "<svg" not in page
    assert "&lt;svg" in page


def test_embedded_manifest_is_valid_json(client: Client) -> None:
    manifest = _embedded_manifest(_page(client))
    assert manifest["service"] == client.app.settings.service_name
    assert manifest["views"], "console shipped without a single view"


def test_views_are_well_formed() -> None:
    ids = [view.id for view in VIEWS]
    assert len(ids) == len(set(ids)), "duplicate view id"
    for view in VIEWS:
        assert view.label.strip()
        assert view.kind in ("calls", "chat")
        if view.kind == "chat":
            assert view.stream_path == CHAT_PATH
        assert view.calls, f"view {view.id} has no calls"
        for call in view.calls:
            assert call.method in METHODS
            assert call.path.startswith("/")


def test_manifest_covers_every_authenticated_route() -> None:
    """The console is the only supported UI: if a route exists, an operator can reach it."""
    required = {
        (route.method, route.pattern)
        for route in ROUTES
        if not route.public and (route.method, route.pattern) not in NOT_IN_CONSOLE
    }
    assert required - _declared_calls() == set()


def test_manifest_advertises_no_route_the_api_does_not_serve() -> None:
    """Guards against a stale button (and against a console-only privileged path)."""
    assert _declared_calls() <= {(route.method, route.pattern) for route in ROUTES}


def test_console_can_be_switched_off(tmp_path: Any, providers: list[Any]) -> None:
    settings = make_settings(tmp_path, "no-console", console_enabled=False)
    application = App(settings, providers=list(providers))
    application.init_schema()
    for path in CONSOLE_PATHS:
        assert Client(application).get(path).status == 404
@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_served_script_parses_as_javascript(client: Client) -> None:
    found = re.search(r"<script>(.*?)</script>", _page(client), re.DOTALL)
    assert found is not None
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8") as handle:
        handle.write(found.group(1))
        handle.flush()
        check = subprocess.run(["node", "--check", handle.name], capture_output=True, text=True, check=False)
    assert check.returncode == 0, check.stderr[:2000]
