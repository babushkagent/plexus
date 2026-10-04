"""The signals the autoscalers consume, and their refusal to become a new failure mode.

`plexus_queue_depth` is not decoration: `scaling.autoscaler.render_external_metric` and
deploy/k8s/base/scaledobject.yaml both ask for it by name. If it stops being exported,
workers silently stop scaling while every dashboard still looks green.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest

from plexus.api import handlers
from plexus.api.server import App
from plexus.scaling.autoscaler import render_external_metric, render_keda

from conftest import Client

SCALEDOBJECT = Path(__file__).resolve().parents[1] / "deploy" / "k8s" / "base" / "scaledobject.yaml"


def _body(response: Any) -> str:
    raw = response.raw or ""
    return raw.decode() if isinstance(raw, bytes) else raw


def test_backlog_and_breakers_are_exported(
    app: App,
    tenant: str,
    client: Client,
    platform_jwt: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = handlers._tasks(app)
    for index in range(3):
        queue.enqueue(tenant_id=tenant, type="inference.completion", payload={"index": index})

    # Scrapes are rate-limited by design; force a fresh sample for this assertion.
    monkeypatch.setattr(handlers, "_next_sample_at", 0.0)
    body = _body(client.get("/metrics", token=platform_jwt))

    assert "plexus_queue_depth 3.0" in body
    assert 'plexus_circuit_breaker_state{provider="echo"} 0.0' in body


def test_metrics_still_render_when_the_store_is_unavailable(
    app: App,
    client: Client,
    platform_jwt: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A scrape that 500s turns a partial outage into a blind one."""

    def explode(_app: App) -> Any:
        raise RuntimeError("store is down")

    monkeypatch.setattr(handlers, "_tasks", explode)
    monkeypatch.setattr(handlers, "_next_sample_at", 0.0)
    response = client.get("/metrics", token=platform_jwt)

    assert response.status == 200
    assert "plexus_http_requests_total" in _body(response)


def test_keda_scales_on_the_gauge_never_on_raw_sql() -> None:
    """Row-level security hides queue rows from a connection with no scope set.

    A `postgres` trigger runs its own COUNT on such a connection and reads 0 at exactly
    the moment the backlog is deepest, so it must not come back.
    """
    manifest = render_keda()

    assert "type: prometheus" in manifest
    assert "max(plexus_queue_depth)" in manifest
    assert "SELECT" not in manifest.upper()


def test_shipped_scaledobject_matches_the_rendered_design() -> None:
    shipped = SCALEDOBJECT.read_text()
    triggers = shipped[shipped.index("  triggers:"):]

    assert "type: prometheus" in triggers
    assert "max(plexus_queue_depth)" in triggers
    assert "threshold" in triggers, "a trigger without a threshold never scales"
    assert "type: postgres" not in triggers, "RLS-blind raw-SQL trigger returned to the manifest"


def test_external_metric_hpa_names_the_exported_gauge() -> None:
    """The HPA must point at the gauge we publish, not merely mention it in a name."""
    rendered = render_external_metric()

    assert re.search(r"metric:\s*\n\s*name: plexus_queue_depth", rendered), rendered
