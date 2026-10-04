"""End-to-end behaviour of the HTTP surface, driven through the real pipeline."""

from __future__ import annotations

from typing import Any

from plexus.api.server import App

from conftest import Client


def test_liveness_and_readiness_are_public(app: App, client: Client) -> None:
    live = client.get("/healthz")
    assert live.status == 200
    body: dict[str, Any] = live.payload  # type: ignore[assignment]
    assert body["status"] == "ok"
    assert client.get("/readyz").status == 200


def test_protected_route_requires_a_credential(scoped_client: Client) -> None:
    anonymous = Client(scoped_client.app)
    assert anonymous.get("/v1/tenant").status == 401


def test_tenant_reads_its_own_profile(scoped_client: Client) -> None:
    response = scoped_client.get("/v1/tenant")
    assert response.status == 200
    body: dict[str, Any] = response.payload  # type: ignore[assignment]
    assert body["name"] == "acme"
    assert body["effective_limits"]["requests_per_second"] > 0


def test_cross_tenant_reads_are_indistinguishable_from_missing(
    app: App, tenant: str, other_tenant: str
) -> None:
    from conftest import mint_key

    intruder = Client(app, api_key=mint_key(app, other_tenant))
    # A foreign tenant id in the path must not leak that the row exists.
    assert intruder.get(f"/v1/platform/tenants/{tenant}").status in (403, 404)
    assert intruder.get("/v1/tenant").payload["name"] == "globex"


def test_completion_is_billed_and_audited(scoped_client: Client) -> None:
    response = scoped_client.post(
        "/v1/chat/completions", {"model": "echo", "messages": [{"role": "user", "content": "hello"}]}
    )
    assert response.status == 200
    body: dict[str, Any] = response.payload  # type: ignore[assignment]
    assert "hello" in body["choices"][0]["message"]["content"]
    # OpenAI-compatible envelope: an existing SDK client must parse this unchanged.
    assert body["object"] == "chat.completion"
    assert body["id"].startswith("chatcmpl-")
    assert body["usage"]["total_tokens"] > 0
    # ...and the platform extensions that make spend/fallbacks auditable per request.
    assert body["provider"] in {"echo", "echo-b"}  # hash-chosen candidate, stable per tenant
    assert body["attempts"][0]["ok"] is True

    usage = scoped_client.json_of(scoped_client.get("/v1/usage"))
    assert usage["requests"] == 1
    audit = scoped_client.json_of(scoped_client.get("/v1/audit"))
    assert any(item["action"] == "inference.completion" for item in audit["items"])


def test_unknown_model_is_rejected_before_any_provider_call(
    scoped_client: Client, providers: list[Any]
) -> None:
    response = scoped_client.post(
        "/v1/chat/completions", {"model": "gpt-99", "messages": [{"role": "user", "content": "x"}]}
    )
    assert response.status == 400
    error = scoped_client.json_of(response)["error"]
    assert error["code"] == "validation_failed"
    assert error["retryable"] is False
    assert sum(provider.calls for provider in providers) == 0


def test_streaming_returns_server_sent_events(scoped_client: Client) -> None:
    response = scoped_client.post(
        "/v1/chat/completions",
        {"model": "echo", "stream": True, "messages": [{"role": "user", "content": "stream me"}]},
    )
    assert response.status == 200
    assert response.content_type == "text/event-stream"
    assert response.events is not None
    events = list(response.events)
    assert events, "stream produced no events"
    chunks = [event["choices"][0]["delta"].get("content", "") for event in events]
    assert "".join(chunks).endswith("stream me")
    assert events[-1]["choices"][0]["finish_reason"] == "stop"
    assert events[-1]["plexus"]["attempts"], "final chunk must report which provider answered"


def test_metrics_are_exposed_in_prometheus_format(client: Client, platform_jwt: str) -> None:
    response = client.get("/metrics", token=platform_jwt)
    assert response.status == 200
    assert "plexus_http_requests_total" in (response.raw or "")


def test_malformed_body_is_a_400_not_a_500(scoped_client: Client) -> None:
    response = scoped_client.request("POST", "/v1/chat/completions", headers={"content-type": "application/json"})
    assert response.status in (400, 422)
