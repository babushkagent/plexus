"""Durable queue surface: idempotent writes, task reads, dead-letter listings.

Every route here serializes state that lives behind a transaction, so they are the
paths where a missing serializer or an unstable request fingerprint turns into a 500.
"""

from __future__ import annotations

from typing import Any

from conftest import Client
from plexus.api.server import App


def test_task_round_trips_through_the_api(scoped_client: Client) -> None:
    enqueued = scoped_client.post("/v1/tasks", {"type": "index.build", "payload": {"shard": 3}})
    assert enqueued.status == 201
    task_id: str = enqueued.payload["id"]  # type: ignore[index]

    response = scoped_client.get(f"/v1/tasks/{task_id}")
    assert response.status == 200
    body: dict[str, Any] = response.payload  # type: ignore[assignment]
    assert body["id"] == task_id
    assert body["type"] == "index.build"
    assert body["status"] == "pending"
    assert body["payload"] == {"shard": 3}


def test_idempotent_retry_is_replayed_not_re_enqueued(scoped_client: Client) -> None:
    headers = {"idempotency-key": "same-key-every-time"}
    first = scoped_client.post("/v1/tasks", {"type": "index.build"}, headers=headers)
    assert first.status == 201

    retry = scoped_client.post("/v1/tasks", {"type": "index.build"}, headers=headers)
    assert retry.status == 201
    assert retry.payload == first.payload
    assert retry.headers.get("X-Plexus-Replayed") == "true"


def test_reusing_an_idempotency_key_for_different_work_conflicts(
    scoped_client: Client,
) -> None:
    """A key pins one request: silently accepting a second payload would lose an effect."""
    headers = {"idempotency-key": "reused-for-different-body"}
    assert scoped_client.post("/v1/tasks", {"type": "index.build"}, headers=headers).status == 201

    conflict = scoped_client.post("/v1/tasks", {"type": "report.build"}, headers=headers)
    assert conflict.status == 409
    assert conflict.payload["error"]["code"] == "idempotency_conflict"  # type: ignore[index]


def test_dead_letters_are_tenant_scoped(app: App, scoped_client: Client, other_tenant: str) -> None:
    from conftest import mint_key

    foreign = Client(app, api_key=mint_key(app, other_tenant))
    mine = scoped_client.json_of(scoped_client.get("/v1/dead-letters"))
    theirs = foreign.json_of(foreign.get("/v1/dead-letters"))
    assert mine["items"] == []
    assert theirs["items"] == []
