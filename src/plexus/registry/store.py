"""Model registry, deployments, and runs: the durable truth of what may serve.

Two rules shape this module. (1) Versions are content-addressed by digest, so a
retried registration is idempotent rather than a second row that confuses rollout.
(2) Anything that can receive production traffic must have passed an eval and be
signed *before* promotion -- the gate lives here, not in CI, so no code path can
forget it. Every state change writes an outbox event in the same transaction.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from ..errors import Conflict, NotFound, QuotaExceeded, ValidationFailed
from ..ids import new_id
from ..scaling.hashring import HashRing
from ..store.db import Tx
from ..store.uow import OutboxRepository

STAGE_NONE = "none"
STAGE_STAGING = "staging"
STAGE_CANARY = "canary"
STAGE_PRODUCTION = "production"
STAGE_ARCHIVED = "archived"

STAGES: tuple[str, ...] = (STAGE_NONE, STAGE_STAGING, STAGE_CANARY, STAGE_PRODUCTION, STAGE_ARCHIVED)
# Stages that may receive user traffic: they carry the same release gate.
TRAFFIC_STAGES: frozenset[str] = frozenset({STAGE_CANARY, STAGE_PRODUCTION})

DEPLOY_PENDING = "pending"
DEPLOY_PROGRESSING = "progressing"
DEPLOY_READY = "ready"
DEPLOY_FAILED = "failed"
DEPLOY_TERMINATED = "terminated"


def now_ms() -> int:
    return int(time.time() * 1000)


def _dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, separators=(",", ":"), sort_keys=True)


def _loads(raw: Any, default: Any = None) -> Any:
    if raw in (None, ""):
        return {} if default is None else default
    return json.loads(raw)


@dataclass(frozen=True, slots=True)
class Artifact:
    id: str
    tenant_id: str
    digest: str
    kind: str
    uri: str
    size_bytes: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at_ms: int = 0

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Artifact:
        return cls(
            id=row["id"],
            tenant_id=row["tenant_id"],
            digest=row["digest"],
            kind=row["kind"],
            uri=row["uri"],
            size_bytes=int(row["size_bytes"] or 0),
            metadata=_loads(row["metadata_json"]),
            created_at_ms=int(row["created_at_ms"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "digest": self.digest,
            "kind": self.kind,
            "uri": self.uri,
            "size_bytes": self.size_bytes,
            "metadata": self.metadata,
            "created_at_ms": self.created_at_ms,
        }


@dataclass(frozen=True, slots=True)
class ModelVersion:
    id: str
    tenant_id: str
    name: str
    version: int
    digest: str
    stage: str = STAGE_NONE
    created_by: str = ""
    parent_id: str | None = None
    size_bytes: int = 0
    eval_passed: bool | None = None
    eval_metrics: dict[str, Any] = field(default_factory=dict)
    signature: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at_ms: int = 0

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> ModelVersion:
        raw_eval = row["eval_passed"]
        return cls(
            id=row["id"],
            tenant_id=row["tenant_id"],
            name=row["name"],
            version=int(row["version"]),
            digest=row["digest"],
            stage=row["stage"],
            created_by=row["created_by"],
            parent_id=row["parent_id"],
            size_bytes=int(row["size_bytes"] or 0),
            eval_passed=None if raw_eval is None else bool(raw_eval),
            eval_metrics=_loads(row["eval_json"]),
            signature=row["signature"],
            metadata=_loads(row["metadata_json"]),
            created_at_ms=int(row["created_at_ms"]),
        )

    @property
    def serves_traffic(self) -> bool:
        return self.stage in TRAFFIC_STAGES

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "version": self.version,
            "digest": self.digest,
            "stage": self.stage,
            "created_by": self.created_by,
            "parent_id": self.parent_id,
            "size_bytes": self.size_bytes,
            "eval_passed": self.eval_passed,
            "eval_metrics": self.eval_metrics,
            "signature": self.signature,
            "metadata": self.metadata,
            "created_at_ms": self.created_at_ms,
        }


@dataclass(frozen=True, slots=True)
class Deployment:
    id: str
    tenant_id: str
    name: str
    model_version_id: str
    status: str = DEPLOY_PENDING
    desired_replicas: int = 1
    min_replicas: int = 1
    max_replicas: int = 3
    traffic_percent: int = 100
    shard: int = 0
    created_at_ms: int = 0
    updated_at_ms: int = 0

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Deployment:
        return cls(
            id=row["id"],
            tenant_id=row["tenant_id"],
            name=row["name"],
            model_version_id=row["model_version_id"],
            status=row["status"],
            desired_replicas=int(row["desired_replicas"]),
            min_replicas=int(row["min_replicas"]),
            max_replicas=int(row["max_replicas"]),
            traffic_percent=int(row["traffic_percent"]),
            shard=int(row["shard"]),
            created_at_ms=int(row["created_at_ms"]),
            updated_at_ms=int(row["updated_at_ms"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "model_version_id": self.model_version_id,
            "status": self.status,
            "desired_replicas": self.desired_replicas,
            "min_replicas": self.min_replicas,
            "max_replicas": self.max_replicas,
            "traffic_percent": self.traffic_percent,
            "shard": self.shard,
            "created_at_ms": self.created_at_ms,
            "updated_at_ms": self.updated_at_ms,
        }


@dataclass(frozen=True, slots=True)
class Run:
    id: str
    tenant_id: str
    kind: str
    status: str = "running"
    model_version_id: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    started_at_ms: int = 0
    finished_at_ms: int | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Run:
        return cls(
            id=row["id"],
            tenant_id=row["tenant_id"],
            kind=row["kind"],
            status=row["status"],
            model_version_id=row["model_version_id"],
            metrics=_loads(row["metrics_json"]),
            error=row["error"],
            started_at_ms=int(row["started_at_ms"]),
            finished_at_ms=None if row["finished_at_ms"] is None else int(row["finished_at_ms"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "status": self.status,
            "model_version_id": self.model_version_id,
            "metrics": self.metrics,
            "error": self.error,
            "started_at_ms": self.started_at_ms,
            "finished_at_ms": self.finished_at_ms,
        }


class ModelRepository:
    """Content-addressed model versions with hard promotion gates."""

    def __init__(self, tx: Tx, *, outbox: OutboxRepository | None = None) -> None:
        self._tx = tx
        self._outbox = outbox or OutboxRepository(tx)

    def register(
        self,
        *,
        tenant_id: str,
        name: str,
        digest: str,
        created_by: str,
        parent_id: str | None = None,
        size_bytes: int = 0,
        metadata: dict[str, Any] | None = None,
        uri: str | None = None,
        kind: str = "weights",
        max_versions: int | None = None,
    ) -> tuple[ModelVersion, bool]:
        """Return (version, created). Re-registering a digest is idempotent in-tenant."""
        if not name or not digest:
            raise ValidationFailed("model name and digest are required")
        existing = self._tx.query_one(
            "SELECT * FROM model_versions WHERE tenant_id = ? AND digest = ?",
            (tenant_id, digest),
        )
        if existing is not None:
            return ModelVersion.from_row(existing), False
        if max_versions is not None and self.count(tenant_id=tenant_id) >= max_versions:
            raise QuotaExceeded(
                "model version limit reached",
                details={"limit": max_versions},
                retry_after_s=3600.0,
            )
        version = int(self._tx.scalar(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM model_versions WHERE tenant_id = ? AND name = ?",
            (tenant_id, name),
        ) or 1)
        record = ModelVersion(
            id=new_id("mv"),
            tenant_id=tenant_id,
            name=name,
            version=version,
            digest=digest,
            created_by=created_by,
            parent_id=parent_id,
            size_bytes=size_bytes,
            metadata=metadata or {},
            created_at_ms=now_ms(),
        )
        try:
            self._tx.execute(
                """
                INSERT INTO model_versions (id, tenant_id, name, version, digest, stage, created_by,
                    parent_id, size_bytes, eval_passed, eval_json, signature, metadata_json, created_at_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, '{}', NULL, ?, ?)
                """,
                (
                    record.id,
                    tenant_id,
                    name,
                    version,
                    digest,
                    STAGE_NONE,
                    created_by,
                    parent_id,
                    size_bytes,
                    _dumps(record.metadata),
                    record.created_at_ms,
                ),
            )
        except Exception as exc:
            # A concurrent registration of the same digest wins on the unique index; that
            # is a successful (idempotent) call, not a failure the client should retry.
            if _is_unique_violation(exc):
                raced = self._tx.query_one(
                    "SELECT * FROM model_versions WHERE tenant_id = ? AND digest = ?",
                    (tenant_id, digest),
                )
                if raced is not None:
                    return ModelVersion.from_row(raced), False
            raise
        if uri:
            self.attach_artifact(
                tenant_id=tenant_id,
                digest=digest,
                kind=kind,
                uri=uri,
                size_bytes=size_bytes,
            )
        self._outbox.append(
            tenant_id=tenant_id,
            type="model.version.registered",
            subject=record.id,
            payload={"name": name, "version": version, "digest": digest},
        )
        return record, True

    def attach_artifact(
        self,
        *,
        tenant_id: str,
        digest: str,
        kind: str,
        uri: str,
        size_bytes: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> Artifact:
        existing = self._tx.query_one(
            "SELECT * FROM artifacts WHERE tenant_id = ? AND digest = ? AND kind = ?",
            (tenant_id, digest, kind),
        )
        if existing is not None:
            return Artifact.from_row(existing)
        artifact = Artifact(
            id=new_id("art"),
            tenant_id=tenant_id,
            digest=digest,
            kind=kind,
            uri=uri,
            size_bytes=size_bytes,
            metadata=metadata or {},
            created_at_ms=now_ms(),
        )
        self._tx.execute(
            """
            INSERT INTO artifacts (id, tenant_id, digest, kind, uri, size_bytes, metadata_json, created_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact.id,
                tenant_id,
                digest,
                kind,
                uri,
                size_bytes,
                _dumps(artifact.metadata),
                artifact.created_at_ms,
            ),
        )
        return artifact

    def artifacts(self, *, tenant_id: str, digest: str) -> list[Artifact]:
        rows = self._tx.query(
            "SELECT * FROM artifacts WHERE tenant_id = ? AND digest = ? ORDER BY kind",
            (tenant_id, digest),
        )
        return [Artifact.from_row(row) for row in rows]

    def record_eval(
        self,
        *,
        tenant_id: str,
        version_id: str,
        passed: bool,
        metrics: dict[str, Any] | None = None,
    ) -> ModelVersion:
        self._require_row(tenant_id, version_id)
        self._tx.execute(
            "UPDATE model_versions SET eval_passed = ?, eval_json = ? WHERE tenant_id = ? AND id = ?",
            (1 if passed else 0, _dumps(metrics or {}), tenant_id, version_id),
        )
        version = self.require(tenant_id, version_id)
        self._outbox.append(
            tenant_id=tenant_id,
            type="model.version.evaluated",
            subject=version_id,
            payload={"passed": passed, "metrics": metrics or {}},
        )
        return version

    def sign(self, *, tenant_id: str, version_id: str, signature: str) -> ModelVersion:
        if not signature:
            raise ValidationFailed("signature must not be empty")
        self._require_row(tenant_id, version_id)
        self._tx.execute(
            "UPDATE model_versions SET signature = ? WHERE tenant_id = ? AND id = ?",
            (signature, tenant_id, version_id),
        )
        return self.require(tenant_id, version_id)

    def promote(self, *, tenant_id: str, version_id: str, stage: str, actor: str) -> ModelVersion:
        """Move a version to `stage`, refusing unverified builds for traffic stages."""
        if stage not in STAGES:
            raise ValidationFailed(
                "unknown stage",
                details={"stage": stage, "allowed": list(STAGES)},
            )
        version = self._require_row(tenant_id, version_id)
        if stage in TRAFFIC_STAGES:
            missing: list[str] = []
            if not version.eval_passed:
                missing.append("eval_passed")
            if not version.signature:
                missing.append("signature")
            if missing:
                raise Conflict(
                    "promotion blocked by release gate",
                    details={"missing": missing, "stage": stage, "version_id": version_id},
                )
        previous = version.stage
        if stage in TRAFFIC_STAGES:
            # Only one live version per name per traffic stage: two of them would make
            # "the model in production" ambiguous for rollback and audit.
            self._tx.execute(
                "UPDATE model_versions SET stage = ? WHERE tenant_id = ? AND name = ? AND stage = ?",
                (STAGE_STAGING, tenant_id, version.name, stage),
            )
        self._tx.execute(
            "UPDATE model_versions SET stage = ? WHERE tenant_id = ? AND id = ?",
            (stage, tenant_id, version_id),
        )
        self._outbox.append(
            tenant_id=tenant_id,
            type="model.version.promoted",
            subject=version_id,
            payload={"from": previous, "to": stage, "actor": actor},
        )
        return self.require(tenant_id, version_id)

    def get(self, tenant_id: str, version_id: str) -> ModelVersion | None:
        row = self._tx.query_one(
            "SELECT * FROM model_versions WHERE tenant_id = ? AND id = ?",
            (tenant_id, version_id),
        )
        return ModelVersion.from_row(row) if row else None

    def require(self, tenant_id: str, version_id: str) -> ModelVersion:
        version = self.get(tenant_id, version_id)
        if version is None:
            raise NotFound("model version not found", details={"version_id": version_id})
        return version

    def by_name(self, tenant_id: str, name: str, version: int) -> ModelVersion | None:
        row = self._tx.query_one(
            "SELECT * FROM model_versions WHERE tenant_id = ? AND name = ? AND version = ?",
            (tenant_id, name, version),
        )
        return ModelVersion.from_row(row) if row else None

    def latest(self, tenant_id: str, name: str, *, stage: str | None = None) -> ModelVersion | None:
        sql = "SELECT * FROM model_versions WHERE tenant_id = ? AND name = ?"
        params: list[Any] = [tenant_id, name]
        if stage is not None:
            sql += " AND stage = ?"
            params.append(stage)
        sql += " ORDER BY version DESC LIMIT 1"
        row = self._tx.query_one(sql, params)
        return ModelVersion.from_row(row) if row else None

    def lineage(self, *, tenant_id: str, version_id: str, max_depth: int = 32) -> list[ModelVersion]:
        """Walk parent_id to the root; provenance questions must be answerable in one call."""
        chain: list[ModelVersion] = []
        seen: set[str] = set()
        cursor: str | None = version_id
        while cursor is not None and len(chain) <= max_depth:
            if cursor in seen:
                raise Conflict("lineage cycle detected", details={"version_id": cursor})
            seen.add(cursor)
            row = self._tx.query_one(
                "SELECT * FROM model_versions WHERE tenant_id = ? AND id = ?",
                (tenant_id, cursor),
            )
            if row is None:
                break
            version = ModelVersion.from_row(row)
            chain.append(version)
            cursor = version.parent_id
        if not chain:
            raise NotFound("model version not found", details={"version_id": version_id})
        return chain

    def list(self, *, tenant_id: str, name: str | None = None, stage: str | None = None, limit: int = 50) -> list[ModelVersion]:
        sql = "SELECT * FROM model_versions WHERE tenant_id = ?"
        params: list[Any] = [tenant_id]
        if name is not None:
            sql += " AND name = ?"
            params.append(name)
        if stage is not None:
            sql += " AND stage = ?"
            params.append(stage)
        sql += " ORDER BY created_at_ms DESC LIMIT ?"
        params.append(limit)
        return [ModelVersion.from_row(row) for row in self._tx.query(sql, params)]

    def count(self, *, tenant_id: str) -> int:
        return int(self._tx.scalar("SELECT COUNT(*) FROM model_versions WHERE tenant_id = ?", (tenant_id,)) or 0)

    def _require_row(self, tenant_id: str, version_id: str) -> ModelVersion:
        row = self._tx.query_one(
            "SELECT * FROM model_versions WHERE tenant_id = ? AND id = ?",
            (tenant_id, version_id),
        )
        if row is None:
            raise NotFound("model version not found", details={"version_id": version_id})
        return ModelVersion.from_row(row)


class DeploymentRepository:
    """Desired-state records for serving; the reconciler owns the actual pods."""

    def __init__(self, tx: Tx, *, outbox: OutboxRepository | None = None, shards: int = 64) -> None:
        self._tx = tx
        self._outbox = outbox or OutboxRepository(tx)
        self._shards = max(1, shards)
        self._ring = HashRing.from_members([f"shard-{index}" for index in range(self._shards)])

    def shard_for(self, tenant_id: str, name: str) -> int:
        """Deterministic placement so every replica agrees without a coordination round trip."""
        return self._ring.shard_for(f"{tenant_id}/{name}", self._shards)

    def upsert(
        self,
        *,
        tenant_id: str,
        name: str,
        model_version_id: str,
        min_replicas: int = 1,
        max_replicas: int = 3,
        desired_replicas: int | None = None,
        traffic_percent: int = 100,
        max_replicas_allowed: int | None = None,
    ) -> Deployment:
        version = self._tx.query_one(
            "SELECT * FROM model_versions WHERE tenant_id = ? AND id = ?",
            (tenant_id, model_version_id),
        )
        if version is None:
            raise NotFound("model version not found", details={"version_id": model_version_id})
        if version["stage"] not in TRAFFIC_STAGES:
            raise Conflict(
                "only promoted model versions may be deployed",
                details={"version_id": model_version_id, "stage": version["stage"]},
            )
        if min_replicas < 0 or min_replicas > max_replicas:
            raise ValidationFailed("min_replicas must be within 0..max_replicas")
        if max_replicas_allowed is not None and max_replicas > max_replicas_allowed:
            raise QuotaExceeded(
                "replica ceiling exceeds the plan limit",
                details={"max_replicas": max_replicas, "plan_limit": max_replicas_allowed},
                retry_after_s=3600.0,
            )
        if not 0 <= traffic_percent <= 100:
            raise ValidationFailed("traffic_percent must be between 0 and 100")
        desired = desired_replicas if desired_replicas is not None else min(2, max_replicas)
        desired = max(min_replicas, min(desired, max_replicas))
        now = now_ms()
        existing = self._tx.query_one(
            "SELECT * FROM deployments WHERE tenant_id = ? AND name = ?",
            (tenant_id, name),
        )
        if existing is None:
            deployment = Deployment(
                id=new_id("dep"),
                tenant_id=tenant_id,
                name=name,
                model_version_id=model_version_id,
                status=DEPLOY_PENDING,
                desired_replicas=desired,
                min_replicas=min_replicas,
                max_replicas=max_replicas,
                traffic_percent=traffic_percent,
                shard=self.shard_for(tenant_id, name),
                created_at_ms=now,
                updated_at_ms=now,
            )
            self._tx.execute(
                """
                INSERT INTO deployments (id, tenant_id, name, model_version_id, status, desired_replicas,
                    min_replicas, max_replicas, traffic_percent, shard, created_at_ms, updated_at_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    deployment.id,
                    tenant_id,
                    name,
                    model_version_id,
                    deployment.status,
                    desired,
                    min_replicas,
                    max_replicas,
                    traffic_percent,
                    deployment.shard,
                    now,
                    now,
                ),
            )
            self._outbox.append(
                tenant_id=tenant_id,
                type="deployment.created",
                subject=deployment.id,
                payload={"name": name, "model_version_id": model_version_id, "shard": deployment.shard},
            )
            return deployment
        record = Deployment.from_row(existing)
        changed_model = record.model_version_id != model_version_id
        self._tx.execute(
            """
            UPDATE deployments
               SET model_version_id = ?, status = ?, desired_replicas = ?, min_replicas = ?,
                   max_replicas = ?, traffic_percent = ?, updated_at_ms = ?
             WHERE tenant_id = ? AND id = ?
            """,
            (
                model_version_id,
                DEPLOY_PROGRESSING if changed_model else record.status,
                desired,
                min_replicas,
                max_replicas,
                traffic_percent,
                now,
                tenant_id,
                record.id,
            ),
        )
        self._outbox.append(
            tenant_id=tenant_id,
            type="deployment.updated",
            subject=record.id,
            payload={"name": name, "model_version_id": model_version_id, "desired_replicas": desired},
        )
        return self.require(tenant_id, record.id)

    def set_status(self, tenant_id: str, deployment_id: str, status: str) -> Deployment:
        if status not in {DEPLOY_PENDING, DEPLOY_PROGRESSING, DEPLOY_READY, DEPLOY_FAILED, DEPLOY_TERMINATED}:
            raise ValidationFailed("unknown deployment status", details={"status": status})
        record = self._require_row(tenant_id, deployment_id)
        self._tx.execute(
            "UPDATE deployments SET status = ?, updated_at_ms = ? WHERE tenant_id = ? AND id = ?",
            (status, now_ms(), tenant_id, deployment_id),
        )
        return self.require(tenant_id, record.id)

    def set_traffic(self, tenant_id: str, deployment_id: str, traffic_percent: int) -> Deployment:
        if not 0 <= traffic_percent <= 100:
            raise ValidationFailed("traffic_percent must be between 0 and 100")
        self._require_row(tenant_id, deployment_id)
        self._tx.execute(
            "UPDATE deployments SET traffic_percent = ?, updated_at_ms = ? WHERE tenant_id = ? AND id = ?",
            (traffic_percent, now_ms(), tenant_id, deployment_id),
        )
        self._outbox.append(
            tenant_id=tenant_id,
            type="deployment.traffic",
            subject=deployment_id,
            payload={"traffic_percent": traffic_percent},
        )
        return self.require(tenant_id, deployment_id)

    def scale(self, tenant_id: str, deployment_id: str, replicas: int) -> Deployment:
        record = self._require_row(tenant_id, deployment_id)
        if replicas < record.min_replicas or replicas > record.max_replicas:
            raise Conflict(
                "replica count outside the autoscaling band",
                details={
                    "requested": replicas,
                    "min_replicas": record.min_replicas,
                    "max_replicas": record.max_replicas,
                },
            )
        self._tx.execute(
            "UPDATE deployments SET desired_replicas = ?, status = ?, updated_at_ms = ? WHERE tenant_id = ? AND id = ?",
            (replicas, DEPLOY_PROGRESSING, now_ms(), tenant_id, deployment_id),
        )
        self._outbox.append(
            tenant_id=tenant_id,
            type="deployment.scaled",
            subject=deployment_id,
            payload={"desired_replicas": replicas},
        )
        return self.require(tenant_id, deployment_id)

    def rollback(self, *, tenant_id: str, deployment_id: str, model_version_id: str) -> Deployment:
        record = self._require_row(tenant_id, deployment_id)
        target = self._tx.query_one(
            "SELECT * FROM model_versions WHERE tenant_id = ? AND id = ?",
            (tenant_id, model_version_id),
        )
        if target is None:
            raise NotFound("model version not found", details={"version_id": model_version_id})
        if target["stage"] not in TRAFFIC_STAGES:
            raise Conflict(
                "rollback target is not promoted",
                details={"version_id": model_version_id, "stage": target["stage"]},
            )
        self._tx.execute(
            "UPDATE deployments SET model_version_id = ?, status = ?, updated_at_ms = ? WHERE tenant_id = ? AND id = ?",
            (model_version_id, DEPLOY_PROGRESSING, now_ms(), tenant_id, deployment_id),
        )
        self._outbox.append(
            tenant_id=tenant_id,
            type="deployment.rolled_back",
            subject=deployment_id,
            payload={"from": record.model_version_id, "to": model_version_id},
        )
        return self.require(tenant_id, deployment_id)

    def get(self, tenant_id: str, deployment_id: str) -> Deployment | None:
        row = self._tx.query_one(
            "SELECT * FROM deployments WHERE tenant_id = ? AND id = ?",
            (tenant_id, deployment_id),
        )
        return Deployment.from_row(row) if row else None

    def by_name(self, tenant_id: str, name: str) -> Deployment | None:
        row = self._tx.query_one(
            "SELECT * FROM deployments WHERE tenant_id = ? AND name = ?",
            (tenant_id, name),
        )
        return Deployment.from_row(row) if row else None

    def require(self, tenant_id: str, deployment_id: str) -> Deployment:
        record = self.get(tenant_id, deployment_id)
        if record is None:
            raise NotFound("deployment not found", details={"deployment_id": deployment_id})
        return record

    def list(self, *, tenant_id: str, limit: int = 50) -> list[Deployment]:
        rows = self._tx.query(
            "SELECT * FROM deployments WHERE tenant_id = ? ORDER BY updated_at_ms DESC LIMIT ?",
            (tenant_id, limit),
        )
        return [Deployment.from_row(row) for row in rows]

    def _require_row(self, tenant_id: str, deployment_id: str) -> Deployment:
        record = self.get(tenant_id, deployment_id)
        if record is None:
            raise NotFound("deployment not found", details={"deployment_id": deployment_id})
        return record


class RunRepository:
    def __init__(self, tx: Tx) -> None:
        self._tx = tx

    def start(
        self,
        *,
        tenant_id: str,
        kind: str,
        model_version_id: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> Run:
        run = Run(
            id=new_id("run"),
            tenant_id=tenant_id,
            kind=kind,
            model_version_id=model_version_id,
            metrics=metrics or {},
            started_at_ms=now_ms(),
        )
        self._tx.execute(
            """
            INSERT INTO runs (id, tenant_id, kind, status, model_version_id, metrics_json, error,
                started_at_ms, finished_at_ms)
            VALUES (?, ?, ?, 'running', ?, ?, NULL, ?, NULL)
            """,
            (run.id, tenant_id, kind, model_version_id, _dumps(run.metrics), run.started_at_ms),
        )
        return run

    def finish(
        self,
        *,
        tenant_id: str,
        run_id: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> Run:
        if status not in {"succeeded", "failed", "cancelled"}:
            raise ValidationFailed("unknown run status", details={"status": status})
        self._require_row(tenant_id, run_id)
        self._tx.execute(
            """
            UPDATE runs SET status = ?, metrics_json = ?, error = ?, finished_at_ms = ?
             WHERE tenant_id = ? AND id = ? AND finished_at_ms IS NULL
            """,
            (status, _dumps(metrics or {}), error, now_ms(), tenant_id, run_id),
        )
        return self.require(tenant_id, run_id)

    def get(self, tenant_id: str, run_id: str) -> Run | None:
        row = self._tx.query_one(
            "SELECT * FROM runs WHERE tenant_id = ? AND id = ?",
            (tenant_id, run_id),
        )
        return Run.from_row(row) if row else None

    def require(self, tenant_id: str, run_id: str) -> Run:
        run = self.get(tenant_id, run_id)
        if run is None:
            raise NotFound("run not found", details={"run_id": run_id})
        return run

    def list(self, *, tenant_id: str, status: str | None = None, limit: int = 50) -> list[Run]:
        sql = "SELECT * FROM runs WHERE tenant_id = ?"
        params: list[Any] = [tenant_id]
        if status is not None:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY started_at_ms DESC LIMIT ?"
        params.append(limit)
        return [Run.from_row(row) for row in self._tx.query(sql, params)]

    def _require_row(self, tenant_id: str, run_id: str) -> Run:
        return self.require(tenant_id, run_id)


def _is_unique_violation(exc: BaseException) -> bool:
    name = type(exc).__name__
    return "Unique" in name or "Integrity" in name or "UniqueViolation" in name
