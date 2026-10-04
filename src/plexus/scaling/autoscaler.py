"""Demand-driven replica control with stabilization windows.

Queue depth is the signal that matters for a durable work queue: it measures backlog
directly instead of proxying it through CPU. Latency is used as a guard rail so a
stalled dependency cannot be "fixed" by adding replicas forever. Hysteresis (slow
scale-down, fast scale-up) keeps a fleet from oscillating around its target.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from plexus.config import Settings

Clock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class LoadSample:
    ready_depth: int
    replicas: int = 1
    p95_latency_ms: float | None = None
    rps: float | None = None
    at_s: float | None = None

    @classmethod
    def from_queue(
        cls,
        depth: Mapping[str, int],
        *,
        replicas: int = 1,
        p95_latency_ms: float | None = None,
        rps: float | None = None,
    ) -> LoadSample:
        return cls(
            ready_depth=int(depth.get("ready", depth.get("pending", 0))),
            replicas=replicas,
            p95_latency_ms=p95_latency_ms,
            rps=rps,
        )


@dataclass(frozen=True, slots=True)
class ScalingDecision:
    desired: int
    previous: int
    reason: str
    signals: dict[str, float] = field(default_factory=dict)
    pending_since_s: float | None = None

    @property
    def changed(self) -> bool:
        return self.desired != self.previous


class Autoscaler:
    """Queue-depth target with latency guard rail and asymmetric stabilization."""

    def __init__(
        self,
        *,
        min_replicas: int = 2,
        max_replicas: int = 40,
        target_queue_depth: int = 64,
        target_p95_ms: float | None = None,
        stabilize_up_s: float = 30.0,
        stabilize_down_s: float = 300.0,
        emergency_multiplier: float = 4.0,
        clock: Clock = time.monotonic,
    ) -> None:
        if min_replicas < 1:
            raise ValueError("min_replicas must be >= 1")
        if max_replicas < min_replicas:
            raise ValueError("max_replicas must be >= min_replicas")
        if target_queue_depth < 1:
            raise ValueError("target_queue_depth must be >= 1")
        self.min_replicas = int(min_replicas)
        self.max_replicas = int(max_replicas)
        self.target_queue_depth = int(target_queue_depth)
        self.target_p95_ms = target_p95_ms
        self.stabilize_up_s = float(stabilize_up_s)
        self.stabilize_down_s = float(stabilize_down_s)
        self.emergency_multiplier = float(emergency_multiplier)
        self._clock = clock
        self._desired = int(min_replicas)
        self._candidate: int | None = None
        self._candidate_since: float | None = None

    @property
    def desired(self) -> int:
        return self._desired

    @classmethod
    def from_settings(cls, settings: Settings, **overrides: object) -> Autoscaler:
        options: dict[str, object] = {
            "min_replicas": settings.api_min_replicas,
            "max_replicas": settings.api_max_replicas,
            "target_queue_depth": settings.target_queue_depth,
            "stabilize_up_s": settings.stabilize_up_s,
            "stabilize_down_s": settings.stabilize_down_s,
        }
        options.update(overrides)
        return cls(**options)  # type: ignore[arg-type]

    def decide(self, sample: LoadSample) -> ScalingDecision:
        now = self._clock() if sample.at_s is None else float(sample.at_s)
        proposed, reason = self._propose(sample)
        signals = {
            "ready_depth": float(sample.ready_depth),
            "replicas": float(sample.replicas),
            "per_replica": (sample.ready_depth / max(1, sample.replicas)),
            "target_queue_depth": float(self.target_queue_depth),
        }
        if sample.p95_latency_ms is not None:
            signals["p95_latency_ms"] = float(sample.p95_latency_ms)
        if sample.rps is not None:
            signals["rps"] = float(sample.rps)

        if proposed == self._desired:
            self._candidate = None
            self._candidate_since = None
            return ScalingDecision(self._desired, self._desired, "stable", signals)

        if self._candidate != proposed:
            self._candidate = proposed
            self._candidate_since = now
            return ScalingDecision(self._desired, self._desired, f"observing {reason}", signals, pending_since_s=now)

        holding = now - (self._candidate_since or now)
        scaling_up = proposed > self._desired
        emergency = scaling_up and sample.ready_depth >= self.target_queue_depth * self.emergency_multiplier
        window = 0.0 if emergency else (self.stabilize_up_s if scaling_up else self.stabilize_down_s)
        if holding >= window:
            previous = self._desired
            self._desired = proposed
            self._candidate = None
            self._candidate_since = None
            reason = f"emergency_{reason}" if emergency else reason
            return ScalingDecision(proposed, previous, reason, signals)
        return ScalingDecision(self._desired, self._desired, f"stabilizing {reason}", signals, pending_since_s=holding)

    def _propose(self, sample: LoadSample) -> tuple[int, str]:
        depth = max(0, int(sample.ready_depth))
        replicas = max(1, int(sample.replicas))
        per_replica = depth / replicas
        if depth == 0:
            proposed, reason = self.min_replicas, "no backlog"
        else:
            proposed = math.ceil(replicas * (per_replica / self.target_queue_depth))
            reason = "queue depth"
        if self.target_p95_ms and sample.p95_latency_ms is not None:
            if sample.p95_latency_ms > self.target_p95_ms * 1.5 and depth > 0:
                proposed, reason = max(proposed, replicas + 1), "p95 latency guard"
            elif sample.p95_latency_ms < self.target_p95_ms * 0.25 and depth == 0:
                proposed, reason = min(proposed, max(self.min_replicas, replicas - 1)), "latency headroom"
        return clamp(proposed, self.min_replicas, self.max_replicas), reason

    def reset(self, *, desired: int | None = None) -> None:
        self._desired = self.min_replicas if desired is None else clamp(desired, self.min_replicas, self.max_replicas)
        self._candidate = None
        self._candidate_since = None


def clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, int(value)))


def render_hpa(
    *,
    name: str = "plexus-api",
    namespace: str = "plexus",
    min_replicas: int = 2,
    max_replicas: int = 40,
    target_utilization: int = 65,
    scale_down_stabilization_s: int = 300,
    scale_up_stabilization_s: int = 30,
) -> str:
    return f"""apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {name}
  namespace: {namespace}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {name}
  minReplicas: {min_replicas}
  maxReplicas: {max_replicas}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {target_utilization}
  behavior:
    scaleUp:
      stabilizationWindowSeconds: {scale_up_stabilization_s}
      policies:
        - type: Percent
          value: 100
          periodSeconds: 30
    scaleDown:
      stabilizationWindowSeconds: {scale_down_stabilization_s}
      policies:
        - type: Pods
          value: 1
          periodSeconds: 60
"""


def render_keda(
    *,
    name: str = "plexus-worker",
    namespace: str = "plexus",
    min_replicas: int = 1,
    max_replicas: int = 50,
    target_queue_depth: int = 64,
    prometheus_url: str = "http://prometheus-operated.monitoring:9090",
) -> str:
    """ScaledObject driven by the exported backlog gauge, never by raw SQL.

    A `postgres` trigger would COUNT the tasks table from its own connection, and
    row-level security only reveals queue rows to connections that declare the worker
    scope -- so KEDA would read a reassuring 0 while 200k tasks waited. The API process
    does hold that scope, so it publishes `plexus_queue_depth` and KEDA consumes the same
    number the in-process autoscaler uses. `max`, not `sum`: every replica reports the
    same database-wide depth, so summing would multiply it by the replica count.
    """
    return f"""apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: {name}
  namespace: {namespace}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {name}
  minReplicaCount: {min_replicas}
  maxReplicaCount: {max_replicas}
  pollingInterval: 30
  cooldownPeriod: 300
  triggers:
    - type: prometheus
      authModes: bearer
      metadata:
        serverAddress: {prometheus_url}
        metricName: plexus_queue_depth
        query: max(plexus_queue_depth)
        threshold: "{target_queue_depth}"
      authenticationRef:
        name: plexus-prometheus-access
"""


def render_external_metric(*, name: str = "plexus_queue_depth", namespace: str = "plexus") -> str:
    """Backlog HPA for clusters that have prometheus-adapter but not KEDA.

    Every replica exports the same database-wide depth, so the Pods average equals that
    depth and `averageValue` acts as a global backlog threshold rather than a per-replica
    share. Still correct -- more workers drain the backlog and the metric falls -- it just
    reacts in whole-backlog steps instead of per-pod ones.
    """
    return f"""apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {name}
  namespace: {namespace}
  annotations:
    plexus.io/description: Backlog-driven scaling for the durable task worker
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: plexus-worker
  minReplicas: 1
  maxReplicas: 50
  metrics:
    - type: Pods
      pods:
        metric:
          name: {name}
        target:
          type: AverageValue
          averageValue: "64"
"""
