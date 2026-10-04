"""Weighted fair scheduling with noisy-neighbour protection.

A single tenant that enqueues 100k embedding jobs must not starve everyone else's
inference batch. Flows are served in virtual-time order (stride = 1/weight), which
gives proportional fairness without a priority inversion, plus a share ceiling that
caps how much of a scheduling window one tenant may take.
"""

from __future__ import annotations

import threading
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class _Flow(Generic[T]):
    tenant_id: str
    weight: float = 1.0
    virtual_time: float = 0.0
    items: deque[T] = field(default_factory=deque)

    @property
    def backlog(self) -> int:
        return len(self.items)


@dataclass(frozen=True, slots=True)
class FlowSnapshot:
    tenant_id: str
    weight: float
    backlog: int
    virtual_time: float
    share: float


class FairQueue(Generic[T]):
    """Virtual-time scheduler over per-tenant FIFO flows."""

    def __init__(self, *, default_weight: float = 1.0, max_share: float = 1.0, window: int = 256) -> None:
        if default_weight <= 0:
            raise ValueError("default_weight must be > 0")
        if not 0 < max_share <= 1:
            raise ValueError("max_share must be in (0, 1]")
        self._default_weight = default_weight
        self._max_share = max_share
        self._window = max(2, int(window))
        self._flows: dict[str, _Flow[T]] = {}
        self._recent: deque[str] = deque(maxlen=self._window)
        self._served_total = 0
        self._lock = threading.RLock()

    def offer(self, tenant_id: str, item: T, *, weight: float | None = None) -> None:
        with self._lock:
            flow = self._flows.get(tenant_id)
            if flow is None:
                flow = _Flow(tenant_id=tenant_id, weight=self._weight_for(weight))
                self._flows[tenant_id] = flow
            elif weight is not None:
                flow.weight = self._weight_for(weight)
            flow.items.append(item)

    def set_weight(self, tenant_id: str, weight: float) -> None:
        with self._lock:
            flow = self._flows.get(tenant_id)
            if flow is not None:
                flow.weight = self._weight_for(weight)

    def pick(self) -> tuple[str, T] | None:
        """Serve the next item under weighted fairness; None when idle."""
        with self._lock:
            tenant_id = self._select()
            if tenant_id is None:
                return None
            flow = self._flows[tenant_id]
            item = flow.items.popleft()
            flow.virtual_time += 1.0 / flow.weight
            self._recent.append(tenant_id)
            self._served_total += 1
            if not flow.items and flow.virtual_time > 1e6:
                # Prevent unbounded virtual-time drift for long-lived idle flows.
                self._renormalize()
            return tenant_id, item

    def peek(self) -> tuple[str, T] | None:
        with self._lock:
            tenant_id = self._select()
            if tenant_id is None:
                return None
            return tenant_id, self._flows[tenant_id].items[0]

    def plan(self, limit: int) -> list[str]:
        """Dry-run schedule for the next `limit` services (used to batch queue claims)."""
        with self._lock:
            virtual = {tenant: flow.virtual_time for tenant, flow in self._flows.items() if flow.items}
            weights = {tenant: flow.weight for tenant, flow in self._flows.items()}
            remaining = {tenant: flow.backlog for tenant, flow in self._flows.items() if flow.items}
            recent = deque(self._recent)
            order: list[str] = []
            for _ in range(max(0, int(limit))):
                ready = [tenant for tenant, left in remaining.items() if left > 0]
                if not ready:
                    break
                tenant = self._select_from(ready, virtual, weights, recent)
                order.append(tenant)
                virtual[tenant] += 1.0 / weights[tenant]
                remaining[tenant] -= 1
                recent.append(tenant)
                if len(recent) > self._window:
                    recent.popleft()
            return order

    def depth(self, tenant_id: str | None = None) -> int:
        with self._lock:
            if tenant_id is not None:
                flow = self._flows.get(tenant_id)
                return flow.backlog if flow else 0
            return sum(flow.backlog for flow in self._flows.values())

    def tenants(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._flows))

    def share(self, tenant_id: str) -> float:
        with self._lock:
            if not self._recent:
                return 0.0
            return self._recent.count(tenant_id) / len(self._recent)

    def shares(self) -> dict[str, float]:
        with self._lock:
            total = len(self._recent)
            if not total:
                return {}
            counts: dict[str, int] = {}
            for tenant in self._recent:
                counts[tenant] = counts.get(tenant, 0) + 1
            return {tenant: count / total for tenant, count in sorted(counts.items())}

    def snapshot(self) -> list[FlowSnapshot]:
        with self._lock:
            return [
                FlowSnapshot(
                    tenant_id=flow.tenant_id,
                    weight=flow.weight,
                    backlog=flow.backlog,
                    virtual_time=flow.virtual_time,
                    share=self.share(flow.tenant_id),
                )
                for flow in sorted(self._flows.values(), key=lambda item: item.tenant_id)
            ]

    @property
    def served(self) -> int:
        with self._lock:
            return self._served_total

    def _select(self) -> str | None:
        ready = [tenant for tenant, flow in self._flows.items() if flow.backlog > 0]
        if not ready:
            return None
        counts = Counter(self._recent) if len(self._recent) >= self._window else None
        return self._best(ready, lambda tenant: self._flows[tenant].virtual_time, counts)

    def _select_from(
        self,
        ready: list[str],
        virtual: dict[str, float],
        weights: dict[str, float],
        recent: deque[str],
    ) -> str:
        counts = Counter(recent) if len(recent) >= self._window else None
        return self._best(ready, lambda tenant: virtual.get(tenant, 0.0), counts)

    def _best(
        self,
        ready: list[str],
        virtual_time,
        counts: Counter | None,
    ) -> str:
        """Lowest virtual time wins; the share ceiling demotes a hogging tenant."""
        eligible = ready
        if counts is not None and self._max_share < 1.0:
            window = len(self._recent)
            capped = [tenant for tenant in ready if (counts.get(tenant, 0) / window) < self._max_share]
            if capped:
                eligible = capped
        return min(eligible, key=lambda tenant: (virtual_time(tenant), -self._weight_of(tenant), tenant))

    def _weight_of(self, tenant: str) -> float:
        flow = self._flows.get(tenant)
        return flow.weight if flow else 1.0

    def _renormalize(self) -> None:
        floor = min((flow.virtual_time for flow in self._flows.values()), default=0.0)
        for flow in self._flows.values():
            flow.virtual_time -= floor

    @staticmethod
    def _weight_for(weight: float | None) -> float:
        value = 1.0 if weight is None else float(weight)
        if value <= 0:
            raise ValueError("weight must be > 0")
        return value


def plan_tenant_claims(
    queue_depths: dict[str, int],
    *,
    weights: dict[str, float] | None = None,
    limit: int = 32,
    max_share: float = 1.0,
) -> dict[str, int]:
    """Fair per-tenant claim budget for one worker poll (no tenant takes the whole batch)."""
    scheduler: FairQueue[str] = FairQueue(default_weight=1.0, max_share=max_share)
    for tenant, depth in queue_depths.items():
        for index in range(min(int(depth), limit)):
            scheduler.offer(tenant, f"{tenant}:{index}", weight=(weights or {}).get(tenant))
    counts: dict[str, int] = {}
    for _ in range(max(0, int(limit))):
        picked = scheduler.pick()
        if picked is None:
            break
        counts[picked[0]] = counts.get(picked[0], 0) + 1
    return counts
