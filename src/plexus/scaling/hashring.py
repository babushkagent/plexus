"""Consistent hashing: the primitive that makes the platform scale-invariant.

Naive `hash(key) % N` rebinding reshuffles almost every key when a replica joins
or leaves, which for an MLOps control plane means cache storms, re-sharded model
copies, and routing churn. A hash ring moves ~1/N of the keyspace instead, so
scaling out is a non-event for correctness and cheap for caches.

The same ring drives provider affinity (sticky tenant->endpoint binding with an
ordered fallback list) and deployment shard assignment.
"""

from __future__ import annotations

import bisect
import hashlib
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field

HashFn = Callable[[str], int]


def blake2b_hash(point: str) -> int:
    return int.from_bytes(hashlib.blake2b(point.encode(), digest_size=8).digest(), "big")


def fnv1a_hash(point: str) -> int:
    value = 0xCBBF4A4442BAA657
    for byte in point.encode():
        value = ((value ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return value


@dataclass(frozen=True, slots=True)
class RingPoint:
    position: int
    node: str

    @property
    def label(self) -> str:
        return f"{self.node}@{self.position:x}"


def _expand(node: str, weight: float, vnodes: int) -> list[int]:
    count = max(1, round(vnodes * max(0.01, weight)))
    return [blake2b_hash(f"{node}#{index}") for index in range(count)]


class HashRing:
    """Deterministic rendezvous of keys to nodes with bounded rebalancing."""

    def __init__(self, *, vnodes: int = 128, hash_fn: HashFn = blake2b_hash) -> None:
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self._vnodes = vnodes
        self._hash = hash_fn
        self._weights: dict[str, float] = {}
        self._points: list[RingPoint] = []
        self._positions: list[int] = []

    @classmethod
    def from_members(cls, members: Iterable[str] | Mapping[str, float], *, vnodes: int = 128) -> HashRing:
        ring = cls(vnodes=vnodes)
        ring.set_members(members)
        return ring

    @property
    def members(self) -> tuple[str, ...]:
        return tuple(sorted(self._weights))

    @property
    def points(self) -> int:
        return len(self._points)

    def __len__(self) -> int:
        return len(self._weights)

    def __contains__(self, node: object) -> bool:
        return node in self._weights

    def add(self, node: str, *, weight: float = 1.0) -> None:
        if not node:
            raise ValueError("node name must be non-empty")
        if weight <= 0:
            raise ValueError("weight must be > 0")
        self._weights[node] = weight
        self._rebuild()

    def remove(self, node: str) -> bool:
        existed = self._weights.pop(node, None) is not None
        if existed:
            self._rebuild()
        return existed

    def set_members(self, members: Iterable[str] | Mapping[str, float]) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Converge to `members`, returning (added, removed)."""
        desired: dict[str, float] = dict(members) if isinstance(members, Mapping) else dict.fromkeys(members, 1.0)
        added = tuple(sorted(set(desired) - set(self._weights)))
        removed = tuple(sorted(set(self._weights) - set(desired)))
        if added or removed:
            self._weights = desired
            self._rebuild()
        return added, removed

    def node_for(self, key: str) -> str | None:
        """Owner of `key`, stable for as long as the membership is."""
        if not self._positions:
            return None
        index = bisect.bisect_right(self._positions, self._hash(key)) % len(self._points)
        return self._points[index].node

    def nodes_for(self, key: str, count: int | None = None) -> tuple[str, ...]:
        """Distinct owners clockwise from the primary: an ordered preference list."""
        if not self._positions:
            return ()
        wanted = len(self._weights) if count is None else max(1, min(count, len(self._weights)))
        start = bisect.bisect_right(self._positions, self._hash(key)) % len(self._points)
        ordered: list[str] = []
        seen: set[str] = set()
        for offset in range(len(self._points)):
            node = self._points[(start + offset) % len(self._points)].node
            if node not in seen:
                seen.add(node)
                ordered.append(node)
                if len(ordered) >= wanted:
                    break
        return tuple(ordered)

    def shard_for(self, key: str, shards: int) -> int:
        """Stable shard index; independent of node membership (used by deployments)."""
        if shards < 1:
            raise ValueError("shards must be >= 1")
        return self._hash(key) % shards

    def positions(self) -> list[tuple[int, str]]:
        return [(point.position, point.node) for point in self._points]

    def _rebuild(self) -> None:
        points = [
            RingPoint(position=position, node=node)
            for node, weight in sorted(self._weights.items())
            for position in sorted(_expand(node, weight, self._vnodes))
        ]
        points.sort(key=lambda point: point.position)
        self._points = points
        self._positions = [point.position for point in points]


def keys_moved(before: HashRing, after: HashRing, keys: Iterable[str]) -> int:
    """Keys whose owner differs between two rings; the scale-invariance assertion."""
    return sum(1 for key in keys if before.node_for(key) != after.node_for(key))


def distribution(ring: HashRing, keys: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for key in keys:
        node = ring.node_for(key)
        if node is not None:
            counts[node] = counts.get(node, 0) + 1
    return counts


class Rendezvous:
    """Highest-random-weight hashing: no ring, minimal movement, best for small pools."""

    def __init__(self, *, hash_fn: HashFn = blake2b_hash) -> None:
        self._hash = hash_fn
        self._nodes: set[str] = set()

    def add(self, node: str) -> None:
        self._nodes.add(node)

    def remove(self, node: str) -> bool:
        existed = node in self._nodes
        self._nodes.discard(node)
        return existed

    @property
    def members(self) -> tuple[str, ...]:
        return tuple(sorted(self._nodes))

    def nodes_for(self, key: str, count: int | None = None) -> tuple[str, ...]:
        scored = sorted(self._nodes, key=lambda node: self._hash(f"{key}\x1f{node}"), reverse=True)
        return tuple(scored if count is None else scored[: max(1, count)])

    def node_for(self, key: str) -> str | None:
        ordered = self.nodes_for(key, 1)
        return ordered[0] if ordered else None


def iter_owned(ring: HashRing, keys: Iterable[str], node: str) -> Iterator[str]:
    for key in keys:
        if ring.node_for(key) == node:
            yield key


@dataclass(frozen=True, slots=True)
class Placement:
    key: str
    primary: str | None
    replicas: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, object]:
        return {"key": self.key, "primary": self.primary, "replicas": list(self.replicas)}


def place(ring: HashRing, key: str, *, replication: int = 1) -> Placement:
    owners = ring.nodes_for(key, replication)
    return Placement(key=key, primary=owners[0] if owners else None, replicas=owners)
