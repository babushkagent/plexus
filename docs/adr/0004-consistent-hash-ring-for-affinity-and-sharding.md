# 0004 — One consistent hash ring decides affinity and shard ownership

- Status: Accepted
- Date: 2026-10-04
- Scope: `src/plexus/scaling/hashring.py`, `llm/router.py`

## Context

Two separate problems in this platform are secretly the same problem:

1. Inference routing must be *sticky* -- sending a tenant's traffic for one model to the
   same provider keeps prompt caches warm, keeps spend attributable, and makes an incident
   reproducible. But it must also degrade gracefully when that provider is unhealthy.
2. Background work must be partitioned so that replicas do not duplicate effort, and
   adding a replica must not reshuffle the world.

`hash(key) % N` fails both: changing `N` rebinds almost every key. For an inference gateway
that means a cold cache storm on every scale-out event, which is precisely when the system
is already under pressure.

## Decision

A single hash-ring implementation (`HashRing`, blake2b over virtual nodes) is the only
placement primitive. Keys are `(tenant_id, model)` for provider affinity and shard
identity for work ownership; each member gets enough virtual nodes that the load skew is
small without needing per-member weights in the common case.

The router asks the ring for an **ordered candidate list** rather than a single winner:
the ring decides preference, circuit breakers decide availability. A provider whose
breaker is open moves to the back of the order instead of disappearing, so a full outage
still produces a deterministic attempt sequence and every failure is recorded on the
request (`attempts` in the response envelope).

## Consequences

**Good.** Scaling out moves ~1/N of keys instead of nearly all of them, so cache warmth
and behaviour survive normal operations. Routing decisions are reproducible from the ring
configuration alone -- an incident can be replayed without the original pod. The same code
path is reused for sharding, so there is one implementation to trust and test.

**Costs and gotchas.**

- Ring membership must be identical across replicas or two replicas will disagree about
  affinity; configuration drift between API pods is therefore a correctness bug, not a
  cosmetic one. The ring is rendered from `Settings` and surfaced at `/v1/platform/health`
  so it can be diffed.
- Consistent hashing balances *keys*, not *load*: one tenant generating 90% of traffic on
  one model still lands on one provider. Admission control and fair scheduling (ADR 0003)
  are what protect against that, not the ring.
- Virtual nodes cost memory and a binary search per decision -- negligible here, but the
  ring is not the place to put per-request hot-path work like health checks; those stay in
  the breaker.
