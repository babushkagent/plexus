# 0006 — API and worker are separate deployables that share one database

- Status: Accepted
- Date: 2026-10-04
- Scope: `serve` and `worker` CLI verbs, `deploy/k8s/base/`, HTTP pipeline order

## Context

Serving a chat completion and running a fine-tune have almost nothing in common. One is
p99-sensitive, holds connections open for SSE streams, and wants to die fast when it is
overloaded. The other is throughput-oriented, CPU- or GPU-bound for minutes, and wants to
be left alone. A single process that does both inherits the worst properties of each:

- Batch work starves inference of CPU exactly when latency matters, and Python's GIL makes
  that contention structural rather than a scheduling nuisance.
- One autoscaler has to scale on two contradictory signals. Scaling on request rate leaves
  a training backlog unattended; scaling on queue depth spawns HTTP replicas nobody is
  calling.
- A rolling deploy either interrupts hours-long tasks or blocks rollout indefinitely.

## Decision

One image, one binary, two deployables. `plexus serve` and `plexus worker` are separate
processes that communicate exclusively through Postgres (ADR 0001) -- there is no queue
between them, no shared memory, and neither can reach the other directly.

**Each scales on its own signal.** The API Deployment uses an HPA on CPU utilization
(65%, 2--40 replicas, fast scale-up and slow scale-down) -- for a request-serving process
CPU saturates before anything else does, so it is the honest proxy for request rate.
Workers use a KEDA `ScaledObject` on `plexus_queue_depth` (ADR 0003); clusters with
prometheus-adapter but no KEDA take `render_external_metric()` instead, and CPU-only
scaling of workers is explicitly wrong because backlog and CPU are unrelated until the
fleet is already saturated. All three manifests render from
`src/plexus/scaling/autoscaler.py`, so scaling policy is code-reviewed in one place
instead of drifting across YAML files.

**Shutdown budgets differ, because what they are draining differs.** API pods get
`terminationGracePeriodSeconds: 45` (drain in-flight requests, close streams;
`PLEXUS_SHUTDOWN_GRACE_S` bounds how long the accept thread is joined). Workers get `300`.
`TaskWorker.run_forever()` drains in a deliberate order on SIGTERM: stop claiming → let
submitted handlers finish → hand back anything still marked in-flight (`fail(retry=True)`)
so a peer can claim it now rather than after a lease timeout. What actually bounds the
drain is `terminationGracePeriodSeconds`; a pod that outruns it gets SIGKILL and falls
back to lease expiry, which is why the two numbers must be reasoned about together.

**Workers deliberately have no HTTP probes.** Liveness for a worker is a database fact: if
it stops heartbeating, its leases expire and the tasks become claimable again (ADR 0003).
An HTTP endpoint proves the process accepts connections, which is not the property we care
about, and it would add a port and an attack surface to the component with the most
database privilege. Instead the same signal drives real recovery: stale leases are
reclaimed by peers, and backlog depth -- not pod readiness -- is what scales the fleet.

**Cross-cutting invariants live in one pipeline.** `src/plexus/api/server.py` runs, in
order: route match → authenticate → tenant policy lookup (5s cache) → tenant-active check →
token-bucket rate limit → concurrency gate → bind `TenantContext` → authorize capability →
idempotency claim → handler. Handlers receive an already-scoped, already-authorized call
and cannot accidentally forget a step; adding a control means editing one function.

## Consequences

**Good.** A backlog spike buys worker replicas and leaves inference latency untouched.
Batch work cannot take the control plane down, and a bad dependency in a task handler
cannot hold an HTTP connection open. Both processes are stateless, so both can be replaced
by a Deployment rollout at any time, and local development runs either one against the same
SQLite file.

**Costs and limits.**

- Postgres is on the critical path of both deployables and is a single point of failure by
  construction. That is accepted: it is one well-understood component with PITR, rather
  than two stores that can disagree.
- Workers hold DB connections while executing tasks. A large worker fleet needs a
  transaction pooler (PgBouncer in transaction mode) or the connection budget becomes the
  scaling limit before CPU does.
- Two deployables mean two rollouts that must stay schema-compatible. Migrations are
  therefore expand/contract and run as a Job (`plexus migrate`) between them; a worker
  from last week must tolerate today's columns.
- No worker HTTP surface means no `/readyz` gate, so Kubernetes will happily route scale
  events to a worker that cannot reach the database -- it fails tasks into retries instead
  of failing to start, which is loud in metrics but quiet in `kubectl get pods`.
- Drain is best-effort. A task that outruns `terminationGracePeriodSeconds` is SIGKILLed
  and re-run elsewhere once its lease expires, so every handler must be idempotent; the
  dedupe key and the `attempts` counter make this safe rather than merely likely. Graceful
  shutdown waits for submitted handlers with no deadline of its own, so a hung handler
  delays rollout until Kubernetes escalates -- a watchdog inside the handler is the
  operator's responsibility.
