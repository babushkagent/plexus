# 0001 — Postgres is the only source of truth

- Status: Accepted
- Date: 2026-10-04
- Scope: `src/plexus/store/`, every table in `store/migrations/postgres/`

## Context

An MLOps control plane naturally wants several stores: a relational one for tenants,
models and audit; a blob store for artifacts; Redis for rate-limit counters and caches;
a queue broker for async work; sometimes a graph or document store for lineage metadata.
Each is individually justified. Together they are a distributed system with no
transaction spanning them, and the interesting bugs in this problem domain -- a model
promoted but not billed, a task enqueued for a row that was rolled back, an audit record
describing a state that never committed -- all live in the gaps between them.

The platform also has to be self-hostable by a small team and runnable with `docker
compose up` for evaluation. Every additional stateful component multiplies the backup
story, the upgrade matrix, the monitoring surface and the number of ways a demo fails.

## Decision

One Postgres database holds tenants, API keys, model registry rows, artifacts metadata,
runs, budgets, audit events, idempotency claims, outbox events and the task queue. The
only other required component is an object store for artifact *bytes*, and the database
holds the authoritative pointer plus digest for each of them.

Deliberate consequences of this:

- A handler runs business write, audit row and task enqueue inside one transaction, so
  they are simultaneously visible or simultaneously absent (`store/uow.py`).
- Rate-limit counters and policy caches live in process memory with a short TTL, because
  making them correct across replicas would require either Postgres round-trips on the
  hot path or the Redis we are not operating. They are approximate by design; the
  durable budget check is the ledger, which is in the database (see `llm/router.py`).
- SQLite implements the same schema and semantics for tests and single-node use
  (`store/migrations/sqlite/`), which is why the whole suite runs without a server.

## Consequences

**Good.** One backup, one point-in-time recovery, one consistency story. Rollback is a
real capability rather than a compensation script. The test suite exercises real
transactions. A new deployment is one connection string.

**Costs and risks.**

- Write throughput is bounded by one primary; horizontal read scaling comes from
  replicas, not from sharding, so very large single-tenant installations eventually need
  partitioning by `tenant_id` (the schema keeps `tenant_id` as the leading column of
  every composite key specifically to keep that option cheap).
- The queue competes for the same I/O as OLTP reads. Mitigated by keeping the claim query
  on a narrow index and by `FOR UPDATE SKIP LOCKED`, but a 10M-row backlog does show up
  in API latency; retention/pruning of finished tasks is an operational requirement, not
  an optional cleanup.
- SQLite is a compatibility target, not a production store: `config.py` refuses to start
  `staging`/`prod` with a SQLite URL, and the Postgres-only security properties in ADR
  0002 do not exist there at all.
- In-process counters mean two API replicas each grant their own burst. The plan limit is
  therefore a soft ceiling per replica; enforcing an exact global rate would require
  either Redis or a database write per request.
