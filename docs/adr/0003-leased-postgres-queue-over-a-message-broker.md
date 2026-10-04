# 0003 — The task queue is a leased Postgres table, not a broker

- Status: Accepted
- Date: 2026-10-04
- Scope: `src/plexus/workflow/engine.py`, `tasks` and `outbox_events` tables

## Context

Training, batch embedding, evaluation and bulk export are minutes-long jobs that must
survive a pod being deleted mid-flight. The reflex is Kafka, SQS or Celery+Redis. But the
properties this workload actually needs are: exactly-once *enqueue* (a retried HTTP
request must not create two trainings), visibility timeout with predictable reclaim,
delayed execution, per-tenant fairness, and above all atomicity with the database row
that created the work.

A broker provides at-least-once delivery against a store we control, which means the
dedupe table, the reconciliation job and the "consumer committed but the offset did not
make it" handling still have to be written -- in a second system, with a second failure
mode, outside the transaction that created the task.

## Decision

Tasks are rows. `TaskQueue` (workflow/engine.py) implements:

- **Atomic claim** -- one statement on Postgres using `FOR UPDATE SKIP LOCKED`, so N
  workers never wait on each other and a slow worker cannot block the queue head; SQLite
  gets an optimistic per-row compare-and-set under an immediate transaction.
- **Lease, not delete** -- claiming sets `status = running`, `lease_owner`,
  `lease_expires_at_ms` and increments `attempts`. A heartbeat extends it. A dead worker's
  tasks become claimable again when the lease expires, which is why no HTTP probe is
  needed on workers to detect them (see ADR 0006).
- **Idempotent enqueue** -- `ON CONFLICT (tenant_id, dedupe_key)` returns nothing instead
  of a second task, and `enqueue(tx=...)` can join the caller's transaction so work
  becomes visible exactly when the state that produced it does.
- **Delayed work** -- `run_after_ms` is both retry backoff and scheduled execution.
- **Terminal states** -- attempts exhausted moves a task to dead-letter with its error,
  rather than dropping it.

Cross-component notifications use the transactional **outbox**: `UnitOfWork` inserts
events in the same transaction as the state change and publishes after commit
(`store/uow.py`). Consumers that need a real stream can subscribe to the outbox; the
platform does not require one.

## Consequences

**Good.** Enqueue, audit row and budget write commit together or not at all -- the class
of bug where billing and work disagree is structurally impossible. Crash recovery is a
`SELECT`, and backlog depth is queryable with the same tool used to debug anything else.
One fewer always-on component to operate, secure and upgrade.

**Costs and limits.**

- Throughput ceiling is Postgres insert/claim rate: comfortable for tens of thousands of
  tasks per minute, wrong for a genuine event stream at millions per second. If that
  arrives, the outbox is the seam -- publish it to a broker rather than replacing the
  queue.
- Polling costs something even when idle; workers back off between empty claims, but a
  large fleet of mostly-idle workers is wasted connections. Scale the fleet on backlog,
  which is exactly what `deploy/k8s/base/scaledobject.yaml` does.
- Long-running tasks hold a row in `running`; finished-task retention must be an actual
  operational job or the claim query degrades.
- Fairness across tenants is enforced by the in-process scheduler (`scaling/fairness.py`,
  weighted virtual time with a share ceiling), so it is per-worker rather than globally
  optimal -- one busy tenant can occupy every worker's current batch boundary, just not
  indefinitely.
