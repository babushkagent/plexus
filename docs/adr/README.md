# Architecture decision records

These record the decisions that are expensive to reverse, and -- as importantly -- the
options that were rejected. Each one is written against code that exists in this
repository, so a reader can check the reasoning rather than take it on faith.

Read them in order if you intend to change the storage model, the tenancy boundary, or
the process topology: those three are load-bearing for everything else.

| # | Decision | Drivers | Status |
|---|----------|---------|--------|
| [0001](0001-postgres-as-the-only-source-of-truth.md) | One Postgres database is the only source of truth | Operational surface, transactionality | Accepted |
| [0002](0002-row-level-security-two-database-roles.md) | Tenant isolation enforced twice: app predicates plus RLS, with two database roles | Blast radius, defence in depth | Accepted |
| [0003](0003-leased-postgres-queue-over-a-message-broker.md) | Task queue is a leased table, not Kafka/SQS/Redis | Ordering, exactly-once-ish, ops cost | Accepted |
| [0004](0004-consistent-hash-ring-for-affinity-and-sharding.md) | Consistent hash ring for provider affinity and sharding | Scale invariance, cache locality | Accepted |
| [0005](0005-promotion-gate-in-the-registry.md) | Release gate lives in the registry write path | Unbypassable safety, auditability | Accepted |
| [0006](0006-separate-api-and-worker-processes.md) | API and worker are separate deployables sharing one store | Independent scaling, failure isolation | Accepted |

## What makes an ADR here worth writing

A decision qualifies when it would take a quarter of engineering time to undo, or when a
plausible-sounding alternative was rejected for a reason nobody would remember in six
months. "We use Postgres" is not interesting; "we use Postgres for the queue *and* the
control plane, and here is the specific failure that buys us" is.

Consequences sections are deliberately honest. An ADR that lists only benefits is
marketing, and the next engineer will either believe it (bad) or distrust all of them
(worse).
