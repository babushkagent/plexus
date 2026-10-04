# 0002 — Tenant isolation is enforced twice, by two database roles

- Status: Accepted
- Date: 2026-10-04
- Scope: `store/migrations/postgres/0002_rls.sql`, `0003_worker_scope.sql`,
  `store/db.py`, `deploy/docker/init/01-roles.sh`

## Context

Tenant isolation in a multi-tenant control plane is usually implemented as a `WHERE
tenant_id = ?` clause. That is correct right up until the one query someone writes at
17:50 on a Friday without the clause -- and in this codebase roughly 40 handlers, an
in-process worker, CLI verbs and reporting queries would all have to remember it forever.

A single database role makes that worse: if the role can read every row, any SQL
injection or any missing predicate is a cross-tenant incident with no second line of
defence. Postgres skips row-level security for superusers *and* for table owners, which
is the detail most write-ups of "just use RLS" get wrong.

## Decision

Isolation is enforced at two independent layers.

1. **Application layer.** Every repository method takes an explicit tenant scope and the
   unit of work binds `app.tenant_id` (`store/uow.py`). Cross-tenant reads return 404,
   not 403, so a missing row and someone else's row are indistinguishable to the caller.
2. **Database layer.** Every tenant-owned table has `ROW LEVEL SECURITY` enabled plus
   `FORCE ROW LEVEL SECURITY`, with policies keyed on
   `current_setting('app.tenant_id', true)`. A forgotten predicate then returns zero rows
   instead of another tenant's data.

Two roles, because those policies only bind connections that are neither superuser nor
table owner:

- **Owner role** -- used exclusively by `plexus migrate` (and the Compose `migrate`
  service). Creates tables and policies, then its work is done. No long-running process
  connects with it.
- **`plexus_app`** -- non-superuser, non-owner, created by
  `deploy/docker/init/01-roles.sh`. Every API replica and worker connects with this role,
  so every query is subject to the policies.

Workers own no tenant but legitimately operate across tenants, so queue tables get a
second gate: `TaskQueue` issues `SET LOCAL app.worker_scope = 'true'` inside its own
transactions (`workflow/engine.py`). No HTTP handler can set that variable, and the role
is explicitly **not** granted `BYPASSRLS` -- that privilege is all-or-nothing and leaks
into request paths the first time someone needs a quick fix.

## Consequences

**Good.** A missing predicate is a availability-neutral empty result rather than a data
leak. The credential that leaks in an incident (the app role) is not the credential that
can alter schema. Auditors can verify isolation from the catalog instead of from a code
review of 40 handlers.

**Costs and gotchas, all paid somewhere:**

- Migrations and runtime need different connection strings; a deployment that reuses one
  role silently has no RLS at all. `deploy/k8s/base/kustomization.yaml` documents the two
  secrets, and this is the single most important thing to check in a review.
- Operational tooling that connects as owner (or a superuser) sees everything, so ad-hoc
  `psql` debugging does not reproduce what the app can read. Verify isolation queries
  while connected as `plexus_app`.
- `FORCE ROW LEVEL SECURITY` means even the owner cannot see queue rows without setting
  the scope GUC -- which is why KEDA cannot COUNT the queue directly and the worker
  autoscaler consumes an exported gauge instead (`deploy/k8s/base/scaledobject.yaml`).
- Every new table needs its policy in the same migration that creates it; a table added
  without one is unprotected by default. `tests/` plus the Postgres CI job are the guard.
- SQLite has no RLS, so the isolation tests there only prove the application layer works.
