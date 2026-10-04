# Plexus

**Multi-tenant, scale-invariant, fault-tolerant MLOps / LLMOps control plane and inference gateway.**

One stateless API process that serves inference and owns the control plane, one
durable worker process that owns long-running work, and one database that is the
single source of truth. No cloud SDKs, no queue broker, no workflow engine, and
**zero runtime Python dependencies** -- it runs on a laptop, in Docker Compose, or
on any conformant Kubernetes, unchanged.

- **Language / runtime:** Python 3.13, stdlib-only core (`uvloop`, `psycopg` and OTLP are optional extras)
- **Store:** PostgreSQL 15+ in production, SQLite for local dev and tests
- **Compatibility:** OpenAI-compatible `/v1/chat/completions`, including SSE streaming

---

## Architecture

Two independently scaling processes share one database. The split is deliberate:
the API is latency-sensitive and scales with request rate, the worker is
throughput-sensitive and scales with queue depth. Sharing a process would let a
batch job starve inference of CPU and make both autoscalers lie.

```mermaid
flowchart TB
    subgraph CALLERS["Callers"]
        SDK["OpenAI-compatible SDKs"]
        CLI["plexus CLI"]
        CICD["CI/CD pipelines"]
    end

    subgraph EDGE["Stateless edge - scale on RPS and latency"]
        ING["Ingress / load balancer<br/>healthz + readyz"]
        API["plexus serve<br/>HTTP pipeline + composition root"]
        GW["Inference router<br/>hash ring, circuit breakers, budget"]
    end

    subgraph WORKERS["Stateless workers - scale on queue depth"]
        W1["plexus worker<br/>lease queue, retries, DLQ, sagas"]
    end

    subgraph UPSTREAMS["Model providers"]
        OA["OpenAI-compatible endpoint"]
        OL["Ollama / local GPU"]
        EC["echo - deterministic test provider"]
    end

    subgraph STATE["Durable state - the only place truth lives"]
        PG[("PostgreSQL<br/>row-level security per tenant")]
        OB["Transactional outbox<br/>same commit as state"]
        SQ[("tasks table<br/>leases + fair scheduling")]
    end

    subgraph TELEMETRY["Observability"]
        LOG["Structured JSON logs"]
        TRC["W3C trace context"]
        MET["Prometheus metrics<br/>GET /metrics"]
    end

    subgraph CTRL["Platform control"]
        HPA["HPA - CPU for the API"]
        KEDA["KEDA - queue depth for workers"]
    end

    SDK --> ING --> API
    CLI --> API
    CICD --> API

    API -->|"admit, scope, authorize"| GW
    GW -->|"candidate order from consistent hash;<br/>fallback when a breaker opens"| UPSTREAMS
    GW -->|"usage + spend, never bypassable"| PG

    API -->|"state + outbox event: one commit"| PG
    API -->|"enqueue async work"| SQ
    PG --- OB

    W1 -->|"claim by lease, fair per tenant"| SQ
    W1 -->|"heartbeat, retry with jitter, dead-letter"| PG
    W1 -->|"relay published events"| OB

    API -.-> TELEMETRY
    W1 -.-> TELEMETRY
    HPA -.->|"desired replicas"| EDGE
    KEDA -.->|"desired replicas"| WORKERS
```

### How a request flows

Every cross-cutting invariant is enforced by **one pipeline** in
`src/plexus/api/server.py`, so handlers stay pure business logic and cannot
silently skip a security control.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant P as HTTP edge
    participant R as Inference router
    participant D as Postgres
    participant U as Provider

    C->>P: POST /v1/chat/completions + x-api-key
    P->>P: route match, else 404 or 405
    P->>D: authenticate credential, load tenant policy
    P->>P: tenant active? rate limit? concurrency gate?
    P->>P: bind TenantContext for this task
    P->>P: authorize capability inference:call
    P->>D: idempotency claim - replay or 409
    P->>R: route(tenant, request)
    R->>D: assert monthly budget before spend
    R->>U: call hashed primary candidate
    alt provider unhealthy or rate limited
        R->>U: try next candidate, same request
    end
    R->>D: record usage and charge spend
    R-->>P: completion, attempts, cost
    P->>D: audit inference.completion - no prompt text
    P-->>C: 200 OpenAI envelope + X-Request-Id + Traceparent
```

### What each component owns

| Component | Path | Owns |
| --- | --- | --- |
| HTTP edge | `src/plexus/api/server.py` | Identity, tenant scoping, admission, RBAC, idempotency, error mapping, composition root |
| Handlers | `src/plexus/api/handlers.py` | ~40 routes as plain functions; the routing table declares each required capability |
| Inference router | `src/plexus/llm/router.py` | The only place that decides which upstream answers; budget and usage accounting |
| Providers | `src/plexus/llm/provider.py` | Transport to OpenAI-compatible, Ollama and echo backends behind one protocol |
| Task engine | `src/plexus/workflow/engine.py` | Leased queue, retries, dead letters, sagas; the queue lives in the same DB as the rows |
| Resilience | `src/plexus/workflow/policy.py` | Retry policies with jitter and per-dependency circuit breakers |
| Scaling | `src/plexus/scaling/` | Consistent hash ring, weighted fair scheduling, token buckets, autoscaler decisions |
| Registry | `src/plexus/registry/store.py` | Model versions, artifacts, deployments, runs, and the promotion gate |
| Store | `src/plexus/store/` | Dialect abstraction, checksummed migrations, unit of work, outbox, repositories |
| Tenancy | `src/plexus/tenancy/` | Tenant context, JWT/API-key auth, capability RBAC, plan limits |
| Telemetry | `src/plexus/telemetry.py` | Logs, trace context and metrics sharing one correlation id |

---

## The four load-bearing invariants

### 1. Tenant isolation is enforced twice, on purpose

`TenantContext` is bound per request and the repositories refuse to operate
without it, so forgetting a `WHERE tenant_id = ?` is an error rather than a leak.
On top of that, Postgres enforces it server side:

```sql
-- src/plexus/store/migrations/postgres/0002_rls.sql
ALTER TABLE model_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_versions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_model_versions ON model_versions
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
```

The app connects as a **non-superuser, non-table-owner** role -- otherwise
Postgres silently skips RLS. Cross-tenant reads return `404`, not `403`, so a
foreign id leaks nothing about whether the row exists.

### 2. Scale-invariance: adding capacity must not change behaviour

Naive `hash(key) % N` rebinding reshuffles nearly every key when a node joins or
leaves -- cache storms, re-sharded model copies, routing churn. A consistent hash
ring with virtual nodes moves about `1/N` of the keyspace instead. The same ring
drives provider affinity and deployment shard assignment
(`src/plexus/scaling/hashring.py`).

- Candidate order is derived from a hash of `(tenant, model)`, so a tenant keeps
  provider affinity (cache and keep-alive reuse) while tenants spread across the pool.
- Admission control is O(1) per request regardless of tenant count: plan limits are
  merged once and cached for 5s rather than queried per call.
- `FairQueue` serves flows in virtual-time order with a share ceiling, so one tenant
  enqueueing 100k embedding jobs cannot starve anyone else's inference batch.
- IDs are ULIDs: time-sortable, so hot writes never btree-bounce and range scans
  stay local to a shard.

### 3. Fault tolerance: fail well, not loudly

| Failure | Response |
| --- | --- |
| Provider returns 5xx / rate limit | Next hashed candidate is tried **in the same request**; every attempt is recorded and returned as `attempts[]` |
| Provider stays unhealthy | Per-provider circuit breaker opens; the router stops routing to it and fails over instead of queueing a stampede |
| Worker dies mid-task | Lease expires, another worker re-claims; handlers are idempotent, so at-least-once plus dedupe keys is the contract -- exactly-once is a myth |
| Dependency recovers | Breaker goes half-open and probes with a bounded number of requests before closing |
| Poison message | Exhausts `max_attempts` and moves to a dead letter, queryable at `GET /v1/dead-letters` and requeueable |
| Process killed | SIGTERM drains in-flight work within the shutdown grace; a second signal exits |

Retries always carry jitter: synchronized retries from N replicas are how a
degraded dependency stays down.

### 4. No write can be processed without being saved, or vice versa

The task queue and the outbox live in **the same database as the business rows**.
Enqueueing work, emitting an event and mutating state share one commit
(`src/plexus/store/uow.py`), so "we saved it but never processed it" is not a
reachable state. Domain events are published after commit, at-least-once, and can
never describe a rolled-back write.

Writes that must survive their own rollback -- audit entries and idempotency
records -- commit in a separate immediate transaction on purpose.

---

## Model lifecycle: the gate lives in the domain, not in CI

`src/plexus/registry/store.py` rejects promotion unless the version has both
passed an eval and been signed, so no code path can forget it.

```text
registered --> evaluated --> signed --> staging --> production
                                 (any stage can be rolled back to)
```

- Versions are **content-addressed by digest**, so a retried registration is
  idempotent instead of a second row that confuses a rollout.
- Traffic shifting (`/v1/deployments/{id}/traffic`) and rollback are explicit,
  audited operations.
- Every state change writes an outbox event in the same transaction.

---

## Quick start

Requires Python 3.13. No other dependency is needed to run the core.

```bash
git clone <your-fork> plexus && cd plexus

uv venv .venv && uv pip install -e ".[dev]"   # or: python -m venv .venv && pip install -e ".[dev]"

export PLEXUS_DATABASE_URL="sqlite:///./plexus.sqlite3"
export PLEXUS_ENV=dev

.venv/bin/plexus config-check                 # redacted effective config + store probe
.venv/bin/plexus seed --name acme-demo        # prints an owner API key exactly once
.venv/bin/plexus serve --migrate              # http://127.0.0.1:8080
```

In a second terminal, start the worker:

```bash
.venv/bin/plexus worker --concurrency 4
```

Then call it with an OpenAI-compatible client:

```bash
curl -sS localhost:8080/v1/chat/completions \
  -H "x-api-key: $PLEXUS_API_KEY" \
  -H "content-type: application/json" \
  -d '{"model":"echo","messages":[{"role":"user","content":"hello plexus"}]}'
```

The `echo` provider is always registered, so a fresh deployment can be smoke
tested with no credentials. Add real upstreams with `OPENAI_API_KEY` and/or
`OLLAMA_BASE_URL`.

### CLI

| Command | Purpose |
| --- | --- |
| `plexus serve` | Run the control plane and inference gateway; `--migrate` applies schema first |
| `plexus worker` | Run the task worker; `--types`, `--concurrency`, `--once` for drain-and-exit |
| `plexus migrate` | Apply pending migrations; `--force` re-applies |
| `plexus config-check` | Print redacted config and probe the store; exits non-zero if unreachable |
| `plexus seed` | Create a demo tenant and print one owner API key |
| `plexus token` | Mint a JWT for a tenant, with `--roles`, `--plan` and `--platform` |
| `plexus version` | Name, version and interpreter |

---

## Configuration

Everything comes from the environment (12-factor), optionally seeded from `.env`.
Configuration is validated **eagerly**: a misconfigured pod dies at startup rather
than serving traffic with a silent default. `plexus config-check` prints every
problem at once.

| Variable | Default | Notes |
| --- | --- | --- |
| `PLEXUS_ENV` | `dev` | `dev`, `test`, `staging`, `prod`; the last two enable production checks |
| `PLEXUS_DATABASE_URL` | `sqlite:///var/lib/plexus/plexus.sqlite3` | `postgresql://...` required outside dev/test |
| `PLEXUS_JWT_SECRET` | insecure dev value | Must be >= 32 chars in staging/prod |
| `PLEXUS_API_KEY_PEPPER` | empty | Required in staging/prod; keys are stored as HMAC digests only |
| `PLEXUS_PORT` | `8080` | HTTP bind port |
| `PLEXUS_DB_POOL_SIZE` | `8` | Connection pool size |
| `PLEXUS_WORKER_CONCURRENCY` | `4` | Handler threads per worker process |
| `PLEXUS_TASK_LEASE_S` / `PLEXUS_TASK_HEARTBEAT_S` | `60` / `15` | Heartbeat must be well below the lease |
| `PLEXUS_TARGET_QUEUE_DEPTH` | `64` | Autoscaler target for workers |
| `PLEXUS_API_MIN_REPLICAS` / `MAX` | `2` / `40` | Bounds used by the rendered HPA |
| `OPENAI_API_KEY`, `OLLAMA_BASE_URL` | empty, local | Configure the upstream pool |
| `PLEXUS_ALLOWED_MODELS` | `gpt-4o-mini,llama3.1:8b,echo` | Allowlist of routable model names |

Refusing to start is a feature: it converts a class of silent production outage
into a failed rollout that never receives traffic.

---

## API surface

| Area | Endpoints |
| --- | --- |
| Ops | `GET /healthz`, `GET /readyz`, `GET /metrics` (platform) |
| Tenant & identity | `GET/PATCH /v1/tenant`, `/v1/keys`, `/v1/tokens`, `/v1/members/{subject}` |
| Inference | `POST /v1/chat/completions`, `GET /v1/models` |
| Registry | `POST /v1/models`, `/v1/artifacts`, `/v1/models/{id}/lineage`, `/eval`, `/sign`, `/promote` |
| Deployments | `POST /v1/deployments`, `/scale`, `/traffic`, `/rollback` |
| Runs & tasks | `POST/GET /v1/runs`, `/v1/tasks`, `/v1/dead-letters`, `/{id}/requeue`, `/{id}/cancel` |
| Billing & audit | `GET /v1/usage`, `GET /v1/audit` |
| Platform | `/v1/platform/tenants...`, `/health`, `/scaling`, `/queue` |

Authentication is either `Authorization: Bearer <jwt>` or `x-api-key: <key>`.
Mutating requests accept an `Idempotency-Key` header; a replay returns the original
response with `X-Plexus-Replayed: true`, and a concurrent duplicate returns `409`.

Authorization is capability-based. Roles (`owner`, `admin`, `ml_engineer`,
`inference_user`, `viewer`, `auditor`) are convenient bundles; capabilities are the
contract, declared in the routing table, which keeps authorization auditable in
one screen and prevents "admin can do anything from this endpoint".

Plan defaults are enforced at admission and cannot be bypassed by background work,
because budget accounting lives in the router rather than in a handler:

| Plan | RPS | Burst | Concurrency | Monthly budget | Max replicas |
| --- | --- | --- | --- | --- | --- |
| `free` | 5 | 10 | 2 | $50 | 3 |
| `standard` | 50 | 100 | 32 | $2,500 | 20 |
| `enterprise` | 500 | 1,000 | 256 | unlimited | 200 |

---

## Deployment

Cloud-agnostic by construction: the core imports no cloud SDK and speaks only HTTP
and the Postgres wire protocol.

### Docker Compose

```bash
docker compose -f deploy/docker/compose.yaml up --build          # API + worker + Postgres
docker compose -f deploy/docker/compose.yaml --profile seed run --rm seed
docker compose -f deploy/docker/compose.yaml --profile tools run --rm tools token --tenant-id <id>
```

`seed` creates a demo tenant and prints its API key exactly once; `tools` runs any
CLI verb against the compose database without installing anything locally.

### Kubernetes

Secrets are deliberately not generated by kustomize -- a committed placeholder is
a secret that shipped. Create all three first, then apply:

```bash
kubectl create namespace plexus
kubectl create secret generic plexus-secrets -n plexus \
  --from-literal=PLEXUS_JWT_SECRET="$(openssl rand -base64 48 | tr -d '\n')" \
  --from-literal=PLEXUS_API_KEY_PEPPER="$(openssl rand -base64 32 | tr -d '\n')" \
  --from-literal=PLEXUS_DATABASE_URL="postgresql://plexus_app:...@pg-rw:5432/plexus" \
  --from-literal=OPENAI_API_KEY="..."
kubectl create secret generic plexus-postgres -n plexus \
  --from-literal=connection-string="postgresql://plexus_app:...@pg-rw:5432/plexus"
kubectl create secret generic plexus-prometheus-token -n plexus \
  --from-literal=token="Bearer <read-only Prometheus API token>"

kubectl apply -k deploy/k8s/base        # deployments, services, PDB, HPA, ScaledObject
```

`PLEXUS_ENV=prod` turns a missing or weak value into a failed rollout with the full
list of problems, rather than an incident at 3am.

**Two database roles, because migrations and RLS cannot share one.** Postgres skips
row-level security for superusers and table owners, so the role that runs DDL must not
be the role that serves traffic:

```sql
CREATE ROLE plexus_owner LOGIN PASSWORD '...';   -- owns the schema, migration Job only
CREATE ROLE plexus_app  LOGIN PASSWORD '...';   -- runtime: not owner, not superuser, no BYPASSRLS
GRANT USAGE ON SCHEMA public TO plexus_app;
ALTER DEFAULT PRIVILEGES FOR ROLE plexus_owner IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO plexus_app;
```

```bash
# one-shot Job (or an Argo pre-sync hook) as the owner role:
PLEXUS_DATABASE_URL="postgresql://plexus_owner:...@pg-rw:5432/plexus" plexus migrate
```

Every runtime pod then connects as `plexus_app`, and queue workers add nothing but
`SET LOCAL app.worker_scope = 'true'` inside their own transactions -- no role has
`BYPASSRLS`. Compose does the bootstrap for you in
`deploy/docker/init/01-roles.sh`; on Kubernetes run it once yourself. Reasoning in
`docs/adr/0002-row-level-security-two-database-roles.md`, and `deploy/k8s/base/secrets.example.yaml`
is the secret shape (keep real values in Sealed Secrets / SOPS, not in git).

**`deploy/k8s/base/scaledobject.yaml` needs KEDA and a reachable Prometheus.** Queue depth is
exported as `plexus_queue_depth` on `/metrics`; the trigger scrapes it rather than
querying Postgres directly, because an external SQL probe would connect as a role
without the worker scope and always read a backlog of zero. On a cluster with
neither, remove the two KEDA resources from `deploy/k8s/base/kustomization.yaml` and
rely on `deploy/k8s/base/hpa.yaml`.

`src/plexus/scaling/autoscaler.py` is the single source of truth for scaling policy
and can render the manifests it implies -- queue depth drives workers, CPU guards
the API, and hysteresis (fast scale-up, slow scale-down) stops a fleet from
oscillating around its target:

```bash
.venv/bin/python -c 'from plexus.scaling.autoscaler import render_hpa, render_keda; print(render_hpa()); print(render_keda())'
python tools/check_manifests.py          # fails if the shipped manifests drift from the renderer
```

Point-in-time recovery, connection pooling and logical replication are the
operator's job (PgBouncer, managed Postgres, or CloudNativePG all work).

---

## Security model

- **Credentials:** only HMAC digests of API keys are persisted, so a read-only
  database leak cannot be replayed. Revocation is checked on every use and JWTs are
  denylisted by `jti` mid-lifetime.
- **Algorithm pinning:** only the configured JWT algorithm is accepted; `alg: none`
  and confusion attacks are rejected before signature parsing. Tokens are
  short-lived with an issuer, audience and leeway window.
- **Input limits:** request body and prompt size are capped before any provider
  call, so an oversized prompt cannot become upstream spend.
- **Auditability:** every state change and every completion writes an audit row with
  actor, action, resource, result and trace id. Completions record cost, provider
  and fallback count but **never prompt or completion text**, so the audit log
  cannot become a second store of customer content.
- **Error hygiene:** internal failures return a stable envelope with a machine code
  and a retryability hint, and never leak stack traces to callers.

---

## Observability

Logs, traces and metrics share one correlation context, so a single `trace_id`
links an HTTP request, its provider attempts and the worker tasks it spawned --
which is what makes multi-tenant incident triage survivable.

- Structured JSON logs with tenant, subject, request and trace ids bound per task.
- W3C `traceparent` is propagated inbound and echoed on every response; shapes
  follow OpenTelemetry so the stdlib implementation can be swapped for an OTLP
  exporter without touching call sites (`.[observability]` extra).
- `GET /metrics` exposes Prometheus counters and latency histograms plus the two
  signals an autoscaler actually needs: `plexus_queue_depth` (ready tasks, the KEDA
  target) and `plexus_circuit_breaker_state{provider="..."}` (0 closed, 1 half-open,
  2 open), alongside fallbacks and idempotent replays. Both are sampled with a short
  TTL and can never turn a scrape into a 500.

---

## Testing and quality

```bash
.venv/bin/python -m pytest              # isolation, RBAC, billing, idempotency, fallback
.venv/bin/ruff check . && .venv/bin/mypy
python tools/check_manifests.py         # deploy/ parses and matches the rendered policy
```

The suite runs on SQLite with the deterministic `echo` provider, so it needs no
network and no credentials. Isolation is asserted from the outside in: a request
carrying tenant A's credential must not be able to distinguish tenant B's rows
from rows that never existed.

SQLite cannot prove that row-level security works, so `.github/workflows/ci.yml`
runs a second job against a real Postgres: `plexus migrate`, `plexus config-check`,
and an assertion that every tenant table has an RLS policy *and*
`relforcerowsecurity = 't'`. Lint and type checks, a matrix over supported Python
versions, manifest drift, and a build-and-run of the container are separate jobs.

Decisions worth arguing about are recorded in `docs/adr/`, each with the rejected
alternative and an honest cost section.

---

## Status and honest gaps

Implemented and covered by tests: multi-tenancy with RLS, capability RBAC, JWT and
API-key auth, admission control and plan limits, idempotency, the inference gateway
with hashing/fallback/breakers/budget, the model registry with its promotion gate,
deployments with traffic shifting and rollback, the leased task queue with dead
letters and sagas, fair scheduling, consistent hashing, autoscaling policy, audit
log, usage billing and telemetry.

Known gaps before this carries a paid production fleet:

- **Single-writer scaling.** The task queue relies on one Postgres primary; very
  high enqueue rates need partitioning or a dedicated broker behind the same
  `TaskQueue` interface.
- **In-process rate limits by default.** Correct per-tenant for one replica; a
  fleet-wide global budget needs the shared Redis/Postgres bucket implementation
  that the `RateLimitRegistry` interface is designed to accept.
- **No built-in artifact storage.** Artifacts are registered by URI and digest;
  blob storage and signed download URLs are external.
- **No GPU scheduling.** Plexus orchestrates and governs; it does not replace vLLM,
  Ray or a scheduler -- it fronts them.
- **No dashboards or alerts shipped.** `/metrics` is complete enough to build them
  (backlog, breaker state, p99 by route, spend), but the Grafana JSON and alert rules
  are site-specific and deliberately left out.
- **Worker autoscaling assumes KEDA + Prometheus.** Without them the fleet is static
  or CPU-driven; backlog still drains, just not elastically.
