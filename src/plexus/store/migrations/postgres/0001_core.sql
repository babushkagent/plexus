-- Plexus core schema (Postgres dialect: production).
-- Kept logically identical to the SQLite schema; only types differ.
CREATE TABLE tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    plan TEXT NOT NULL DEFAULT 'standard',
    status TEXT NOT NULL DEFAULT 'active',
    rps_limit DOUBLE PRECISION,
    burst DOUBLE PRECISION,
    max_concurrency BIGINT,
    monthly_budget_usd DOUBLE PRECISION,
    monthly_spend_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    settings_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);

CREATE TABLE memberships (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, subject, role)
);

CREATE TABLE api_keys (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    digest TEXT NOT NULL UNIQUE,
    plan TEXT NOT NULL DEFAULT 'standard',
    roles_json TEXT NOT NULL DEFAULT '[]',
    capabilities_json TEXT NOT NULL DEFAULT '[]',
    label TEXT NOT NULL DEFAULT '',
    last4 TEXT NOT NULL DEFAULT '',
    expires_at_ms BIGINT,
    revoked_at_ms BIGINT,
    last_used_at_ms BIGINT,
    use_count BIGINT NOT NULL DEFAULT 0,
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX api_keys_tenant_idx ON api_keys (tenant_id);

CREATE TABLE denied_tokens (
    jti TEXT PRIMARY KEY,
    tenant_id TEXT,
    reason TEXT NOT NULL DEFAULT '',
    expires_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX denied_tokens_expires_idx ON denied_tokens (expires_at_ms);

CREATE TABLE idempotency_keys (
    tenant_id TEXT NOT NULL,
    key TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status_code BIGINT,
    response_json TEXT,
    completed_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, key)
);

-- Digest uniqueness is scoped per tenant on purpose. A global UNIQUE(digest) lets one
-- tenant's write fail because a *different* tenant holds the same content digest, and a
-- failed insert doubles as an existence oracle over other tenants' weights.
CREATE TABLE model_versions (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    version BIGINT NOT NULL,
    digest TEXT NOT NULL,
    stage TEXT NOT NULL DEFAULT 'none',
    created_by TEXT NOT NULL,
    parent_id TEXT,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    eval_passed BIGINT,
    eval_json TEXT NOT NULL DEFAULT '{}',
    signature TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms BIGINT NOT NULL,
    UNIQUE (tenant_id, name, version),
    UNIQUE (tenant_id, digest)
);
CREATE INDEX model_versions_tenant_stage_idx ON model_versions (tenant_id, stage);

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    digest TEXT NOT NULL,
    kind TEXT NOT NULL,
    uri TEXT NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms BIGINT NOT NULL,
    UNIQUE (tenant_id, digest)
);
CREATE INDEX artifacts_tenant_kind_idx ON artifacts (tenant_id, kind);

CREATE TABLE deployments (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    model_version_id TEXT NOT NULL REFERENCES model_versions(id),
    status TEXT NOT NULL DEFAULT 'pending',
    desired_replicas BIGINT NOT NULL DEFAULT 1,
    min_replicas BIGINT NOT NULL DEFAULT 1,
    max_replicas BIGINT NOT NULL DEFAULT 3,
    traffic_percent BIGINT NOT NULL DEFAULT 100,
    shard BIGINT NOT NULL DEFAULT 0,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (tenant_id, name)
);

CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    model_version_id TEXT,
    metrics_json TEXT NOT NULL DEFAULT '{}',
    error TEXT,
    started_at_ms BIGINT NOT NULL,
    finished_at_ms BIGINT
);
CREATE INDEX runs_tenant_created_idx ON runs (tenant_id, started_at_ms);

CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    payload_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT NOT NULL DEFAULT '{}',
    attempts BIGINT NOT NULL DEFAULT 0,
    max_attempts BIGINT NOT NULL DEFAULT 5,
    dedupe_key TEXT,
    run_after_ms BIGINT NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires_at_ms BIGINT,
    heartbeat_at_ms BIGINT,
    last_error TEXT,
    created_at_ms BIGINT NOT NULL,
    started_at_ms BIGINT,
    finished_at_ms BIGINT
);
CREATE UNIQUE INDEX tasks_dedupe_idx ON tasks (tenant_id, dedupe_key) WHERE dedupe_key IS NOT NULL;
CREATE INDEX tasks_ready_idx ON tasks (status, run_after_ms, lease_expires_at_ms);
CREATE INDEX tasks_tenant_idx ON tasks (tenant_id, status);

CREATE TABLE outbox_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    type TEXT NOT NULL,
    subject TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms BIGINT NOT NULL,
    published_at_ms BIGINT
);
CREATE INDEX outbox_unpublished_idx ON outbox_events (published_at_ms, created_at_ms);

CREATE TABLE usage_records (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    credential_id TEXT,
    model TEXT NOT NULL,
    provider TEXT NOT NULL DEFAULT '',
    request_tokens BIGINT NOT NULL DEFAULT 0,
    response_tokens BIGINT NOT NULL DEFAULT 0,
    cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    status_code BIGINT NOT NULL DEFAULT 0,
    cache_hit BIGINT NOT NULL DEFAULT 0,
    request_id TEXT,
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX usage_tenant_created_idx ON usage_records (tenant_id, created_at_ms);

CREATE TABLE audit_log (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    resource TEXT NOT NULL DEFAULT '',
    result TEXT NOT NULL DEFAULT 'success',
    trace_id TEXT,
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX audit_tenant_created_idx ON audit_log (tenant_id, created_at_ms);
