# 0005 — The release gate lives in the registry write path, not in CI

- Status: Accepted
- Date: 2026-10-04
- Scope: `src/plexus/registry/store.py` (`promote`), `model_versions.stage`

## Context

Every team that ships models eventually has the same incident: something reaches
production that was never evaluated, because the check that was supposed to stop it lived
in a pipeline script. Pipeline checks are advisory. They can be skipped with `--no-verify`,
retried from a branch, run against a different registry than the one serving traffic, or
simply bypassed by anyone with a valid token and `curl`.

A promotion gate is only worth having if there is no way to express "make this version
serve traffic" that skips it. That means the check has to be in the same transaction as
the state change it protects, in the layer every caller must pass through.

## Decision

`ModelRepository.promote()` is the only way a version changes stage, and it refuses to
commit an unverified build into a traffic-bearing stage:

- Stages are `none | staging | canary | production | archived`. `TRAFFIC_STAGES` is
  `{canary, production}` -- canary carries the same gate as production, because 1% of
  users is not a debugging environment.
- Promoting into a traffic stage requires `eval_passed` and a non-empty `signature`. A
  missing prerequisite raises `Conflict("promotion blocked by release gate")` with
  `details.missing` listing exactly what is absent, so a CI job can print an actionable
  reason instead of a stack trace.
- Those two facts are written by separate calls (`POST .../eval`, `POST .../sign`)
  that record an audit row and an outbox event; neither can be set at registration time,
  so a build cannot arrive pre-qualified.
- **At most one live version per model name per traffic stage.** Promoting a new version
  demotes the previous one to `staging` in the same statement, which keeps "what is in
  production" unambiguous for rollback, billing attribution and audit. Roll-forward and
  rollback are then single-row transitions.
- Every transition appends `model.version.promoted` with `from`, `to` and `actor` to the
  outbox (ADR 0001), so the release history is derived state rather than a log file
  someone has to keep.

The gate is enforced in the domain layer, not in the HTTP handler. A future gRPC front
door, a CLI verb or a migration script gets the same refusal for free; forgetting to wire
it up is not an option those paths have.

## Consequences

**Good.** "Prod" is a database fact with one writer, so `SELECT ... WHERE stage =
'production'` is a trustworthy answer and an unverified model cannot be serving traffic
right now even if something is misconfigured. Unsafe promotions fail loudly at the only
moment they could be refused cheaply, and the audit trail is complete without any external
system being healthy.

**Costs and limits.**

- Emergent incidents can still take down a live version; the gate blocks *unverified*
  promotions, not bad ones. Deployments still need traffic shifting and rollback.
- The gate trusts that whoever calls `POST .../eval` reported an honest result. It is
  an integrity control against accidents and bypasses, not a defence against a malicious
  actor holding `model:promote` plus `model:write`. Capability separation
  (`ml_engineer` cannot promote) and the audit log are what narrow that.
- CI still has to *run* the evaluation; the gate only refuses to accept its absence. Teams
  that never wire up an evaluator will find every promotion blocked, which is the intended
  failure direction but is surprising on day one.
- Signature verification is deliberately not implemented here: Plexus stores and requires
  an opaque signature and leaves cryptographic policy (cosign, Sigstore, KMS) to the
  supply chain tooling that already exists for this.
- One live version per name/stage forbids blue/green cohorts at the registry level;
  multi-version traffic splitting is expressed as a Deployment with weights, not as two
  `production` rows.
