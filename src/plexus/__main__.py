"""`plexus` command line: one binary, two long-running roles and a few ops verbs.

The API and the worker are separate processes on purpose. The API is latency
sensitive and scales with request rate; the worker is throughput sensitive and
scales with queue depth. Sharing a process would let a batch task starve inference
requests of CPU and make both autoscalers lie.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from collections.abc import Callable
from typing import Any

from .api.server import App, serve
from .config import ConfigError, Settings, load_settings
from .errors import Conflict
from .telemetry import METRICS, configure_logging, new_trace_id
from .tenancy.auth import TokenIssuer, mint_api_key
from .tenancy.context import Plan
from .tenancy.rbac import ROLE_OWNER
from .workflow.engine import TaskQueue, TaskWorker

EXIT_OK = 0
EXIT_CONFIG = 2
EXIT_UNREACHABLE = 3


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        settings = _settings_for(args)
    except ConfigError as exc:
        print("configuration invalid:", file=sys.stderr)
        for problem in exc.problems:
            print(f"  - {problem}", file=sys.stderr)
        return EXIT_CONFIG
    configure_logging(settings.log_level, json_logs=settings.json_logs)
    command: Callable[[argparse.Namespace, Settings], int] = getattr(args, "_command")
    try:
        return command(args, settings)
    except ConfigError as exc:
        for problem in exc.problems:
            print(f"error: {problem}", file=sys.stderr)
        return EXIT_CONFIG
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_UNREACHABLE


# --------------------------------------------------------------------------- verbs


def _cmd_version(args: argparse.Namespace, settings: Settings) -> int:
    from . import __version__

    print(json.dumps({"name": settings.service_name, "version": __version__, "python": sys.version.split()[0]}))
    return EXIT_OK


def _cmd_config_check(args: argparse.Namespace, settings: Settings) -> int:
    """Print the redacted effective config and confirm the store answers."""
    body = _redacted(settings)
    reachable: dict[str, Any]
    try:
        from .store.db import Database

        database = Database(settings)
        reachable = {"database": "reachable" if database.ping() else "unreachable"}
    except Exception as exc:  # noqa: BLE001 - a check command reports, it does not raise
        reachable = {"database": f"error: {type(exc).__name__}: {exc}"}
    print(json.dumps({**body, **reachable}, indent=2, sort_keys=True))
    if reachable.get("database") != "reachable":
        return EXIT_UNREACHABLE
    return EXIT_OK


def _cmd_migrate(args: argparse.Namespace, settings: Settings) -> int:
    from .store.db import Database

    applied = Database(settings).migrate(force=args.force)
    print(json.dumps({"applied": applied, "dialect": settings.database_url.split(":", 1)[0]}))
    return EXIT_OK


def _cmd_serve(args: argparse.Namespace, settings: Settings) -> int:
    app = App(settings)
    if args.migrate:
        app.init_schema()
    serve(app, host=args.host, port=args.port)
    return EXIT_OK


def _cmd_worker(args: argparse.Namespace, settings: Settings) -> int:
    """Run the task worker. Registers platform handlers; extra types are no-ops."""
    app = App(settings)
    if args.migrate:
        app.init_schema()
    queue = TaskQueue(
        app.db,
        lease_s=settings.task_lease_s,
        heartbeat_s=settings.task_heartbeat_s,
        default_max_attempts=settings.task_max_attempts,
    )
    concurrency = args.concurrency or settings.worker_concurrency
    worker = TaskWorker(queue, owner=args.owner, concurrency=concurrency, types=args.types)
    register_platform_handlers(worker, app)
    stop = _install_stop_handler()
    print(
        json.dumps(
            {
                "event": "worker_started",
                "owner": worker.owner,
                "concurrency": concurrency,
                "handlers": sorted(worker.registered_types()),
            }
        ),
        flush=True,
    )
    if args.once:
        processed = worker.drain_once()
        print(json.dumps({"event": "drained", "processed": processed}))
        return EXIT_OK
    worker.run_forever(stop=stop)
    return EXIT_OK


def _cmd_seed(args: argparse.Namespace, settings: Settings) -> int:
    """Create a demo tenant plus one owner API key. Prints the secret exactly once."""
    app = App(settings)
    app.init_schema()
    plan = Plan(args.plan)
    with app.uow(immediate=True) as uow:
        existing = [t for t in uow.tenants.list() if t.name == args.name]
        if existing and not args.force:
            raise Conflict(f"tenant {args.name!r} already exists", details={"tenant_id": existing[0].id})
        tenant = existing[0] if existing else uow.tenants.create(name=args.name, plan=plan)
        minted = mint_api_key(
            tenant_id=tenant.id,
            subject=args.subject,
            plan=plan,
            roles=[ROLE_OWNER],
            label=args.label,
            pepper=settings.api_key_pepper,
        )
        uow.keys.add(minted.record)
        tenant_id = tenant.id
    # The plaintext key lives in this response and nowhere else: we store a digest.
    print(
        json.dumps(
            {
                "tenant_id": tenant_id,
                "key_id": minted.record.id,
                "api_key": minted.secret,
                "plan": plan.value,
                "note": "store the api_key now; only a digest is persisted",
            },
            indent=2,
        )
    )
    return EXIT_OK


def _cmd_token(args: argparse.Namespace, settings: Settings) -> int:
    """Mint a short-lived JWT. Handy for local testing and CI smoke tests."""
    issuer = TokenIssuer(settings)
    token = issuer.issue(
        tenant_id=args.tenant_id,
        subject=args.subject,
        roles=args.roles or [ROLE_OWNER],
        plan=Plan(args.plan),
        platform=args.platform,
        ttl_s=args.ttl_s,
    )
    print(token)
    return EXIT_OK


# ------------------------------------------------------------------- worker tasks


def register_platform_handlers(worker: TaskWorker, app: App) -> None:
    """Wire the task types the control plane itself enqueues.

    Handlers are idempotent by construction: each one re-reads current state and
    writes a monotonic marker, so a lease-expiry replay is a harmless no-op.
    """

    def outbox_relay(payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = str(payload.get("tenant_id", ""))
        limit = int(payload.get("limit", 100) or 100)
        with app.uow(tenant_id=tenant_id or None, immediate=True) as uow:
            pending = uow.outbox.unpublished(limit=limit)
            uow.outbox.mark_published([str(row["id"]) for row in pending])
        return {"published": len(pending)}

    def usage_rollup(payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = str(payload.get("tenant_id", ""))
        days = int(payload.get("days", 30) or 30)
        import time

        since = int((time.time() - days * 86_400) * 1000)
        with app.uow(tenant_id=tenant_id) as uow:
            summary = uow.usage.summarize(tenant_id, since_ms=since)
        return {"tenant_id": tenant_id, "requests": summary.get("requests", 0)}

    def echo(payload: dict[str, Any]) -> dict[str, Any]:
        return {"echo": {k: v for k, v in payload.items() if k not in {"id", "tenant_id", "type"}}}

    worker.register("outbox.relay", outbox_relay)
    worker.register("usage.rollup", usage_rollup)
    worker.register("echo", echo)


# ------------------------------------------------------------------------ plumbing


def _install_stop_handler() -> Any:
    import signal
    import threading

    stop = threading.Event()

    def _handler(signum: int, _frame: Any) -> None:
        if stop.is_set():
            os._exit(128 + signum)
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(ValueError):
            signal.signal(sig, _handler)
    return stop


def _settings_for(args: argparse.Namespace) -> Settings:
    overrides: dict[str, Any] = {}
    if getattr(args, "database_url", None):
        overrides["database_url"] = args.database_url
    if getattr(args, "env", None):
        overrides["env"] = args.env
    if getattr(args, "port", None) is not None:
        overrides["port"] = args.port
    settings = load_settings(os.environ, dotenv=args.dotenv, **overrides)
    # CLI-invoked one-shots should be greppable as one trace across processes.
    os.environ.setdefault("PLEXUS_TRACE_ID", new_trace_id())
    return settings


_SECRET_FIELDS = ("jwt_secret", "api_key_pepper", "openai_api_key")


def _redacted(settings: Settings) -> dict[str, Any]:
    body: dict[str, Any] = {}
    for field in settings.__dataclass_fields__:
        value = getattr(settings, field)
        if field in _SECRET_FIELDS:
            body[field] = "***set***" if value else "***unset***"
        elif isinstance(value, (str, int, float, bool, type(None))):
            body[field] = value
        elif isinstance(value, tuple):
            body[field] = list(value)
        elif hasattr(value, "value"):
            body[field] = value.value
    url = str(body.get("database_url", ""))
    if "@" in url:
        scheme, _, rest = url.partition("://")
        body["database_url"] = f"{scheme}://***@{rest.rsplit('@', 1)[-1]}"
    return body


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="plexus", description=__doc__.splitlines()[0] if __doc__ else "plexus")
    parser.add_argument("--dotenv", default=".env", help="path to an env file (use 'none' to skip)")
    parser.add_argument("--database-url", dest="database_url", help="override PLEXUS_DATABASE_URL")
    parser.add_argument("--env", choices=["dev", "test", "prod"], help="runtime profile")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add(name: str, help_text: str, command: Callable[..., int]) -> argparse.ArgumentParser:
        sub = subparsers.add_parser(name, help=help_text, description=help_text)
        sub.set_defaults(_command=command)
        return sub

    add("version", "Print version information", _cmd_version)
    add("config-check", "Validate configuration and probe the store", _cmd_config_check)

    migrate = add("migrate", "Apply pending schema migrations", _cmd_migrate)
    migrate.add_argument("--force", action="store_true", help="re-run migrations that already applied")

    serve_p = add("serve", "Run the HTTP control plane and inference gateway", _cmd_serve)
    serve_p.add_argument("--host", help="bind address (default from settings)")
    serve_p.add_argument("--port", type=int, help="bind port (default from settings)")
    serve_p.add_argument("--migrate", action="store_true", help="apply migrations before serving")

    worker_p = add("worker", "Run the asynchronous task worker", _cmd_worker)
    worker_p.add_argument("--concurrency", type=int, default=None)
    worker_p.add_argument("--owner", help="lease owner name (defaults to a generated id)")
    worker_p.add_argument("--types", nargs="*", default=None, help="claim only these task types")
    worker_p.add_argument("--once", action="store_true", help="drain runnable tasks and exit")
    worker_p.add_argument("--migrate", action="store_true", help="apply migrations before serving")

    seed = add("seed", "Create a demo tenant and print an owner API key", _cmd_seed)
    seed.add_argument("--name", default="acme-demo")
    seed.add_argument("--subject", default="seed@plexus.dev")
    seed.add_argument("--label", default="seed")
    seed.add_argument("--plan", choices=[p.value for p in Plan], default=Plan.STANDARD.value)
    seed.add_argument("--force", action="store_true", help="reuse an existing tenant of the same name")

    token = add("token", "Mint a JWT for a tenant", _cmd_token)
    token.add_argument("--tenant-id", required=True)
    token.add_argument("--subject", default="cli")
    token.add_argument("--roles", nargs="*", default=None)
    token.add_argument("--plan", choices=[p.value for p in Plan], default=Plan.STANDARD.value)
    token.add_argument("--platform", action="store_true", help="grant the platform plane")
    token.add_argument("--ttl-s", type=int, default=900)

    return parser


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
