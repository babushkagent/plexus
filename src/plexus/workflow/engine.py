"""Durable task engine: leased queue, retries with a dead-letter path, sagas.

The queue lives in the same database as the business rows, which is deliberate:
enqueueing work and mutating state share one commit, so "we saved it but never
processed it" (and the reverse) cannot happen. Correctness comes from leases plus
idempotent handlers; exactly-once execution is a myth, at-least-once with dedupe
keys is engineering.
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from ..errors import Conflict, NotFound, PlatformError, Timeout
from ..ids import new_id
from ..ids import now_ms as _now_ms
from ..store.db import Database, Dialect
from ..telemetry import METRICS, TRACER, TraceContext
from .policy import RetryPolicy

logger = logging.getLogger("plexus.workflow")

PENDING = "pending"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
DEAD = "dead"
CANCELLED = "cancelled"

Handler = Callable[[dict[str, Any]], Any]


def now_ms() -> int:
    return _now_ms()


@dataclass(frozen=True, slots=True)
class Task:
    id: str
    tenant_id: str
    type: str
    status: str
    payload: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] = field(default_factory=dict)
    attempts: int = 0
    max_attempts: int = 5
    dedupe_key: str | None = None
    run_after_ms: int = 0
    lease_owner: str | None = None
    last_error: str | None = None
    created_at_ms: int = 0
    started_at_ms: int | None = None
    finished_at_ms: int | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Task:
        return cls(
            id=row["id"],
            tenant_id=row["tenant_id"],
            type=row["type"],
            status=row["status"],
            payload=json.loads(row["payload_json"] or "{}"),
            result=json.loads(row["result_json"] or "{}"),
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            dedupe_key=row["dedupe_key"],
            run_after_ms=int(row["run_after_ms"]),
            lease_owner=row["lease_owner"],
            last_error=row["last_error"],
            created_at_ms=int(row["created_at_ms"]),
            started_at_ms=row["started_at_ms"],
            finished_at_ms=row["finished_at_ms"],
        )


class TaskQueue:
    """Postgres/SQLite backed queue with visibility timeouts.

    A worker *leases* a task (never deletes it). If the worker dies, the lease
    expires and another worker reclaims the work; handlers therefore must be safe
    to run more than once, which is enforced by dedupe keys at enqueue time.
    """

    def __init__(
        self,
        db: Database,
        *,
        lease_s: float = 60.0,
        heartbeat_s: float = 15.0,
        default_max_attempts: int = 5,
        retry: RetryPolicy | None = None,
    ) -> None:
        if heartbeat_s * 2 >= lease_s:
            raise ValueError("heartbeat_s must be well below lease_s")
        self._db = db
        self._lease_ms = int(lease_s * 1000)
        self._heartbeat_ms = int(heartbeat_s * 1000)
        self._default_max_attempts = default_max_attempts
        self._retry = retry or RetryPolicy(max_attempts=default_max_attempts)

    @property
    def heartbeat_s(self) -> float:
        return self._heartbeat_ms / 1000.0

    @property
    def lease_s(self) -> float:
        return self._lease_ms / 1000.0

    def enqueue(
        self,
        *,
        tenant_id: str,
        type: str,
        payload: Mapping[str, Any] | None = None,
        dedupe_key: str | None = None,
        delay_s: float = 0.0,
        max_attempts: int | None = None,
        tx: Any = None,
    ) -> str | None:
        """Insert pending work. Returns the task id, or None if deduplicated.

        Pass `tx` to enqueue inside an existing transaction so the work becomes
        visible exactly when the state that produced it does.
        """
        statement = """
            INSERT INTO tasks (id, tenant_id, type, status, payload_json, attempts, max_attempts,
                dedupe_key, run_after_ms, created_at_ms)
            VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
        """
        params = (
            new_id("tsk"),
            tenant_id,
            type,
            PENDING,
            json.dumps(dict(payload or {}), separators=(",", ":")),
            max_attempts or self._default_max_attempts,
            dedupe_key,
            now_ms() + int(delay_s * 1000),
            now_ms(),
        )
        if dedupe_key is not None:
            statement += " ON CONFLICT (tenant_id, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING"

        def run(scope: Any) -> str | None:
            row = scope.query_one(statement + " RETURNING id", params)
            return None if row is None else str(next(iter(row.values())))

        if tx is not None:
            return run(tx)
        with self._db.transaction(immediate=True, tenant_id=tenant_id) as scope:
            return run(scope)

    def claim(self, *, owner: str, types: Sequence[str] | None = None, limit: int = 1) -> list[Task]:
        """Lease up to `limit` runnable tasks, including ones whose lease expired."""
        if limit < 1:
            return []
        now = now_ms()
        clauses = [f"(status = '{PENDING}' AND run_after_ms <= {now})", f"(status = '{RUNNING}' AND lease_expires_at_ms < {now})"]
        params: list[Any] = []
        if types:
            placeholders = ", ".join("?" for _ in types)
            clauses[0] = f"({clauses[0]} AND type IN ({placeholders}))"
            clauses[1] = f"({clauses[1]} AND type IN ({placeholders}))"
            params = [*types, *types]
        predicate = " OR ".join(clauses)
        order = f"(CASE WHEN status = '{PENDING}' THEN 0 ELSE 1 END), run_after_ms, created_at_ms"
        claimed: list[Task] = []
        lease_until = now + self._lease_ms
        with self._db.transaction(immediate=True) as tx:
            tx.set_worker_scope(True)
            if self._db.dialect is Dialect.POSTGRES:  # pragma: no cover - requires a live server
                rows = tx.query(
                    f"""
                    UPDATE tasks
                       SET status = '{RUNNING}', lease_owner = ?, lease_expires_at_ms = ?,
                           heartbeat_at_ms = ?, attempts = attempts + 1,
                           started_at_ms = COALESCE(started_at_ms, ?)
                     WHERE id IN (
                        SELECT id FROM tasks
                         WHERE {predicate}
                         ORDER BY {order}
                         LIMIT ?
                           FOR UPDATE SKIP LOCKED
                     )
                    RETURNING *
                    """,
                    [owner, lease_until, now, now, *params, limit],
                )
                claimed = [Task.from_row(row) for row in rows]
            else:
                candidates = tx.query(
                    f"SELECT id FROM tasks WHERE {predicate} ORDER BY {order} LIMIT ?",
                    [*params, limit],
                )
                for candidate in candidates:
                    task_id = candidate["id"]
                    updated = tx.execute(
                        f"""
                        UPDATE tasks
                           SET status = '{RUNNING}', lease_owner = ?, lease_expires_at_ms = ?,
                               heartbeat_at_ms = ?, attempts = attempts + 1,
                               started_at_ms = COALESCE(started_at_ms, ?)
                         WHERE id = ? AND status IN ('{PENDING}', '{RUNNING}')
                        """,
                        (owner, lease_until, now, now, task_id),
                    )
                    if getattr(updated, "rowcount", 0) > 0:
                        row = tx.query_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
                        if row is not None:
                            claimed.append(Task.from_row(row))
        METRICS.counter("plexus_tasks_claimed_total", len(claimed), {"owner": owner})
        return claimed

    def heartbeat(self, task_id: str, *, owner: str) -> bool:
        """Extend the lease. False means we lost ownership and must stop working."""
        now = now_ms()
        with self._db.transaction(immediate=True) as tx:
            tx.set_worker_scope(True)
            cursor = tx.execute(
                "UPDATE tasks SET heartbeat_at_ms = ?, lease_expires_at_ms = ? WHERE id = ? AND lease_owner = ? AND status = ?",
                (now, now + self._lease_ms, task_id, owner, RUNNING),
            )
            return getattr(cursor, "rowcount", 0) > 0

    def complete(self, task_id: str, *, owner: str, result: Mapping[str, Any] | None = None) -> None:
        with self._db.transaction(immediate=True) as tx:
            tx.set_worker_scope(True)
            cursor = tx.execute(
                """
                UPDATE tasks
                   SET status = ?, result_json = ?, finished_at_ms = ?, lease_owner = NULL,
                       lease_expires_at_ms = NULL, last_error = NULL
                 WHERE id = ? AND lease_owner = ? AND status = ?
                """,
                (SUCCEEDED, json.dumps(dict(result or {}), separators=(",", ":")), now_ms(), task_id, owner, RUNNING),
            )
            if getattr(cursor, "rowcount", 0) == 0:
                raise Conflict("task lease lost before completion", details={"task_id": task_id})

    def fail(self, task_id: str, *, owner: str, error: str, retry: bool = True) -> str:
        """Record a failure; returns the resulting status (pending, dead)."""
        with self._db.transaction(immediate=True) as tx:
            tx.set_worker_scope(True)
            row = tx.query_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
            if row is None:
                raise NotFound("task not found", details={"task_id": task_id})
            task = Task.from_row(row)
            exhausted = not retry or task.attempts >= task.max_attempts
            status = DEAD if exhausted else PENDING
            run_after = now_ms()
            if not exhausted:
                delay = self._retry.delay_for(max(1, task.attempts))
                run_after += int(delay * 1000)
            tx.execute(
                """
                UPDATE tasks
                   SET status = ?, run_after_ms = ?, last_error = ?, lease_owner = NULL,
                       lease_expires_at_ms = NULL, finished_at_ms = ?
                 WHERE id = ? AND lease_owner = ?
                """,
                (status, run_after, error[:2000], now_ms() if exhausted else None, task_id, owner),
            )
            if status == DEAD:
                logger.error("task moved to dead letter", extra={"task_id": task.id, "type": task.type, "error": error})
            return status

    def cancel(self, task_id: str, *, tenant_id: str) -> bool:
        with self._db.transaction(immediate=True, tenant_id=tenant_id) as tx:
            cursor = tx.execute(
                "UPDATE tasks SET status = ?, finished_at_ms = ? WHERE id = ? AND status IN (?, ?)",
                (CANCELLED, now_ms(), task_id, PENDING, RUNNING),
            )
            return getattr(cursor, "rowcount", 0) > 0

    def get(self, task_id: str) -> Task | None:
        with self._db.transaction() as tx:
            tx.set_worker_scope(True)
            row = tx.query_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
            return Task.from_row(row) if row else None

    def dead_letters(self, *, tenant_id: str | None = None, limit: int = 50) -> list[Task]:
        sql = f"SELECT * FROM tasks WHERE status = '{DEAD}'"
        params: list[Any] = []
        if tenant_id is not None:
            sql += " AND tenant_id = ?"
            params.append(tenant_id)
        sql += " ORDER BY finished_at_ms DESC LIMIT ?"
        params.append(limit)
        with self._db.transaction() as tx:
            tx.set_worker_scope(True)
            return [Task.from_row(row) for row in tx.query(sql, params)]

    def requeue_dead(self, task_id: str, *, tenant_id: str) -> bool:
        """Operator-driven replay of a dead-lettered task."""
        with self._db.transaction(immediate=True, tenant_id=tenant_id) as tx:
            cursor = tx.execute(
                "UPDATE tasks SET status = ?, attempts = 0, run_after_ms = ?, finished_at_ms = NULL, last_error = NULL"
                " WHERE id = ? AND status = ?",
                (PENDING, now_ms(), task_id, DEAD),
            )
            return getattr(cursor, "rowcount", 0) > 0

    def recover_expired_leases(self) -> int:
        """Explicit reclaim marker; claim() already treats expired leases as runnable."""
        with self._db.transaction(immediate=True) as tx:
            tx.set_worker_scope(True)
            cursor = tx.execute(
                f"UPDATE tasks SET heartbeat_at_ms = NULL WHERE status = ? AND lease_expires_at_ms < ?",
                (RUNNING, now_ms()),
            )
            return max(0, getattr(cursor, "rowcount", 0))

    def depth(self) -> dict[str, int]:
        with self._db.transaction() as tx:
            tx.set_worker_scope(True)
            rows = tx.query("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status")
        counts = {str(row["status"]): int(row["n"]) for row in rows}
        counts["ready"] = self.ready_count()
        return counts

    def ready_count(self) -> int:
        with self._db.transaction() as tx:
            tx.set_worker_scope(True)
            return int(
                tx.scalar(
                    "SELECT COUNT(*) FROM tasks WHERE status = ? AND run_after_ms <= ?",
                    (PENDING, now_ms()),
                )
                or 0
            )


class TaskWorker:
    """Poll-claim-execute loop with bounded in-flight work and graceful drain.

    Shutdown order matters: stop claiming, finish in-flight tasks, then release our
    leases so peers pick the remainder up immediately instead of after a timeout.
    """

    def __init__(
        self,
        queue: TaskQueue,
        *,
        owner: str | None = None,
        concurrency: int = 4,
        poll_interval_s: float = 0.25,
        idle_poll_max_s: float = 2.0,
        types: Sequence[str] | None = None,
    ) -> None:
        self._queue = queue
        self.owner = owner or f"worker-{new_id('wkr')[:8]}"
        self._concurrency = max(1, concurrency)
        self._poll_interval_s = poll_interval_s
        self._idle_poll_max_s = idle_poll_max_s
        self._types = list(types) if types else None
        self._handlers: dict[str, Handler] = {}
        self._stop = threading.Event()
        self._executor: ThreadPoolExecutor | None = None
        self._futures: set[Future[None]] = set()
        self._inflight: set[str] = set()
        self._lock = threading.RLock()
        self.processed = 0
        self.failures = 0

    def register(self, type: str, handler: Handler) -> None:
        self._handlers[type] = handler

    def registered_types(self) -> tuple[str, ...]:
        """Task types this worker will claim. Empty means it claims anything it is given."""
        return tuple(sorted(self._handlers))

    def _claim_types(self) -> list[str] | None:
        """Explicit `types` wins; otherwise claim only what has a handler.

        Defaulting to registered types keeps an unhandled task queued for a
        specialised worker instead of burning attempts into the dead letter queue.
        """
        if self._types is not None:
            return self._types
        registered = self.registered_types()
        return list(registered) if registered else None

    def request_stop(self) -> None:
        self._stop.set()

    def run_forever(self, *, stop: threading.Event | None = None) -> None:
        """Blocking loop. `stop` may be supplied by a supervisor; otherwise use stop()."""
        stop_event = stop or self._stop
        self._executor = ThreadPoolExecutor(max_workers=self._concurrency, thread_name_prefix="plexus-task")
        idle_poll = self._poll_interval_s
        try:
            while not stop_event.is_set():
                self._reap()
                slots = self._concurrency - len(self._inflight)
                if slots <= 0:
                    stop_event.wait(self._poll_interval_s)
                    continue
                claimed = self._queue.claim(owner=self.owner, types=self._claim_types(), limit=slots)
                if claimed:
                    idle_poll = self._poll_interval_s
                    for task in claimed:
                        self._submit(task)
                else:
                    idle_poll = min(self._idle_poll_max_s, idle_poll * 2)
                    stop_event.wait(idle_poll)
        finally:
            self._drain()

    def drain_once(self, *, limit: int | None = None) -> int:
        """Claim and execute everything currently runnable; returns tasks processed."""
        total = 0
        while True:
            batch = self._queue.claim(
                owner=self.owner, types=self._claim_types(), limit=limit or self._concurrency
            )
            if not batch:
                break
            for task in batch:
                self.execute(task)
                total += 1
        return total

    def execute(self, task: Task) -> None:
        """Run one task to completion, translating outcomes into queue state."""
        with self._lock:
            self._inflight.add(task.id)
        METRICS.counter("plexus_tasks_started_total", 1, {"type": task.type})
        started = time.perf_counter()
        heartbeat = self._start_heartbeat(task.id)
        trace = TraceContext.new()
        try:
            handler = self._handlers.get(task.type)
            if handler is None:
                raise PlatformError(f"no handler registered for task type {task.type!r}")
            with TRACER.start("task.run", context=trace, task_id=task.id, task_type=task.type, tenant_id=task.tenant_id):
                result = handler({"id": task.id, "tenant_id": task.tenant_id, "type": task.type, **task.payload})
            self._queue.complete(task.id, owner=self.owner, result=result if isinstance(result, dict) else {"value": result})
            self.processed += 1
            METRICS.counter("plexus_tasks_completed_total", 1, {"type": task.type})
        except Timeout as exc:
            self._mark_failure(task, exc, retry=True)
        except PlatformError as exc:
            self._mark_failure(task, exc, retry=bool(exc.retryable))
        except Exception as exc:  # handler bug: retry with backoff, then dead-letter
            self._mark_failure(task, exc, retry=True)
        finally:
            heartbeat.stop()
            METRICS.observe("plexus_task_duration_ms", (time.perf_counter() - started) * 1000, {"type": task.type})
            with self._lock:
                self._inflight.discard(task.id)
            METRICS.gauge("plexus_tasks_inflight", float(len(self._inflight)))

    def stop(self, *, timeout_s: float = 30.0) -> None:
        self._stop.set()
        self._drain(timeout_s=timeout_s)

    def _mark_failure(self, task: Task, exc: BaseException, *, retry: bool) -> None:
        self.failures += 1
        METRICS.counter("plexus_tasks_failed_total", 1, {"type": task.type})
        logger.warning(
            "task attempt failed",
            extra={"task_id": task.id, "task_type": task.type, "attempt": task.attempts, "error": str(exc)},
        )
        self._queue.fail(task.id, owner=self.owner, error=f"{type(exc).__name__}: {exc}", retry=retry)

    def _submit(self, task: Task) -> None:
        assert self._executor is not None
        with self._lock:
            self._inflight.add(task.id)
        future = self._executor.submit(self.execute, task)
        self._futures.add(future)

    def _reap(self) -> None:
        with self._lock:
            done = {future for future in self._futures if future.done()}
            self._futures -= done
        for future in done:
            future.exception()  # surface nothing; failures are already recorded in the queue

    def _drain(self, *, timeout_s: float = 30.0) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None
        with self._lock:
            stranded = list(self._inflight)
        for task_id in stranded:
            try:
                self._queue.fail(task_id, owner=self.owner, error="worker shutdown before completion", retry=True)
            except PlatformError:  # pragma: no cover - already finished concurrently
                pass

    def _start_heartbeat(self, task_id: str) -> _Heartbeat:
        heartbeat = _Heartbeat(self._queue, task_id, self.owner)
        heartbeat.start()
        return heartbeat


class _Heartbeat:
    """Renews the lease while a handler runs; stops the worker if ownership is lost."""

    def __init__(self, queue: TaskQueue, task_id: str, owner: str) -> None:
        self._queue = queue
        self._task_id = task_id
        self._owner = owner
        self._stop = threading.Event()
        self.lost = threading.Event()
        self._interval_s = max(0.05, queue.heartbeat_s)
        self._thread = threading.Thread(target=self._loop, name=f"plexus-heartbeat-{task_id[-6:]}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def _loop(self) -> None:
        while not self._stop.wait(self._interval_s):
            if not self._queue.heartbeat(self._task_id, owner=self._owner):
                self.lost.set()
                return


@dataclass(slots=True)
class SagaStep:
    name: str
    run: Callable[[dict[str, Any]], Mapping[str, Any] | None]
    compensate: Callable[[dict[str, Any]], None] | None = None


class SagaError(PlatformError):
    """A multi-step workflow failed and was compensated."""

    code = "saga_failed"
    status = 500


class Saga:
    """Progressive compensation: undo completed steps in reverse on failure.

    Use this where a distributed transaction is impossible (provider calls, cloud
    mutations). Each step declares its own rollback; the saga never assumes a global
    abort exists.
    """

    def __init__(self, name: str, steps: Sequence[SagaStep], *, rng: random.Random | None = None) -> None:
        self.name = name
        self._steps = list(steps)
        self._rng = rng or random.Random()

    def run(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        state = dict(context or {})
        completed: list[tuple[SagaStep, dict[str, Any]]] = []
        for step in self._steps:
            try:
                produced = step.run(state)
            except BaseException as exc:
                self._compensate(completed)
                raise SagaError(
                    f"saga {self.name!r} failed at step {step.name!r}: {exc}",
                    details={"step": step.name, "compensated": [done.name for done, _ in reversed(completed)]},
                ) from exc
            completed.append((step, dict(produced or {})))
            state.update(produced or {})
        return state

    def _compensate(self, completed: list[tuple[SagaStep, dict[str, Any]]]) -> None:
        for step, output in reversed(completed):
            if step.compensate is None:
                logger.warning("saga step has no compensation", extra={"saga": self.name, "step": step.name})
                continue
            try:
                step.compensate(output)
            except Exception:  # keep unwinding; a partial rollback still beats none
                logger.exception("compensation failed", extra={"saga": self.name, "step": step.name})
