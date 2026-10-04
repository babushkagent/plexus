"""Database access: one thin, honest abstraction over SQLite (local/test) and
Postgres (production).

Two invariants matter more than the API surface:
1. Every tenant-owned read/write happens inside a transaction that has bound the
   tenant id (advisory: Postgres RLS enforces it server side, SQLite relies on the
   repository predicates, and tests assert both).
2. Migrations are checksummed and applied exactly once under a lock, so N pods
   booting at once cannot double-apply or drift.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from importlib import resources
from pathlib import Path
from typing import Any, Protocol

from ..config import Settings
from ..errors import PlatformError, UpstreamUnavailable


class Dialect(str, Enum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"


class Tx(Protocol):
    def execute(self, sql: str, params: Sequence[Any] | None = None) -> Any: ...

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]: ...

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None: ...

    def scalar(self, sql: str, params: Sequence[Any] | None = None) -> Any: ...

    def set_tenant(self, tenant_id: str) -> None: ...

    def set_worker_scope(self, enabled: bool) -> None: ...

    def emit(self, event: Mapping[str, Any]) -> None: ...


@dataclass(frozen=True, slots=True)
class DatabaseUrl:
    dialect: Dialect
    path: str | None = None
    dsn: str | None = None

    @classmethod
    def parse(cls, url: str) -> DatabaseUrl:
        if url.startswith("sqlite:///"):
            return cls(Dialect.SQLITE, path=url[len("sqlite:///") :])
        if url.startswith(("postgresql://", "postgres://")):
            return cls(Dialect.POSTGRES, dsn=url)
        raise PlatformError(f"unsupported database url scheme: {url!r}")


class _Backend(Protocol):
    dialect: Dialect

    def connection(self) -> Any: ...

    def begin(self, conn: Any, *, immediate: bool = False) -> None: ...

    def commit(self, conn: Any) -> None: ...

    def rollback(self, conn: Any) -> None: ...

    def in_transaction(self, conn: Any) -> bool: ...

    def close(self) -> None: ...


class SqliteBackend:
    """WAL + busy timeout + foreign keys; one connection per thread."""

    dialect = Dialect.SQLITE

    def __init__(self, path: str, *, statement_timeout_ms: int) -> None:
        self._path = path
        if path not in (":memory:", "") and not path.startswith(":memory:"):
            Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._shared: sqlite3.Connection | None = None
        self._in_memory = path.startswith(":memory:") or path in ("", ":memory:")
        self._lock = threading.RLock()
        self._statement_timeout_ms = statement_timeout_ms

    def connection(self) -> sqlite3.Connection:
        if self._in_memory:
            # A shared in-memory database is scoped to a single connection; safe because
            # tests and CLI runs are short-lived. Writes are serialized by self._lock.
            with self._lock:
                if self._shared is None:
                    self._shared = self._new(":memory:")
                return self._shared
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._new(self._path)
            self._local.conn = conn
        return conn

    def _new(self, path: str) -> sqlite3.Connection:
        # isolation_level=None hands transaction control to us; the default mode
        # silently opens transactions around DML and makes BEGIN placement a lie.
        conn = sqlite3.connect(
            path, timeout=max(1.0, self._statement_timeout_ms / 1000), check_same_thread=False, isolation_level=None
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def begin(self, conn: sqlite3.Connection, *, immediate: bool = False) -> None:
        if not self.in_transaction(conn):
            conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")

    def commit(self, conn: sqlite3.Connection) -> None:
        if self.in_transaction(conn):
            conn.commit()

    def rollback(self, conn: sqlite3.Connection) -> None:
        if self.in_transaction(conn):
            conn.rollback()

    def in_transaction(self, conn: sqlite3.Connection) -> bool:
        return bool(conn.in_transaction)

    def close(self) -> None:
        with self._lock:
            if self._shared is not None:
                self._shared.close()
                self._shared = None
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None


class PostgresBackend:
    dialect = Dialect.POSTGRES

    def __init__(self, dsn: str, *, pool_size: int, statement_timeout_ms: int) -> None:
        try:
            import psycopg  # type: ignore[import-not-found]
            from psycopg_pool import ConnectionPool  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - depends on optional extra
            raise PlatformError(
                "postgres support requires the postgres extra: pip install 'plexus[postgres]'"
            ) from exc
        self._dsn = dsn
        self._statement_timeout_ms = statement_timeout_ms
        self._pool = ConnectionPool(conninfo=dsn, min_size=1, max_size=pool_size, open=True, kwargs={"autocommit": False})

    def connection(self) -> Any:
        conn = self._pool.getconn()
        conn.execute(f"SET statement_timeout = {int(self._statement_timeout_ms)}")
        return conn

    def release(self, conn: Any) -> None:  # pragma: no cover - requires a live server
        self._pool.putconn(conn)

    def begin(self, conn: Any, *, immediate: bool = False) -> None:  # pragma: no cover - requires a live server
        # Postgres has no BEGIN IMMEDIATE; row and row-version locks already give us
        # the mutual exclusion that SQLite needs a write lock for.
        if not self.in_transaction(conn):
            conn.execute("BEGIN")

    def commit(self, conn: Any) -> None:  # pragma: no cover - requires a live server
        conn.commit()

    def rollback(self, conn: Any) -> None:  # pragma: no cover - requires a live server
        conn.rollback()

    def in_transaction(self, conn: Any) -> bool:  # pragma: no cover - requires a live server
        from psycopg.pq import TransactionStatus  # type: ignore[import-not-found]

        return conn.info.transaction_status in {
            TransactionStatus.INTRANS,
            TransactionStatus.INERROR,
            TransactionStatus.ACTIVE,
        }

    def close(self) -> None:  # pragma: no cover - requires a live server
        self._pool.close()


def _to_dialect_sql(sql: str, dialect: Dialect) -> str:
    if dialect is Dialect.SQLITE:
        return sql
    # Repository SQL is written with ? placeholders and SQLite-friendly booleans.
    translated = re.sub(r"\?", "%s", sql)
    translated = translated.replace("TRUE", "1").replace("FALSE", "0")
    return translated


class Database:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        parsed = DatabaseUrl.parse(settings.database_url)
        self.dialect = parsed.dialect
        self._backend: _Backend
        if parsed.dialect is Dialect.SQLITE:
            self._backend = SqliteBackend(parsed.path or ":memory:", statement_timeout_ms=settings.db_statement_timeout_ms)
        else:
            self._backend = PostgresBackend(  # pragma: no cover - requires a live server
                parsed.dsn or "", pool_size=settings.db_pool_size, statement_timeout_ms=settings.db_statement_timeout_ms
            )
        self._write_lock = threading.RLock() if parsed.dialect is Dialect.SQLITE else None
        self._migrated = False
        self._closed = False

    @property
    def is_sqlite(self) -> bool:
        return self.dialect is Dialect.SQLITE

    @contextmanager
    def transaction(self, *, immediate: bool = False, tenant_id: str | None = None) -> Iterator[Tx]:
        """Unit of work. `immediate` takes the write lock up front (SQLite), which is
        how read-modify-write paths avoid SQLITE_BUSY deadlocks under contention."""
        if self._closed:
            raise PlatformError("database is closed")
        conn = self._backend.connection()
        tx = _Tx(conn, self.dialect, self._write_lock)
        try:
            self._backend.begin(conn, immediate=immediate)
            if tenant_id is not None:
                tx.set_tenant(tenant_id)
            yield tx
            self._backend.commit(conn)
        except Exception:
            try:
                self._backend.rollback(conn)
            except Exception:  # pragma: no cover - connection already broken
                pass
            raise
        finally:
            release = getattr(self._backend, "release", None)
            if release is not None:  # pragma: no cover - postgres pool only
                release(conn)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        with self.transaction(immediate=True) as tx:
            return tx.execute(sql, params)

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        conn = self._backend.connection()
        cursor = conn.execute(_to_dialect_sql(sql, self.dialect), tuple(params or ()))
        return [dict(row) for row in cursor.fetchall()]

    def ping(self) -> bool:
        try:
            self.query("SELECT 1 AS ok")
            return True
        except Exception:
            return False

    def migrate(self, *, force: bool = False) -> list[str]:
        """Apply pending migrations; returns the names applied."""
        conn = self._backend.connection()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                name TEXT PRIMARY KEY,
                checksum TEXT NOT NULL,
                applied_at_ms INTEGER NOT NULL
            )
            """
        )
        if self.dialect is Dialect.POSTGRES:  # pragma: no cover - requires a live server
            conn.execute("SELECT pg_advisory_lock(hashtext('plexus_migrations'))")
        self._backend.commit(conn)

        applied = {row["name"]: row["checksum"] for row in self.query("SELECT name, checksum FROM schema_migrations")}
        done: list[str] = []
        for name, sql, checksum in self._migration_files():
            if name in applied:
                if applied[name] != checksum and not force:
                    raise PlatformError(
                        f"migration {name} changed after being applied (checksum drift)",
                        details={"applied": applied[name], "current": checksum},
                    )
                continue
            self._backend.begin(conn, immediate=True)
            try:
                for statement in _split_statements(sql):
                    conn.execute(statement)
                conn.execute(
                    _to_dialect_sql(
                        "INSERT INTO schema_migrations (name, checksum, applied_at_ms) VALUES (?, ?, ?)",
                        self.dialect,
                    ),
                    (name, checksum, int(time.time() * 1000)),
                )
                self._backend.commit(conn)
                done.append(name)
            except Exception:
                self._backend.rollback(conn)
                raise
        if self.dialect is Dialect.POSTGRES:  # pragma: no cover - requires a live server
            conn.execute("SELECT pg_advisory_unlock(hashtext('plexus_migrations'))")
            self._backend.commit(conn)
        self._migrated = True
        return done

    def _migration_files(self) -> list[tuple[str, str, str]]:
        prefix = "sqlite" if self.is_sqlite else "postgres"
        base = Path(str(resources.files("plexus.store"))) / "migrations" / prefix
        out: list[tuple[str, str, str]] = []
        for path in sorted(base.glob("*.sql")):
            sql = path.read_text(encoding="utf-8")
            out.append((path.name, sql, hashlib.sha256(sql.encode()).hexdigest()[:16]))
        if not out:
            raise PlatformError(f"no migrations found for dialect {prefix} at {base}")
        return out

    def close(self) -> None:
        self._closed = True
        self._backend.close()


class _Tx:
    def __init__(self, conn: Any, dialect: Dialect, write_lock: threading.RLock | None) -> None:
        self._conn = conn
        self._dialect = dialect
        self._write_lock = write_lock
        self.outbox: list[dict[str, Any]] = []

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        statement = _to_dialect_sql(sql, self._dialect)
        if self._write_lock is not None and _is_write(statement):
            with self._write_lock:
                return self._conn.execute(statement, tuple(params or ()))
        return self._conn.execute(statement, tuple(params or ()))

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        cursor = self.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def scalar(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        row = self.query_one(sql, params)
        return next(iter(row.values())) if row else None

    def set_tenant(self, tenant_id: str) -> None:
        """Bind the session tenant so Postgres RLS policies can enforce isolation."""
        self.set_local("app.tenant_id", tenant_id)

    def set_worker_scope(self, enabled: bool) -> None:
        """Widen RLS to queue tables for cross-tenant workers (see 0003_worker_scope)."""
        if enabled:
            self.set_local("app.worker_scope", "true")

    def set_local(self, key: str, value: str) -> None:
        if self._dialect is Dialect.POSTGRES:  # pragma: no cover - requires a live server
            self._conn.execute(f"SET LOCAL {key} = %s", (value,))

    def emit(self, event: Mapping[str, Any]) -> None:
        """Queue an outbox event; delivered by UnitOfWork after commit."""
        self.outbox.append(dict(event))


def _is_write(statement: str) -> bool:
    head = statement.strip().split(None, 1)[0].upper() if statement.strip() else ""
    return head in {"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "REPLACE"}


def _split_statements(sql: str) -> Iterator[str]:
    buffer: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(buffer).strip().rstrip(";")
            if statement:
                yield statement
            buffer = []
    tail = "\n".join(buffer).strip().rstrip(";")
    if tail:
        yield tail


def migration_names(dialect: Dialect) -> list[str]:
    prefix = "sqlite" if dialect is Dialect.SQLITE else "postgres"
    base = Path(str(resources.files("plexus.store"))) / "migrations" / prefix
    return sorted(p.name for p in base.glob("*.sql"))
