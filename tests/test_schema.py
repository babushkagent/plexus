"""Schema invariants that only bite in production.

SQLite is forgiving in ways Postgres is not: its INTEGER is always 64-bit and it has no
real column types. These checks keep the two dialects logically identical and keep
narrow integer columns away from millisecond epochs, where Postgres answers with
"integer out of range" on the very first write.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from plexus.store.db import SCHEMA_MIGRATIONS_DDL, Dialect

MIGRATIONS = Path(__file__).resolve().parents[1] / "src" / "plexus" / "store" / "migrations"


def _sql(dialect: Dialect) -> str:
    return "\n".join(path.read_text() for path in sorted((MIGRATIONS / dialect.value).glob("*.sql")))


def _tables(sql: str) -> set[str]:
    return set(re.findall(r"CREATE TABLE(?: IF NOT EXISTS)? (\w+)", sql))


def test_both_dialects_declare_the_same_tables() -> None:
    postgres = _tables(_sql(Dialect.POSTGRES))
    sqlite = _tables(_sql(Dialect.SQLITE))
    assert postgres == sqlite
    assert {"tenants", "tasks", "api_keys"} <= postgres


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(_sql(Dialect.POSTGRES), id="postgres-migrations"),
        pytest.param(SCHEMA_MIGRATIONS_DDL, id="schema-migrations-table"),
    ],
)
def test_postgres_epoch_columns_are_64_bit(sql: str) -> None:
    # Postgres INTEGER tops out at 2_147_483_647; a millisecond epoch is ~1.7e12.
    narrow = re.findall(r"^\s*(\w*_(?:at|after)_ms)\s+(?!BIGINT)(\w+)", sql, re.MULTILINE)
    assert narrow == []


def test_migrations_are_forward_only() -> None:
    for dialect in Dialect:
        assert "DROP TABLE" not in _sql(dialect).upper()


def test_schema_migrations_table_is_created_idempotently() -> None:
    # `migrate()` runs this on every boot, and N pods race to boot at the same time.
    assert "CREATE TABLE IF NOT EXISTS schema_migrations" in SCHEMA_MIGRATIONS_DDL
