"""
Create the database schemas: `python -m options_analytics.migrate`.

Every statement is idempotent (CREATE ... IF NOT EXISTS), so this is safe to
run on every start. Files run in name order. A larger project would use a
migration tool that records which versions have run (Alembic for Postgres,
for example), which also handles changing existing tables.
"""

import logging
import time
from collections.abc import Callable
from importlib import resources

import psycopg

from options_analytics.clickhouse_store import ClickHouseLike, ClickHouseStore
from options_analytics.config import Settings, get_settings
from options_analytics.logs import setup_logging

log = logging.getLogger(__name__)


def sql_files(engine: str) -> list[tuple[str, str]]:
    """(file name, contents) for every .sql file under sql/<engine>/, in order."""
    folder = resources.files("options_analytics") / "sql" / engine
    files = sorted((f for f in folder.iterdir() if f.name.endswith(".sql")), key=lambda f: f.name)
    return [(f.name, f.read_text(encoding="utf-8")) for f in files]


def split_statements(sql: str) -> list[str]:
    """Split a file into statements on ';'. Whole-line comments are removed first."""
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def migrate_clickhouse(store: ClickHouseLike) -> None:
    for name, sql in sql_files("clickhouse"):
        for statement in split_statements(sql):
            store.command(statement)
        log.info("clickhouse: applied %s", name)


def migrate_postgres(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        for name, sql in sql_files("postgres"):
            conn.execute(sql)  # psycopg runs multi-statement SQL when there are no parameters
            log.info("postgres: applied %s", name)


def retry[T](action: Callable[[], T], what: str, attempts: int = 30, delay_s: float = 2.0) -> T:
    """Databases can take a few seconds to accept connections after `docker compose up`."""
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as exc:
            if attempt == attempts:
                raise
            log.info("waiting for %s (%d/%d): %s", what, attempt, attempts, exc)
            time.sleep(delay_s)
    raise AssertionError("unreachable")


def run(settings: Settings) -> None:
    def clickhouse() -> None:
        # Connect to the default database first: ours may not exist yet.
        bootstrap = ClickHouseStore.from_settings(settings, database="default")
        bootstrap.command(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_database}")
        bootstrap.close()
        migrate_clickhouse(ClickHouseStore.from_settings(settings))

    retry(clickhouse, "ClickHouse")
    retry(lambda: migrate_postgres(settings.postgres_dsn), "Postgres")
    log.info("migrations complete")


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    run(settings)


if __name__ == "__main__":
    main()
