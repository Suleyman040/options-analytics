"""
A ClickHouseLike store backed by chdb: ClickHouse embedded in Python.

It runs the same SQL engine as a ClickHouse server, so tests can check every
query in queries.py and every migration without Docker. chdb has no Windows
build; on Windows these tests are skipped (run them against Docker instead,
see the README).
"""

import json
import threading
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, date):
        return value.isoformat()
    return value


class ChdbStore:
    def __init__(self, path: str, database: str = "options_test"):
        from chdb import session

        self.session = session.Session(path)
        self.database = database
        # One embedded session can't run queries concurrently (a ClickHouse server can),
        # and FastAPI serves requests from a thread pool.
        self._lock = threading.Lock()
        self.session.query(f"CREATE DATABASE IF NOT EXISTS {database}")

    def _sql(self, sql: str) -> str:
        # Settings make output match what clickhouse-connect returns, and accept
        # timestamps with fractional seconds on insert.
        return (
            f"USE {self.database}; SET output_format_json_quote_64bit_integers = 0; "
            f"SET date_time_input_format = 'best_effort'; {sql}"
        )

    def command(self, sql: str) -> None:
        with self._lock:
            self.session.query(self._sql(sql))

    def insert(self, table: str, rows: Sequence[Sequence[Any]], columns: Sequence[str]) -> None:
        lines = "\n".join(
            json.dumps({c: _json_value(v) for c, v in zip(columns, row, strict=True)}) for row in rows
        )
        with self._lock:
            self.session.query(
                self._sql(f"INSERT INTO {table} ({', '.join(columns)}) FORMAT JSONEachRow\n{lines}")
            )

    def query(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        converted = {k: _json_value(v) for k, v in (params or {}).items()}
        with self._lock:
            result = self.session.query(self._sql(sql), "JSONEachRow", params=converted)
        return [json.loads(line) for line in result.bytes().decode().splitlines() if line.strip()]

    def close(self) -> None:
        self.session.close()
