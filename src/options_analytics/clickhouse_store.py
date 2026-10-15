"""
ClickHouse access.

The rest of the code only uses three operations (command, insert, query), so
they're described as a Protocol. The real implementation talks to a ClickHouse
server over HTTP with clickhouse-connect. The tests plug in chdb (ClickHouse
embedded in Python) behind the same interface, so every SQL query in this repo
is tested against a real ClickHouse engine in CI without running a server.

Queries use ClickHouse's server-side parameters, written {name:Type}. Values
are sent separately from the SQL, so user input can never change the query
(no SQL injection).
"""

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

import clickhouse_connect

from options_analytics.config import Settings


class ClickHouseLike(Protocol):
    def command(self, sql: str) -> None: ...

    def insert(self, table: str, rows: Sequence[Sequence[Any]], columns: Sequence[str]) -> None: ...

    def query(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]: ...


class ClickHouseStore:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            # Without this, concurrent API requests share one server session
            # and ClickHouse rejects the second query.
            autogenerate_session_id=False,
        )

    @classmethod
    def from_settings(cls, settings: Settings, database: str | None = None) -> "ClickHouseStore":
        return cls(
            settings.clickhouse_host,
            settings.clickhouse_port,
            settings.clickhouse_user,
            settings.clickhouse_password,
            database or settings.clickhouse_database,
        )

    def command(self, sql: str) -> None:
        self.client.command(sql)

    def insert(self, table: str, rows: Sequence[Sequence[Any]], columns: Sequence[str]) -> None:
        self.client.insert(table, rows, column_names=list(columns))

    def query(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        result = self.client.query(sql, parameters=dict(params or {}))
        return list(result.named_results())

    def ping(self) -> bool:
        return bool(self.client.ping())

    def close(self) -> None:
        self.client.close()
