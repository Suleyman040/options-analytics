"""
Shared test fixtures.

`ch` is a ClickHouse store with the schema applied, filled with synthetic data:
- with RUN_CLICKHOUSE_TESTS=1, a real ClickHouse server (from docker compose),
  using a separate `options_test` database so your real data is untouched;
- otherwise chdb (embedded ClickHouse) when it's installed (Linux/macOS, CI);
- otherwise the tests that need it are skipped.
"""

import importlib.util
import json
import os
import sys
import time
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import synthetic

from options_analytics.clickhouse_store import ClickHouseLike, ClickHouseStore
from options_analytics.config import Settings
from options_analytics.instruments import Instrument, instrument_from_deribit
from options_analytics.migrate import migrate_clickhouse
from options_analytics.transform import TICK_COLUMNS, TRADE_COLUMNS, ticker_row, trade_row

FIXTURES = Path(__file__).parent / "fixtures"
TEST_DATABASE = "options_test"


def load_fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture(scope="session")
def now() -> Any:
    return synthetic.now_utc()


@pytest.fixture(scope="session")
def raw_instruments(now: Any) -> list[dict[str, Any]]:
    return synthetic.make_instruments(now)


@pytest.fixture(scope="session")
def instruments(raw_instruments: list[dict[str, Any]]) -> list[Instrument]:
    return [instrument_from_deribit(r) for r in raw_instruments]


def _make_store(tmp_path_factory: pytest.TempPathFactory) -> ClickHouseLike:
    if os.environ.get("RUN_CLICKHOUSE_TESTS"):
        settings = Settings()
        admin = ClickHouseStore.from_settings(settings, database="default")
        admin.command(f"DROP DATABASE IF EXISTS {TEST_DATABASE}")
        admin.command(f"CREATE DATABASE {TEST_DATABASE}")
        return ClickHouseStore.from_settings(settings, database=TEST_DATABASE)
    if importlib.util.find_spec("chdb") is None:
        pytest.skip("needs chdb (Linux/macOS) or RUN_CLICKHOUSE_TESTS=1 with ClickHouse running")
    from chdb_store import ChdbStore

    return ChdbStore(str(tmp_path_factory.mktemp("chdb")), database=TEST_DATABASE)


@pytest.fixture(scope="session")
def ch(
    tmp_path_factory: pytest.TempPathFactory, now: Any, raw_instruments: list[dict[str, Any]]
) -> Iterator[ClickHouseLike]:
    store = _make_store(tmp_path_factory)
    migrate_clickhouse(store)

    by_name = {r["instrument_name"]: instrument_from_deribit(r) for r in raw_instruments}
    tickers, trades = synthetic.history(raw_instruments, now)
    processed_us = int(time.time() * 1e6)
    tick_rows = []
    for received_us, data in tickers:
        row = ticker_row(data, by_name[data["instrument_name"]], received_us, received_us + 2_000)
        assert row is not None
        tick_rows.append(row)
    trade_rows = [
        trade_row(data, by_name[data["instrument_name"]], received_us, processed_us)
        for received_us, data in trades
    ]
    # One trade in an option we never received quotes for: the ASOF JOIN finds no match.
    far_expiry_ms = max(i.expiry_ms for i in by_name.values())
    unquoted = Instrument("BTC-UNQUOTED-150000-C", "BTC", "call", 150_000.0, far_expiry_ms, 0.0001, 1.0, 0.1)
    last_trade = trades[-1][1]
    trade_rows.append(
        trade_row(
            {**last_trade, "trade_id": "BTC-unquoted", "instrument_name": unquoted.instrument_name},
            unquoted,
            trades[-1][0],
            processed_us,
        )
    )
    # Insert in batches, as the processor would.
    for i in range(0, len(tick_rows), 2_000):
        store.insert("option_ticks", tick_rows[i : i + 2_000], TICK_COLUMNS)
    store.insert("option_trades", trade_rows, TRADE_COLUMNS)
    yield store


class FakePostgres:
    """Stands in for PostgresStore in API tests: reference data from memory."""

    def __init__(self, instruments: list[Instrument]):
        self.by_name = {i.instrument_name: i for i in instruments}

    def ping(self) -> bool:
        return True

    def get_instrument(self, name: str) -> Instrument | None:
        return self.by_name.get(name)

    def list_instruments(
        self, currency: str, expiry: date | None = None, option_type: str | None = None
    ) -> list[Instrument]:
        return [
            i
            for i in self.by_name.values()
            if i.currency == currency
            and (expiry is None or i.expiry.date() == expiry)
            and (option_type is None or i.option_type == option_type)
        ]

    def list_expiries(self, currency: str) -> list[dict[str, Any]]:
        out: dict[Any, list[Instrument]] = {}
        for i in self.by_name.values():
            out.setdefault(i.expiry, []).append(i)
        return [
            {
                "expiry": e,
                "instruments": len(v),
                "min_strike": min(i.strike for i in v),
                "max_strike": max(i.strike for i in v),
            }
            for e, v in sorted(out.items())
        ]
