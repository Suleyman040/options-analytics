"""
Reference data in Postgres. Needs a running Postgres:

    POSTGRES_TEST_DSN=postgresql://app:app@localhost:5432/options pytest -m integration

The tests work in a separate schema, so they never touch your real tables.
"""

import os
from collections.abc import Iterator
from dataclasses import replace

import psycopg
import pytest
from psycopg import sql

from options_analytics.instruments import Instrument
from options_analytics.migrate import migrate_postgres
from options_analytics.postgres_store import PostgresStore

pytestmark = pytest.mark.integration
DSN = os.environ.get("POSTGRES_TEST_DSN")
SCHEMA = "options_test"


@pytest.fixture
def pg() -> Iterator[PostgresStore]:
    if not DSN:
        pytest.skip("set POSTGRES_TEST_DSN to run Postgres tests")
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(SCHEMA)))
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(SCHEMA)))
    dsn = f"{DSN}{'&' if '?' in DSN else '?'}options=-csearch_path%3D{SCHEMA}"
    migrate_postgres(dsn)
    store = PostgresStore(dsn)
    yield store
    store.close()


def option(strike: float, option_type: str = "call", expiry_ms: int = 4_102_473_600_000) -> Instrument:
    name = f"BTC-1JAN00-{int(strike)}-{option_type[0].upper()}"
    return Instrument(name, "BTC", option_type, strike, expiry_ms, 0.0001, 1.0, 0.1)  # type: ignore[arg-type]


def test_upsert_is_idempotent_and_detects_changes(pg: PostgresStore) -> None:
    chain = [option(80_000), option(80_000, "put"), option(90_000)]
    first = pg.upsert_instruments(chain)
    assert (first.inserted, first.updated) == (3, 0)

    again = pg.upsert_instruments(chain)
    assert (again.inserted, again.updated) == (0, 0)  # unchanged rows aren't rewritten

    changed = [replace(chain[0], tick_size=0.0005), *chain[1:], option(100_000)]
    result = pg.upsert_instruments(changed)
    assert (result.inserted, result.updated) == (1, 1)
    assert pg.get_instrument(chain[0].instrument_name).tick_size == 0.0005  # type: ignore[union-attr]


def test_missing_instruments_are_deactivated_not_deleted(pg: PostgresStore) -> None:
    chain = [option(80_000), option(90_000)]
    pg.upsert_instruments(chain)
    assert pg.deactivate_missing("BTC", [chain[1].instrument_name]) == 1
    gone = pg.get_instrument(chain[0].instrument_name)
    assert gone is not None and not gone.is_active
    assert [i.instrument_name for i in pg.list_instruments("BTC")] == [chain[1].instrument_name]


def test_constraints_reject_bad_data(pg: PostgresStore) -> None:
    with pytest.raises(psycopg.errors.CheckViolation):
        pg.upsert_instruments([option(-1)])


def test_reads_round_trip(pg: PostgresStore) -> None:
    chain = [option(80_000), option(90_000, "put")]
    pg.upsert_instruments(chain)
    assert sorted(pg.load_instruments("BTC"), key=lambda i: i.strike) == chain
    expiries = pg.list_expiries("BTC")
    assert len(expiries) == 1
    assert (expiries[0]["instruments"], expiries[0]["min_strike"], expiries[0]["max_strike"]) == (
        2,
        80_000,
        90_000,
    )
