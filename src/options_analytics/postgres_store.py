"""
Postgres access for reference data (which options exist, their strikes and expiries).

Postgres suits this data: it's small, relational, changes slowly and benefits
from constraints (a strike must be positive, option_type must be call or put).
High-volume market data goes to ClickHouse instead.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from options_analytics.instruments import Instrument

INSTRUMENT_COLUMNS = """
    instrument_name, currency, option_type, strike::float8 AS strike,
    (extract(epoch FROM expiry) * 1000)::bigint AS expiry_ms,
    tick_size::float8 AS tick_size, contract_size::float8 AS contract_size,
    min_trade_amount::float8 AS min_trade_amount, is_active
"""

# One statement upserts every instrument: unnest() turns the arrays into rows.
# The WHERE on DO UPDATE skips rows that haven't changed, and xmax = 0 is true
# only for freshly inserted rows, so we can count inserts vs updates.
UPSERT_SQL = """
INSERT INTO instruments (
    instrument_name, currency, option_type, strike, expiry,
    tick_size, contract_size, min_trade_amount, is_active
)
SELECT * FROM unnest(
    %(names)s::text[], %(currencies)s::text[], %(types)s::text[], %(strikes)s::numeric[],
    %(expiries)s::timestamptz[], %(ticks)s::numeric[], %(contract_sizes)s::numeric[],
    %(min_amounts)s::numeric[], %(active)s::boolean[]
)
ON CONFLICT (instrument_name) DO UPDATE SET
    is_active = EXCLUDED.is_active,
    tick_size = EXCLUDED.tick_size,
    contract_size = EXCLUDED.contract_size,
    min_trade_amount = EXCLUDED.min_trade_amount,
    updated_at = now()
WHERE (instruments.is_active, instruments.tick_size, instruments.contract_size,
       instruments.min_trade_amount)
      IS DISTINCT FROM
      (EXCLUDED.is_active, EXCLUDED.tick_size, EXCLUDED.contract_size, EXCLUDED.min_trade_amount)
RETURNING (xmax = 0) AS inserted
"""

DEACTIVATE_SQL = """
UPDATE instruments
SET is_active = false, updated_at = now()
WHERE currency = %(currency)s AND is_active AND NOT (instrument_name = ANY(%(names)s))
"""


def _configure(conn: Connection[Any]) -> None:
    """Return timestamps in UTC whatever the server's own time zone is."""
    conn.execute("SET TIME ZONE 'UTC'")
    conn.commit()


@dataclass(frozen=True)
class UpsertResult:
    inserted: int
    updated: int


def _instrument(row: dict[str, Any]) -> Instrument:
    return Instrument(**row)


class PostgresStore:
    def __init__(self, dsn: str, max_size: int = 5, connect_timeout_s: float = 10):
        self.pool = ConnectionPool(dsn, min_size=1, max_size=max_size, configure=_configure, open=True)
        try:
            self.pool.wait(timeout=connect_timeout_s)  # fail fast if Postgres isn't up
        except Exception:
            self.pool.close()
            raise

    def close(self) -> None:
        self.pool.close()

    def ping(self) -> bool:
        with self.pool.connection() as conn:
            conn.execute("SELECT 1")
        return True

    # --- Writes (reference job) ---------------------------------------------

    def upsert_instruments(self, instruments: list[Instrument]) -> UpsertResult:
        params = {
            "names": [i.instrument_name for i in instruments],
            "currencies": [i.currency for i in instruments],
            "types": [i.option_type for i in instruments],
            "strikes": [i.strike for i in instruments],
            "expiries": [i.expiry for i in instruments],
            "ticks": [i.tick_size for i in instruments],
            "contract_sizes": [i.contract_size for i in instruments],
            "min_amounts": [i.min_trade_amount for i in instruments],
            "active": [i.is_active for i in instruments],
        }
        with self.pool.connection() as conn:
            rows = conn.execute(UPSERT_SQL, params).fetchall()
        inserted = sum(1 for (is_insert,) in rows if is_insert)
        return UpsertResult(inserted=inserted, updated=len(rows) - inserted)

    def deactivate_missing(self, currency: str, active_names: list[str]) -> int:
        with self.pool.connection() as conn:
            return conn.execute(DEACTIVATE_SQL, {"currency": currency, "names": active_names}).rowcount

    def record_refresh(
        self, currency: str, started_at: datetime, seen: int, result: UpsertResult, deactivated: int
    ) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO reference_refreshes
                    (currency, started_at, finished_at, instruments_seen, inserted, updated, deactivated)
                VALUES (%s, %s, now(), %s, %s, %s, %s)
                """,
                (currency, started_at, seen, result.inserted, result.updated, deactivated),
            )

    # --- Reads (processor and API) ------------------------------------------

    def load_instruments(
        self, currency: str, expired_within: timedelta = timedelta(days=2)
    ) -> list[Instrument]:
        """Every instrument that is live or expired recently (late messages still need it)."""
        since = datetime.now(UTC) - expired_within
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT {INSTRUMENT_COLUMNS} FROM instruments WHERE currency = %s AND expiry > %s",
                (currency, since),
            )
            return [_instrument(row) for row in cur.fetchall()]

    def get_instrument(self, name: str) -> Instrument | None:
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT {INSTRUMENT_COLUMNS} FROM instruments WHERE instrument_name = %s", (name,))
            row = cur.fetchone()
        return _instrument(row) if row else None

    def list_instruments(
        self, currency: str, expiry: date | None = None, option_type: str | None = None
    ) -> list[Instrument]:
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT {INSTRUMENT_COLUMNS} FROM instruments
                WHERE currency = %(currency)s AND is_active
                  AND (%(expiry)s::date IS NULL OR (expiry AT TIME ZONE 'UTC')::date = %(expiry)s::date)
                  AND (%(option_type)s::text IS NULL OR option_type = %(option_type)s::text)
                ORDER BY expiry, strike, option_type
                """,
                {"currency": currency, "expiry": expiry, "option_type": option_type},
            )
            return [_instrument(row) for row in cur.fetchall()]

    def list_expiries(self, currency: str) -> list[dict[str, Any]]:
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT expiry,
                       count(*) AS instruments,
                       min(strike)::float8 AS min_strike,
                       max(strike)::float8 AS max_strike
                FROM instruments
                WHERE currency = %s AND is_active AND expiry > now()
                GROUP BY expiry
                ORDER BY expiry
                """,
                (currency,),
            )
            return cur.fetchall()
