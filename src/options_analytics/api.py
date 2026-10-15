"""
FastAPI service over the stored data.  `uvicorn options_analytics.api:app`

Open http://localhost:8000/docs for interactive documentation of every
endpoint, and http://localhost:8000 for the live dashboard.

Endpoints are plain `def` functions: the database drivers are synchronous, and
FastAPI runs sync endpoints in a thread pool so they don't block each other.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date, datetime
from importlib import resources
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from options_analytics import __version__, queries
from options_analytics.clickhouse_store import ClickHouseLike, ClickHouseStore
from options_analytics.config import Settings, get_settings
from options_analytics.instruments import Instrument
from options_analytics.postgres_store import PostgresStore

# --- Response models (these also generate the /docs page) -----------------


class Health(BaseModel):
    status: str
    clickhouse: bool
    postgres: bool


class Expiry(BaseModel):
    expiry: datetime
    instruments: int
    min_strike: float
    max_strike: float


class InstrumentOut(BaseModel):
    instrument_name: str
    option_type: str
    strike: float
    expiry: datetime
    tick_size: float
    is_active: bool


class SmilePoint(BaseModel):
    instrument_name: str
    strike: float
    option_type: str
    forward: float
    iv_mark: float | None
    iv_mid: float | None
    deribit_mark_iv: float | None
    delta: float | None
    as_of: datetime


class TermPoint(BaseModel):
    expiry: datetime
    days_to_expiry: float
    forward: float
    atm_strike: float
    atm_iv: float | None


class LatestTick(BaseModel):
    instrument_name: str
    exchange_ts: datetime
    time_to_expiry: float
    underlying_price: float
    index_price: float
    bid_price: float | None
    ask_price: float | None
    mark_price: float
    iv_mark: float | None
    iv_mid: float | None
    delta: float | None
    gamma: float | None
    vega: float | None
    theta: float | None
    deribit_mark_iv: float | None
    deribit_bid_iv: float | None
    deribit_ask_iv: float | None
    deribit_delta: float | None
    deribit_gamma: float | None
    deribit_vega: float | None
    deribit_theta: float | None


class IvBar(BaseModel):
    minute: datetime
    iv_open: float
    iv_high: float
    iv_low: float
    iv_close: float
    underlying_close: float
    ticks: int


class VolPoint(BaseModel):
    minute: datetime
    realised_vol: float | None
    atm_iv: float | None


class Trade(BaseModel):
    trade_ts: datetime
    trade_id: str
    instrument_name: str
    expiry: datetime
    direction: str
    amount: float
    price: float
    bid_price: float | None
    ask_price: float | None
    mark_price: float | None
    trade_iv: float | None
    mark_iv: float | None
    delta: float | None
    delta_traded: float | None
    quote_age_ms: int | None


class FlowRow(BaseModel):
    expiry: datetime
    trades: int
    contracts: float
    bought: float
    sold: float
    net_delta: float | None
    trades_without_quote: int


class Latency(BaseModel):
    ticks: int
    ticks_per_second: float
    exchange_to_ingest_ms: list[float]
    ingest_to_process_ms: list[float]
    process_to_insert_ms: list[float]


class ModelCheckRow(BaseModel):
    bucket: str
    ticks: int
    solver_failures: int
    mean_abs_iv_diff_pts: float | None
    p99_abs_iv_diff_pts: float | None
    mean_abs_delta_diff: float | None


class TableSize(BaseModel):
    table: str
    rows: int
    compressed_bytes: int
    uncompressed_bytes: int


# --- App setup ---------------------------------------------------------------


def create_app(
    ch: ClickHouseLike | None = None,
    pg: PostgresStore | None = None,
    settings: Settings | None = None,
) -> FastAPI:
    """Build the app. Tests pass in their own stores; otherwise they're created from settings."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        s = settings or get_settings()
        app.state.settings = s
        app.state.ch = ch or ClickHouseStore.from_settings(s)
        app.state.pg = pg or PostgresStore(s.postgres_dsn)
        yield
        if pg is None:
            app.state.pg.close()

    app = FastAPI(
        title="Options Analytics API",
        version=__version__,
        description="Implied volatility, Greeks and trade flow from live Deribit options data.",
        lifespan=lifespan,
    )
    _add_routes(app)
    return app


def get_ch(request: Request) -> ClickHouseLike:
    store: ClickHouseLike = request.app.state.ch
    return store


def get_pg(request: Request) -> PostgresStore:
    store: PostgresStore = request.app.state.pg
    return store


def get_currency(request: Request) -> str:
    currency: str = request.app.state.settings.currency
    return currency


CH = Annotated[ClickHouseLike, Depends(get_ch)]
PG = Annotated[PostgresStore, Depends(get_pg)]
Currency = Annotated[str, Depends(get_currency)]
Minutes = Annotated[int, Query(ge=1, le=1440, description="Look-back window in minutes")]
Hours = Annotated[int, Query(ge=1, le=72, description="Look-back window in hours")]


def _instrument_or_404(pg: PostgresStore, name: str) -> Instrument:
    inst = pg.get_instrument(name)
    if inst is None:
        raise HTTPException(status_code=404, detail=f"unknown instrument {name}")
    return inst


def _instrument_params(inst: Instrument) -> dict[str, Any]:
    """Filter values covering the whole ORDER BY prefix, so ClickHouse can use its index."""
    return {
        "currency": inst.currency,
        "expiry_s": inst.expiry_ms // 1000,
        "strike": inst.strike,
        "option_type": inst.option_type,
    }


def _add_routes(app: FastAPI) -> None:
    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def dashboard() -> str:
        return (resources.files("options_analytics") / "static" / "index.html").read_text(encoding="utf-8")

    @app.get("/health", response_model=Health, tags=["meta"])
    def health(ch: CH, pg: PG) -> Health:
        def ok(check: Any) -> bool:
            try:
                check()
                return True
            except Exception:
                return False

        ch_ok = ok(lambda: ch.query("SELECT 1"))
        pg_ok = ok(pg.ping)
        return Health(status="ok" if ch_ok and pg_ok else "degraded", clickhouse=ch_ok, postgres=pg_ok)

    # --- Reference data (Postgres) -----------------------------------------

    @app.get("/expiries", response_model=list[Expiry], tags=["reference data"])
    def expiries(pg: PG, currency: Currency) -> list[dict[str, Any]]:
        return pg.list_expiries(currency)

    @app.get("/instruments", response_model=list[InstrumentOut], tags=["reference data"])
    def instruments(
        pg: PG, currency: Currency, expiry: date | None = None, option_type: str | None = None
    ) -> list[InstrumentOut]:
        return [
            InstrumentOut(
                instrument_name=i.instrument_name,
                option_type=i.option_type,
                strike=i.strike,
                expiry=i.expiry,
                tick_size=i.tick_size,
                is_active=i.is_active,
            )
            for i in pg.list_instruments(currency, expiry, option_type)
        ]

    # --- Volatility (ClickHouse) -------------------------------------------

    @app.get("/smile", response_model=list[SmilePoint], tags=["volatility"])
    def smile(ch: CH, currency: Currency, expiry: date, minutes: Minutes = 5) -> list[dict[str, Any]]:
        """Latest implied vol per strike for one expiry (out-of-the-money options)."""
        return ch.query(queries.SMILE, {"currency": currency, "expiry": expiry, "minutes": minutes})

    @app.get("/term-structure", response_model=list[TermPoint], tags=["volatility"])
    def term_structure(ch: CH, currency: Currency, minutes: Minutes = 5) -> list[dict[str, Any]]:
        """At-the-money implied vol for each expiry."""
        return ch.query(queries.TERM_STRUCTURE, {"currency": currency, "minutes": minutes})

    @app.get("/vol/realised-vs-implied", response_model=list[VolPoint], tags=["volatility"])
    def realised_vs_implied(
        ch: CH,
        currency: Currency,
        expiry: date,
        hours: Hours = 6,
        window: Annotated[int, Query(ge=5, le=1440, description="Rolling window in minutes")] = 60,
    ) -> list[dict[str, Any]]:
        """Rolling realised vol of the index vs ATM implied vol for one expiry, per minute."""
        sql = queries.REALISED_VS_IMPLIED.format(window=int(window) - 1, min_obs=min(int(window), 10))
        return ch.query(sql, {"currency": currency, "expiry": expiry, "hours": hours})

    @app.get("/instruments/{name}/latest", response_model=LatestTick, tags=["instrument"])
    def latest(name: str, ch: CH, pg: PG) -> dict[str, Any]:
        """Latest tick for one option: our IV and Greeks next to Deribit's."""
        rows = ch.query(queries.LATEST_TICK, _instrument_params(_instrument_or_404(pg, name)))
        if not rows:
            raise HTTPException(status_code=404, detail=f"no data for {name} in the last day")
        return rows[0]

    @app.get("/instruments/{name}/iv-history", response_model=list[IvBar], tags=["instrument"])
    def iv_history(name: str, ch: CH, pg: PG, hours: Hours = 6) -> list[dict[str, Any]]:
        """One-minute IV bars (open, high, low, close) from the materialised view."""
        params = _instrument_params(_instrument_or_404(pg, name)) | {"hours": hours}
        return ch.query(queries.IV_HISTORY, params)

    # --- Trades ------------------------------------------------------------

    @app.get("/trades/recent", response_model=list[Trade], tags=["trades"])
    def recent_trades(
        ch: CH,
        currency: Currency,
        minutes: Minutes = 60,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> list[dict[str, Any]]:
        """Recent trades, each matched to the quote just before it (ASOF JOIN)."""
        return ch.query(queries.RECENT_TRADES, {"currency": currency, "minutes": minutes, "limit": limit})

    @app.get("/trades/flow", response_model=list[FlowRow], tags=["trades"])
    def trade_flow(ch: CH, currency: Currency, minutes: Minutes = 60) -> list[dict[str, Any]]:
        """Net delta bought by takers, per expiry."""
        return ch.query(queries.TRADE_FLOW, {"currency": currency, "minutes": minutes})

    # --- Pipeline health ---------------------------------------------------

    @app.get("/pipeline/latency", response_model=Latency, tags=["pipeline"])
    def latency(ch: CH, currency: Currency, minutes: Minutes = 5) -> dict[str, Any]:
        """Throughput and p50/p90/p99 latency of each pipeline stage."""
        return ch.query(queries.LATENCY, {"currency": currency, "minutes": minutes})[0]

    @app.get("/pipeline/model-check", response_model=list[ModelCheckRow], tags=["pipeline"])
    def model_check(ch: CH, currency: Currency, minutes: Minutes = 5) -> list[dict[str, Any]]:
        """How closely our IV and delta match Deribit's, by moneyness."""
        return ch.query(queries.MODEL_CHECK, {"currency": currency, "minutes": minutes})

    @app.get("/pipeline/storage", response_model=list[TableSize], tags=["pipeline"])
    def storage(ch: CH) -> list[dict[str, Any]]:
        """Rows and compressed size of each ClickHouse table."""
        return ch.query(queries.TABLE_SIZES)


app = create_app()
