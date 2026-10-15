"""
Turn raw Deribit messages into rows for ClickHouse.

This is where the pricing maths meets the data: for every ticker update we
solve for implied vol from the mark price and from the bid/ask mid, and
compute our own Greeks. We keep Deribit's published IV and Greeks alongside,
so the API can report how closely the two agree (/pipeline/model-check).

The column order of TickRow and TradeRow is the column list used for inserts,
so it must match sql/clickhouse/*.sql.
"""

from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple

from options_analytics.instruments import Instrument
from options_analytics.pricing import black76_greeks, implied_vol, year_fraction

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def ms_to_dt(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)


def us_to_dt(us: int) -> datetime:
    return EPOCH + timedelta(microseconds=us)


def _positive(value: Any) -> float | None:
    """Deribit marks an empty side of the book with null (and sometimes 0)."""
    if value is None:
        return None
    number = float(value)
    return number if number > 0 else None


def _pct_to_decimal(value: Any) -> float | None:
    """Deribit quotes vol in percent (33.84) and uses 0 for 'none'."""
    number = _positive(value)
    return number / 100 if number is not None else None


def _solve_iv(
    price_usd: float, forward: float, inst: Instrument, t: float, guess: float | None
) -> float | None:
    try:
        return implied_vol(price_usd, forward, inst.strike, t, inst.is_call, guess=guess or 0.5)
    except (ValueError, RuntimeError):
        # Prices outside no-arbitrage bounds happen with stale or one-sided
        # quotes. Store NULL rather than a made-up number.
        return None


class TickRow(NamedTuple):
    instrument_name: str
    currency: str
    expiry: datetime
    strike: float
    option_type: str
    exchange_ts: datetime
    received_ts: datetime
    processed_ts: datetime
    time_to_expiry: float
    underlying_price: float
    index_price: float
    bid_price: float | None
    ask_price: float | None
    mark_price: float
    bid_amount: float
    ask_amount: float
    open_interest: float
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


class TradeRow(NamedTuple):
    trade_id: str
    trade_seq: int
    instrument_name: str
    currency: str
    expiry: datetime
    strike: float
    option_type: str
    trade_ts: datetime
    received_ts: datetime
    processed_ts: datetime
    price: float
    amount: float
    direction: str
    deribit_iv: float | None
    mark_price: float
    index_price: float
    tick_direction: int
    block_trade_id: str
    is_liquidation: int


TICK_COLUMNS = TickRow._fields
TRADE_COLUMNS = TradeRow._fields


def ticker_row(
    data: dict[str, Any],
    inst: Instrument,
    received_us: int,
    processed_us: int,
    iv_guess: float | None = None,
) -> TickRow | None:
    """One ticker notification -> one row, or None if it can't be priced."""
    exchange_ms = int(data["timestamp"])
    t = year_fraction(exchange_ms, inst.expiry_ms)
    forward = _positive(data.get("underlying_price"))
    mark = data.get("mark_price")
    if t <= 0 or forward is None or mark is None:
        return None
    mark = float(mark)
    bid = _positive(data.get("best_bid_price"))
    ask = _positive(data.get("best_ask_price"))

    # Deribit prices are in BTC: multiply by the forward to get USD.
    iv_mark = _solve_iv(mark * forward, forward, inst, t, iv_guess)
    iv_mid = None
    if bid is not None and ask is not None:
        iv_mid = _solve_iv((bid + ask) / 2 * forward, forward, inst, t, iv_mark or iv_guess)
    greeks = black76_greeks(forward, inst.strike, t, iv_mark, inst.is_call) if iv_mark else None
    deribit_greeks = data.get("greeks") or {}

    return TickRow(
        instrument_name=inst.instrument_name,
        currency=inst.currency,
        expiry=inst.expiry,
        strike=inst.strike,
        option_type=inst.option_type,
        exchange_ts=ms_to_dt(exchange_ms),
        received_ts=us_to_dt(received_us),
        processed_ts=us_to_dt(processed_us),
        time_to_expiry=t,
        underlying_price=forward,
        index_price=float(data.get("index_price") or 0.0),
        bid_price=bid,
        ask_price=ask,
        mark_price=mark,
        bid_amount=float(data.get("best_bid_amount") or 0.0),
        ask_amount=float(data.get("best_ask_amount") or 0.0),
        open_interest=float(data.get("open_interest") or 0.0),
        iv_mark=iv_mark,
        iv_mid=iv_mid,
        delta=greeks.delta if greeks else None,
        gamma=greeks.gamma if greeks else None,
        vega=greeks.vega if greeks else None,
        theta=greeks.theta if greeks else None,
        deribit_mark_iv=_pct_to_decimal(data.get("mark_iv")),
        deribit_bid_iv=_pct_to_decimal(data.get("bid_iv")),
        deribit_ask_iv=_pct_to_decimal(data.get("ask_iv")),
        deribit_delta=deribit_greeks.get("delta"),
        deribit_gamma=deribit_greeks.get("gamma"),
        deribit_vega=deribit_greeks.get("vega"),
        deribit_theta=deribit_greeks.get("theta"),
    )


def trade_row(data: dict[str, Any], inst: Instrument, received_us: int, processed_us: int) -> TradeRow:
    """One trade from the trades.option.{currency} channel -> one row."""
    return TradeRow(
        trade_id=str(data["trade_id"]),
        trade_seq=int(data["trade_seq"]),
        instrument_name=inst.instrument_name,
        currency=inst.currency,
        expiry=inst.expiry,
        strike=inst.strike,
        option_type=inst.option_type,
        trade_ts=ms_to_dt(int(data["timestamp"])),
        received_ts=us_to_dt(received_us),
        processed_ts=us_to_dt(processed_us),
        price=float(data["price"]),
        amount=float(data["amount"]),
        direction=data["direction"],
        deribit_iv=_pct_to_decimal(data.get("iv")),
        mark_price=float(data["mark_price"]),
        index_price=float(data["index_price"]),
        tick_direction=int(data.get("tick_direction", 0)),
        block_trade_id=str(data.get("block_trade_id") or ""),
        is_liquidation=1 if data.get("liquidation") else 0,
    )
