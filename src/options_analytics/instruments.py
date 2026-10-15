"""
Option reference data: what an instrument is, and which ones to stream.

Reference data is the slow-changing facts about each option (strike, expiry,
call/put, tick size). It lives in Postgres; market data lives in ClickHouse.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

OptionType = Literal["call", "put"]


@dataclass(frozen=True, slots=True)
class Instrument:
    instrument_name: str  # e.g. BTC-30OCT26-87000-C
    currency: str  # BTC
    option_type: OptionType
    strike: float  # USD
    expiry_ms: int  # Unix ms; Deribit options expire at 08:00 UTC
    tick_size: float  # minimum price increment, in BTC
    contract_size: float
    min_trade_amount: float
    is_active: bool = True

    @property
    def is_call(self) -> bool:
        return self.option_type == "call"

    @property
    def expiry(self) -> datetime:
        return datetime.fromtimestamp(self.expiry_ms / 1000, tz=UTC)


def instrument_from_deribit(raw: Mapping[str, Any]) -> Instrument:
    """Build an Instrument from one item of Deribit's public/get_instruments.

    Deribit's docs recommend using these fields rather than parsing the name.
    """
    return Instrument(
        instrument_name=raw["instrument_name"],
        currency=raw["base_currency"],
        option_type=raw["option_type"],
        strike=float(raw["strike"]),
        expiry_ms=int(raw["expiration_timestamp"]),
        tick_size=float(raw["tick_size"]),
        contract_size=float(raw.get("contract_size", 1.0)),
        min_trade_amount=float(raw.get("min_trade_amount", 0.0)),
        is_active=bool(raw.get("is_active", True)),
    )


def select_instruments(
    instruments: Iterable[Instrument],
    index_price: float,
    now_ms: int,
    max_expiries: int,
    strike_range: float,
) -> list[Instrument]:
    """
    Choose which options to stream: the nearest `max_expiries` expiries, and
    strikes within ±strike_range of the current index price.

    Options far from the money are illiquid and mostly noise (see the README),
    and the nearest expiries are where most of the trading happens.
    """
    live = [i for i in instruments if i.is_active and i.expiry_ms > now_ms]
    expiries = set(sorted({i.expiry_ms for i in live})[:max_expiries])
    lo, hi = index_price * (1 - strike_range), index_price * (1 + strike_range)
    chosen = [i for i in live if i.expiry_ms in expiries and lo <= i.strike <= hi]
    return sorted(chosen, key=lambda i: (i.expiry_ms, i.strike, i.option_type))
