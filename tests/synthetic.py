"""
Synthetic Deribit-shaped data for tests: instruments, ticker updates and trades.

Prices come from Black-76 with a known volatility smile, so tests can check
that the pipeline recovers exactly the vols that went in.
"""

import math
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from options_analytics.pricing import black76_greeks, black76_price, year_fraction

CURRENCY = "BTC"
TICK = 0.0001


def smile_iv(strike: float, forward: float) -> float:
    """A skewed smile: higher vol for low strikes, like the real BTC smile."""
    k = math.log(strike / forward)
    return max(0.45 - 0.30 * k + 1.5 * k * k, 0.15)


def instrument_name(expiry: datetime, strike: float, option_type: str) -> str:
    return f"{CURRENCY}-{expiry.day}{expiry:%b%y}-{int(strike)}-{option_type[0]}".upper()


def make_instruments(
    now: datetime,
    expiry_days: tuple[int, ...] = (3, 30),
    strikes: tuple[float, ...] = tuple(float(k) for k in range(80_000, 100_001, 2_500)),
) -> list[dict[str, Any]]:
    """Raw items in the shape of Deribit's public/get_instruments."""
    today_8am = now.replace(hour=8, minute=0, second=0, microsecond=0)
    out = []
    for days in expiry_days:
        expiry = today_8am + timedelta(days=days)
        for strike in strikes:
            for option_type in ("call", "put"):
                out.append(
                    {
                        "instrument_name": instrument_name(expiry, strike, option_type),
                        "kind": "option",
                        "base_currency": CURRENCY,
                        "settlement_currency": CURRENCY,
                        "option_type": option_type,
                        "strike": strike,
                        "expiration_timestamp": int(expiry.timestamp() * 1000),
                        "tick_size": TICK,
                        "contract_size": 1.0,
                        "min_trade_amount": 0.1,
                        "is_active": True,
                    }
                )
    return out


@dataclass
class Market:
    """A simple market: index follows a random walk, forwards carry 6% a year."""

    index: float = 90_000.0
    carry: float = 0.06
    rng: random.Random = field(default_factory=lambda: random.Random(42))

    def step(self, minutes: float = 0.5) -> None:
        # ~45% annual vol random walk
        self.index *= math.exp(self.rng.gauss(0, 0.45 * math.sqrt(minutes / 525_600)))

    def forward(self, expiry_ms: int, at_ms: int) -> float:
        return self.index * math.exp(self.carry * year_fraction(at_ms, expiry_ms))

    def ticker(self, inst: dict[str, Any], at_ms: int, spread_btc: float = 0.0005) -> dict[str, Any]:
        """A payload shaped like Deribit's ticker.{instrument}.100ms notification."""
        expiry_ms = inst["expiration_timestamp"]
        strike, is_call = inst["strike"], inst["option_type"] == "call"
        t = year_fraction(at_ms, expiry_ms)
        fwd = self.forward(expiry_ms, at_ms)
        iv = smile_iv(strike, fwd)
        mark = black76_price(fwd, strike, t, iv, is_call) / fwd
        bid = round(mark - spread_btc, 4) if mark - spread_btc >= TICK else None
        ask = round(mark + spread_btc, 4)
        greeks = black76_greeks(fwd, strike, t, iv, is_call)
        return {
            "timestamp": at_ms,
            "state": "open",
            "instrument_name": inst["instrument_name"],
            "index_price": round(self.index, 2),
            "underlying_price": fwd,
            "mark_price": mark,
            "mark_iv": round(iv * 100, 6),
            "best_bid_price": bid,
            "best_ask_price": ask,
            "best_bid_amount": 10.0 if bid else 0.0,
            "best_ask_amount": 12.5,
            "bid_iv": 0.0,
            "ask_iv": 0.0,
            "open_interest": 100.0,
            "interest_rate": 0.0,
            "greeks": {
                "delta": greeks.delta,
                "gamma": greeks.gamma,
                "vega": greeks.vega,
                "theta": greeks.theta,
                "rho": 0.0,
            },
        }

    def trade(self, inst: dict[str, Any], at_ms: int, trade_id: int, direction: str) -> dict[str, Any]:
        """A payload shaped like one item of Deribit's trades.option.BTC.100ms notification."""
        quote = self.ticker(inst, at_ms)
        side_price = quote["best_ask_price"] if direction == "buy" else (quote["best_bid_price"] or TICK)
        return {
            "trade_seq": trade_id,
            "trade_id": f"{CURRENCY}-{trade_id}",
            "timestamp": at_ms,
            "tick_direction": 0,
            "price": side_price,
            "mark_price": quote["mark_price"],
            "iv": quote["mark_iv"],
            "instrument_name": inst["instrument_name"],
            "index_price": quote["index_price"],
            "direction": direction,
            "amount": 1.5,
            "contracts": 1.5,
        }


def history(
    instruments: list[dict[str, Any]], now: datetime, minutes: int = 90, every_s: int = 30
) -> tuple[list[tuple[int, dict[str, Any]]], list[tuple[int, dict[str, Any]]]]:
    """Ticker updates for every instrument every `every_s` seconds, plus some trades.

    Returns (tickers, trades) as lists of (received_us, payload).
    """
    market = Market()
    start = now - timedelta(minutes=minutes)
    steps = minutes * 60 // every_s
    tickers, trades = [], []
    trade_id = 1
    for i in range(steps):
        at = start + timedelta(seconds=i * every_s)
        at_ms = int(at.timestamp() * 1000)
        received_us = at_ms * 1000 + 25_000  # pretend each message took 25 ms to arrive
        for inst in instruments:
            tickers.append((received_us, market.ticker(inst, at_ms)))
        if i % 4 == 0:  # a trade every two minutes, a few seconds after the quotes
            inst = instruments[(i * 7) % len(instruments)]
            direction = "buy" if i % 8 == 0 else "sell"
            trades.append((received_us + 3_000_000, market.trade(inst, at_ms + 3_000, trade_id, direction)))
            trade_id += 1
        market.step(every_s / 60)
    return tickers, trades


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)
