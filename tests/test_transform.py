"""Raw Deribit messages -> ClickHouse rows, using a real ticker captured from Deribit."""

import pytest
from conftest import load_fixture

from options_analytics.instruments import instrument_from_deribit
from options_analytics.transform import TICK_COLUMNS, TickRow, ticker_row, trade_row

TICKER = load_fixture("ticker_BTC-30OCT26-87000-C.json")
INSTRUMENT = instrument_from_deribit(load_fixture("instrument_BTC-30OCT26-87000-C.json"))
RECEIVED_US = TICKER["timestamp"] * 1000 + 40_000  # 40 ms after the exchange timestamp


def row_for(data: dict) -> TickRow:
    row = ticker_row(data, INSTRUMENT, RECEIVED_US, RECEIVED_US + 1_500)
    assert row is not None
    return row


def test_real_ticker_reproduces_deribit_iv_and_greeks() -> None:
    row = row_for(TICKER)
    assert row.iv_mark == pytest.approx(0.3384, abs=5e-4)
    assert row.deribit_mark_iv == pytest.approx(0.3384)
    assert row.delta == pytest.approx(TICKER["greeks"]["delta"], abs=1e-4)
    assert row.vega == pytest.approx(TICKER["greeks"]["vega"], rel=1e-3)
    assert row.theta == pytest.approx(TICKER["greeks"]["theta"], rel=1e-3)


def test_mid_iv_sits_between_bid_and_ask_iv() -> None:
    row = row_for(TICKER)
    assert row.iv_mid is not None
    assert TICKER["bid_iv"] / 100 < row.iv_mid < TICKER["ask_iv"] / 100


def test_timestamps_and_units() -> None:
    row = row_for(TICKER)
    assert (row.received_ts - row.exchange_ts).total_seconds() == pytest.approx(0.040)
    assert (row.processed_ts - row.received_ts).total_seconds() == pytest.approx(0.0015)
    assert row.time_to_expiry * 365 == pytest.approx(23.8028, rel=1e-5)
    assert row.expiry.isoformat() == "2026-10-30T08:00:00+00:00"
    assert len(row) == len(TICK_COLUMNS)


@pytest.mark.parametrize("empty", [None, 0, 0.0])
def test_one_sided_book_has_no_mid_iv(empty: float | None) -> None:
    # Deribit marks an empty side with null (and sometimes 0): no mid, so no mid IV.
    row = row_for({**TICKER, "best_bid_price": empty, "bid_iv": 0})
    assert row.bid_price is None
    assert row.iv_mid is None
    assert row.deribit_bid_iv is None
    assert row.iv_mark is not None


def test_price_below_intrinsic_stores_null_iv() -> None:
    # A stale mark below intrinsic value has no implied vol: store NULL, don't crash.
    deep_itm = {**TICKER, "underlying_price": 120_000.0, "mark_price": 0.0001, "best_bid_price": None}
    row = row_for(deep_itm)
    assert row.iv_mark is None
    assert row.delta is None


def test_expired_option_is_skipped() -> None:
    after_expiry = {**TICKER, "timestamp": INSTRUMENT.expiry_ms + 1}
    assert ticker_row(after_expiry, INSTRUMENT, RECEIVED_US, RECEIVED_US) is None


def test_trade_row() -> None:
    trade = {
        "trade_seq": 2,
        "trade_id": "48079289",
        "timestamp": TICKER["timestamp"],
        "tick_direction": 2,
        "price": 0.0325,
        "mark_price": 0.0321,
        "iv": 34.23,
        "instrument_name": INSTRUMENT.instrument_name,
        "index_price": 86224.18,
        "direction": "buy",
        "amount": 3,
        "contracts": 3,
    }
    row = trade_row(trade, INSTRUMENT, RECEIVED_US, RECEIVED_US + 1)
    assert row.trade_id == "48079289"
    assert row.direction == "buy"
    assert row.deribit_iv == pytest.approx(0.3423)
    assert row.strike == 87_000.0
    assert row.block_trade_id == ""
    assert row.is_liquidation == 0
