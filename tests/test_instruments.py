from datetime import UTC, datetime

from conftest import load_fixture

from options_analytics.instruments import Instrument, instrument_from_deribit, select_instruments

DAY_MS = 86_400_000


def make(name: str, strike: float, expiry_ms: int, active: bool = True) -> Instrument:
    return Instrument(name, "BTC", "call", strike, expiry_ms, 0.0001, 1.0, 0.1, active)


def test_instrument_from_deribit() -> None:
    inst = instrument_from_deribit(load_fixture("instrument_BTC-30OCT26-87000-C.json"))
    assert inst.is_call
    assert inst.strike == 87_000.0
    assert inst.expiry == datetime(2026, 10, 30, 8, tzinfo=UTC)


def test_select_nearest_expiries_and_strikes_near_spot() -> None:
    now = 1_000 * DAY_MS
    instruments = [
        make("expired", 100, now - 1),
        make("e1-near", 100, now + 1 * DAY_MS),
        make("e1-far", 200, now + 1 * DAY_MS),
        make("e2-near", 110, now + 2 * DAY_MS),
        make("e2-inactive", 100, now + 2 * DAY_MS, active=False),
        make("e3-near", 95, now + 3 * DAY_MS),
    ]
    chosen = select_instruments(instruments, index_price=100, now_ms=now, max_expiries=2, strike_range=0.25)
    assert [i.instrument_name for i in chosen] == ["e1-near", "e2-near"]
