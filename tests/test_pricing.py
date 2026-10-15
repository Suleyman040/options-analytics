"""
Tests for options_analytics.pricing.

Two kinds of test:

1. Against Deribit. Real numbers captured from the live API by explore.py on
   6 Oct 2026. If our maths reproduces the exchange's, our conventions are right.
2. Self-consistency. Facts that must hold for any inputs: put-call parity,
   Greeks agreeing with finite-difference derivatives, and the IV solver
   undoing the pricer.

Run with:  pytest            (everything)
           pytest -k greeks  (only tests with "greeks" in the name)
           pytest -x         (stop at the first failure)
"""

import pytest

from options_analytics.pricing import (
    Greeks,
    black76_greeks,
    black76_price,
    d1_d2,
    implied_vol,
    year_fraction,
)

# --- Deribit snapshot: BTC-30OCT26-87000-C ----------------------------------

SNAPSHOT_MS = 1_791_290_635_801  # ticker "timestamp"
EXPIRY_MS = 1_793_347_200_000  # 30 Oct 2026 08:00 UTC: Deribit options expire at 08:00 UTC
FORWARD = 86_565.84  # ticker "underlying_price"
STRIKE = 87_000.0
MARK_IV = 0.3384  # ticker "mark_iv" is 33.84 (percent)


def snapshot_t() -> float:
    return year_fraction(SNAPSHOT_MS, EXPIRY_MS)


# --- year_fraction ----------------------------------------------------------


def test_year_fraction_one_day() -> None:
    assert year_fraction(0, 24 * 60 * 60 * 1000) == pytest.approx(1 / 365)


def test_year_fraction_deribit_snapshot() -> None:
    # 23.8028 days from the snapshot to expiry
    assert snapshot_t() == pytest.approx(23.8028 / 365, rel=1e-5)


# --- d1_d2 ------------------------------------------------------------------


def test_d1_d2_hand_example() -> None:
    # F = K, so ln(F/K) = 0. With σ = 0.2, t = 1: d1 = ½·0.04 / 0.2 = 0.1
    d1, d2 = d1_d2(forward=100, strike=100, t=1.0, vol=0.2)
    assert d1 == pytest.approx(0.1)
    assert d2 == pytest.approx(-0.1)


@pytest.mark.parametrize(
    "forward, strike, t, vol",
    [(0, 100, 1, 0.2), (100, 0, 1, 0.2), (100, 100, 0, 0.2), (100, 100, 1, 0), (100, 100, -1, 0.2)],
)
def test_d1_d2_rejects_non_positive_inputs(forward: float, strike: float, t: float, vol: float) -> None:
    with pytest.raises(ValueError):
        d1_d2(forward, strike, t, vol)


# --- black76_price ----------------------------------------------------------


def test_price_hand_example() -> None:
    # ATM call: 100·N(0.1) − 100·N(−0.1) = 100·(2·N(0.1) − 1) ≈ 7.9656
    assert black76_price(100, 100, 1.0, 0.2, is_call=True) == pytest.approx(7.9656, abs=1e-4)


def test_price_matches_deribit_mark() -> None:
    usd = black76_price(FORWARD, STRIKE, snapshot_t(), MARK_IV, is_call=True)
    assert usd / FORWARD == pytest.approx(0.0321, abs=1e-5)  # mark_price is 0.0321 BTC


@pytest.mark.parametrize("strike", [60_000, 80_000, 87_000, 95_000, 120_000])
def test_price_put_call_parity(strike: float) -> None:
    # With zero interest rate: call − put = F − K, at any vol
    t = snapshot_t()
    call = black76_price(FORWARD, strike, t, MARK_IV, is_call=True)
    put = black76_price(FORWARD, strike, t, MARK_IV, is_call=False)
    assert call - put == pytest.approx(FORWARD - strike, abs=1e-6)


# --- black76_greeks ---------------------------------------------------------


def test_greeks_match_deribit() -> None:
    g = black76_greeks(FORWARD, STRIKE, snapshot_t(), MARK_IV, is_call=True)
    assert isinstance(g, Greeks)
    assert g.delta == pytest.approx(0.49414, abs=1e-4)
    assert g.gamma == pytest.approx(5e-05, abs=5e-6)  # Deribit rounds gamma to 5 d.p.
    assert g.vega == pytest.approx(88.18143, rel=1e-3)
    assert g.theta == pytest.approx(-62.67715, rel=1e-3)


def test_greeks_put_vs_call() -> None:
    t = snapshot_t()
    call = black76_greeks(FORWARD, STRIKE, t, MARK_IV, is_call=True)
    put = black76_greeks(FORWARD, STRIKE, t, MARK_IV, is_call=False)
    assert put.delta == pytest.approx(call.delta - 1)
    assert put.gamma == pytest.approx(call.gamma)
    assert put.vega == pytest.approx(call.vega)
    assert put.theta == pytest.approx(call.theta)


@pytest.mark.parametrize(
    "strike, t, vol, is_call",
    [
        (87_000, 23.8 / 365, 0.3384, True),
        (70_000, 23.8 / 365, 0.4845, False),
        (110_000, 90 / 365, 0.46, True),
        (87_000, 2 / 365, 0.60, False),
    ],
)
def test_greeks_match_finite_differences(strike: float, t: float, vol: float, is_call: bool) -> None:
    """Each Greek should equal the slope of the price you get by nudging one input."""
    g = black76_greeks(FORWARD, strike, t, vol, is_call)

    def price(f: float = FORWARD, tt: float = t, v: float = vol) -> float:
        return black76_price(f, strike, tt, v, is_call)

    h = 1.0  # $1 nudge to the forward
    fd_delta = (price(f=FORWARD + h) - price(f=FORWARD - h)) / (2 * h)
    fd_gamma = (price(f=FORWARD + h) - 2 * price() + price(f=FORWARD - h)) / h**2

    dv = 1e-4  # tiny nudge to vol, then scaled to "per 1 vol point"
    fd_vega = (price(v=vol + dv) - price(v=vol - dv)) / (2 * dv) / 100

    one_day = 1 / 365  # theta: value lost as one day passes, so t shrinks
    fd_theta = price(tt=t - one_day / 2) - price(tt=t + one_day / 2)

    assert g.delta == pytest.approx(fd_delta, rel=1e-4)
    assert g.gamma == pytest.approx(fd_gamma, rel=1e-3)
    assert g.vega == pytest.approx(fd_vega, rel=1e-4)
    assert g.theta == pytest.approx(fd_theta, rel=1e-2)


# --- implied_vol ------------------------------------------------------------


@pytest.mark.parametrize(
    "price_btc, expected_iv",
    [
        (0.0321, 0.3384),  # mark_price -> mark_iv
        (0.0315, 0.3325),  # best_bid_price -> bid_iv
        (0.0325, 0.3423),  # best_ask_price -> ask_iv
    ],
)
def test_implied_vol_matches_deribit(price_btc: float, expected_iv: float) -> None:
    iv = implied_vol(price_btc * FORWARD, FORWARD, STRIKE, snapshot_t(), is_call=True)
    assert iv == pytest.approx(expected_iv, abs=5e-4)


@pytest.mark.parametrize(
    "strike, price_btc, expected_iv",
    [
        (40_000, 0.000029, 0.9414),  # worth $2.51, vega only $0.34 per vol point
        (50_000, 0.000160, 0.7946),
        (60_000, 0.000591, 0.6406),
    ],
)
def test_implied_vol_deep_otm_puts(strike: float, price_btc: float, expected_iv: float) -> None:
    # The cheap puts behind the zigzag in smile.png. Deribit rounds these
    # prices to 6 decimal places, a few cents, and with vega this small a
    # few cents moves IV by a noticeable amount, hence the looser tolerance.
    iv = implied_vol(price_btc * FORWARD, FORWARD, strike, snapshot_t(), is_call=False)
    assert iv == pytest.approx(expected_iv, abs=2e-3)


@pytest.mark.parametrize("vol", [0.15, 0.3384, 0.8, 2.0])
@pytest.mark.parametrize("moneyness", [0.8, 0.95, 1.0, 1.05, 1.25])
@pytest.mark.parametrize("days", [7, 30, 365])
@pytest.mark.parametrize("is_call", [True, False])
def test_implied_vol_round_trip(vol: float, moneyness: float, days: int, is_call: bool) -> None:
    """Price an option at a known vol, then check the solver recovers that vol."""
    strike = FORWARD * moneyness
    t = days / 365
    price = black76_price(FORWARD, strike, t, vol, is_call)
    if price - max(FORWARD - strike if is_call else strike - FORWARD, 0) < 0.01:
        pytest.skip("less than 1 cent of time value: the price carries no information about vol")
    assert implied_vol(price, FORWARD, strike, t, is_call) == pytest.approx(vol, abs=1e-6)


@pytest.mark.parametrize(
    "price, is_call",
    [
        (0.0, True),  # free option
        (100.0, True),  # call worth less than intrinsic value F − K = 565.84 at strike 86,000
        (FORWARD + 1, True),  # call worth more than the forward itself
        (90_000.0, False),  # put worth more than its strike of 86,000
    ],
)
def test_implied_vol_rejects_impossible_values(price: float, is_call: bool) -> None:
    with pytest.raises(ValueError):
        implied_vol(price, FORWARD, 86_000, 30 / 365, is_call)


def test_implied_vol_warm_start_gives_same_answer() -> None:
    # The processor passes the previous IV as a starting guess; any guess must converge to the same vol.
    t = snapshot_t()
    price = black76_price(FORWARD, STRIKE, t, MARK_IV, is_call=True)
    for guess in (0.01, 0.3, MARK_IV, 3.0, 50.0):
        assert implied_vol(price, FORWARD, STRIKE, t, True, guess=guess) == pytest.approx(MARK_IV, abs=1e-7)
