"""
Black-76 option pricing, Greeks and implied volatility.

Black-76 is Black-Scholes written in terms of the forward price F instead of
the spot price. Deribit prices every option off the forward for its expiry
(`underlying_price` in the ticker), so this is the right model for its data.

Conventions, chosen to match the numbers Deribit publishes:
- Prices are in USD. Deribit quotes option prices in BTC, so multiply a
  Deribit price by the forward to get USD before calling these functions.
- The interest rate is 0, as Deribit uses. With no interest rate there is no
  discounting.
- `t` is time to expiry in years (365-day year: crypto trades every day).
- `vol` is a decimal: 0.3384 means 33.84%.

tests/test_pricing.py checks these functions against values captured from
Deribit's live API, and against finite-difference derivatives.
"""

from dataclasses import dataclass
from math import erf, exp, log, pi, sqrt

MS_PER_YEAR = 365 * 24 * 60 * 60 * 1000

# Bracket for the implied-vol solver: 0.01% to 1000% vol.
MIN_VOL = 1e-4
MAX_VOL = 10.0


def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function, N(x)."""
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def norm_pdf(x: float) -> float:
    """Standard normal probability density function, φ(x)."""
    return exp(-0.5 * x * x) / sqrt(2.0 * pi)


@dataclass(frozen=True, slots=True)
class Greeks:
    """Sensitivities of an option price, in the same units Deribit uses."""

    delta: float  # USD change per $1 move in the forward
    gamma: float  # change in delta per $1 move in the forward
    vega: float  # USD change per 1 point of vol (e.g. 33.84% -> 34.84%)
    theta: float  # USD change per calendar day, all else equal


def year_fraction(start_ms: int, end_ms: int) -> float:
    """Years between two Unix timestamps in milliseconds."""
    return (end_ms - start_ms) / MS_PER_YEAR


def d1_d2(forward: float, strike: float, t: float, vol: float) -> tuple[float, float]:
    """
    d1 = [ln(F/K) + ½σ²t] / (σ√t),   d2 = d1 − σ√t

    σ√t is the standard deviation of ln(price) over the option's life, so d1
    and d2 measure how far the strike is from the forward in standard deviations.
    """
    if forward <= 0 or strike <= 0 or t <= 0 or vol <= 0:
        raise ValueError(f"inputs must be positive: F={forward} K={strike} t={t} vol={vol}")
    sd = vol * sqrt(t)
    d1 = (log(forward / strike) + 0.5 * vol * vol * t) / sd
    return d1, d1 - sd


def black76_price(forward: float, strike: float, t: float, vol: float, is_call: bool) -> float:
    """
    call = F·N(d1) − K·N(d2)
    put  = K·N(−d2) − F·N(−d1)

    N(d2) is the probability the option finishes in the money.
    """
    d1, d2 = d1_d2(forward, strike, t, vol)
    if is_call:
        return forward * norm_cdf(d1) - strike * norm_cdf(d2)
    return strike * norm_cdf(-d2) - forward * norm_cdf(-d1)


def black76_greeks(forward: float, strike: float, t: float, vol: float, is_call: bool) -> Greeks:
    """
    delta = N(d1) for a call, N(d1) − 1 for a put
    gamma = φ(d1) / (F σ √t)
    vega  = F φ(d1) √t / 100            (per 1 vol point)
    theta = −F φ(d1) σ / (2 √t) / 365   (per calendar day)

    Gamma, vega and theta are the same for calls and puts: put-call parity
    says call − put = F − K, which is linear in F and doesn't depend on σ or t.
    """
    d1, _ = d1_d2(forward, strike, t, vol)
    pdf = norm_pdf(d1)
    sqrt_t = sqrt(t)
    return Greeks(
        delta=norm_cdf(d1) if is_call else norm_cdf(d1) - 1.0,
        gamma=pdf / (forward * vol * sqrt_t),
        vega=forward * pdf * sqrt_t / 100.0,
        theta=-forward * pdf * vol / (2.0 * sqrt_t) / 365.0,
    )


def implied_vol(
    price: float,
    forward: float,
    strike: float,
    t: float,
    is_call: bool,
    guess: float = 0.5,
    tol: float = 1e-8,
    max_iter: int = 100,
) -> float:
    """
    The vol at which black76_price(...) equals `price`.

    There's no closed form, so it's solved numerically with Newton's method,
    kept inside a bracket [lo, hi] that always contains the answer. If a Newton
    step would leave the bracket, it bisects instead.

    Why the bracket: far from the money vega is tiny, so Newton's step
    (diff / vega) can fling the guess to a negative or huge vol. Bisection is
    slow but can't fail, and Newton is very fast once it's close.

    `guess` lets a caller warm-start from the last solved vol for the same
    option, which usually converges in 1–2 iterations.

    Raises ValueError if the price is outside the no-arbitrage bounds
    (call: max(F−K, 0) < price < F, put: max(K−F, 0) < price < K), and
    RuntimeError if it fails to converge.
    """
    intrinsic = max(forward - strike, 0.0) if is_call else max(strike - forward, 0.0)
    upper = forward if is_call else strike
    if not intrinsic < price < upper:
        raise ValueError(f"price {price} outside no-arbitrage bounds ({intrinsic}, {upper})")

    lo, hi = MIN_VOL, MAX_VOL
    vol = min(max(guess, lo), hi)
    for _ in range(max_iter):
        diff = black76_price(forward, strike, t, vol, is_call) - price
        if abs(diff) < tol:
            return vol
        # Price rises with vol, so a positive diff means the guess is too high.
        if diff > 0:
            hi = vol
        else:
            lo = vol
        d1, _ = d1_d2(forward, strike, t, vol)
        vega_raw = forward * norm_pdf(d1) * sqrt(t)  # dPrice/dVol, per unit of vol
        new_vol = vol - diff / vega_raw if vega_raw > 0 else -1.0
        if not lo < new_vol < hi:
            new_vol = 0.5 * (lo + hi)
        if hi - lo < 1e-12:
            return new_vol
        vol = new_vol
    raise RuntimeError(f"implied_vol did not converge: price={price} F={forward} K={strike}")
