"""
Exploratory look at live Deribit BTC options data: the option chain, one full
ticker, and the volatility smile for the expiry nearest 30 days out.

    pip install -e ".[scripts]"
    python scripts/explore.py
"""

import json

import httpx
import matplotlib.pyplot as plt
import pandas as pd

BASE_URL = "https://www.deribit.com/api/v2/public"


def get(method: str, **params):
    """Call a public Deribit REST method and return its 'result' field."""
    response = httpx.get(f"{BASE_URL}/{method}", params=params, timeout=10)
    response.raise_for_status()
    return response.json()["result"]


# Reference data: every live BTC option. Timestamps are Unix ms; keep them in UTC.
instruments = pd.DataFrame(get("get_instruments", currency="BTC", kind="option", expired="false"))
instruments["expiry"] = pd.to_datetime(instruments["expiration_timestamp"], unit="ms", utc=True)
print(f"{len(instruments)} live BTC options across {instruments['expiry'].nunique()} expiries\n")

# Market snapshot of the whole chain. mark_price is in BTC, mark_iv in percent,
# underlying_price is the forward for each option's expiry.
summary = pd.DataFrame(get("get_book_summary_by_currency", currency="BTC", kind="option"))
chain = summary[["instrument_name", "mark_price", "mark_iv", "underlying_price"]].merge(
    instruments[["instrument_name", "strike", "option_type", "expiry"]], on="instrument_name"
)

# Expiry closest to 30 days out.
target = pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=30)
expiry = min(chain["expiry"].unique(), key=lambda e: abs(e - target))
one_expiry = chain[chain["expiry"] == expiry].sort_values("strike")
forward = one_expiry["underlying_price"].iloc[0]  # shared by every option in the expiry

calls = one_expiry[one_expiry["option_type"] == "call"]
print(f"Calls expiring {expiry:%d %b %Y}, forward price ≈ ${forward:,.0f}")
print(calls[["instrument_name", "strike", "mark_price", "mark_iv"]].to_string(index=False), "\n")

# Full ticker for the at-the-money call: the payload the pipeline streams live.
atm_name = calls.loc[(calls["strike"] - forward).abs().idxmin(), "instrument_name"]
print(f"Full ticker for {atm_name}:")
print(json.dumps(get("ticker", instrument_name=atm_name), indent=2))

# Volatility smile: calls and puts should overlap (put-call parity).
fig, ax = plt.subplots(figsize=(9, 5))
for option_type, marker in [("call", "o"), ("put", "x")]:
    group = one_expiry[one_expiry["option_type"] == option_type]
    ax.plot(group["strike"], group["mark_iv"], marker=marker, label=option_type)
ax.axvline(forward, linestyle="--", color="grey", label="forward")
ax.set(
    xlabel="Strike (USD)",
    ylabel="Mark IV (%)",
    title=f"BTC volatility smile, {expiry:%d %b %Y} expiry",
)
ax.legend()
fig.savefig("smile.png", dpi=150, bbox_inches="tight")
print("\nSaved smile.png")
