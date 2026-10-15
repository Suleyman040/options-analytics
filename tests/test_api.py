"""
Every API endpoint, and so every query in queries.py, run against a real
ClickHouse engine filled with synthetic data whose true vols are known.
"""

from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
import synthetic
from conftest import FakePostgres
from fastapi.testclient import TestClient

from options_analytics.api import create_app
from options_analytics.clickhouse_store import ClickHouseLike
from options_analytics.config import Settings
from options_analytics.instruments import Instrument


@pytest.fixture(scope="module")
def client(ch: ClickHouseLike, instruments: list[Instrument]) -> Iterator[TestClient]:
    app = create_app(ch=ch, pg=FakePostgres(instruments), settings=Settings(currency="BTC"))  # type: ignore[arg-type]
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="module")
def far_expiry(instruments: list[Instrument]) -> datetime:
    return max(i.expiry for i in instruments)


def get(client: TestClient, path: str) -> Any:
    response = client.get(path)
    assert response.status_code == 200, response.text
    return response.json()


def test_health(client: TestClient) -> None:
    assert get(client, "/health") == {"status": "ok", "clickhouse": True, "postgres": True}


def test_smile_recovers_the_input_vols_using_otm_options(client: TestClient, far_expiry: datetime) -> None:
    points = get(client, f"/smile?expiry={far_expiry.date()}&minutes=10")
    assert [p["strike"] for p in points] == sorted(p["strike"] for p in points)
    assert len(points) == 9  # one option per strike
    for p in points:
        assert p["option_type"] == ("put" if p["strike"] < p["forward"] else "call")
        assert p["iv_mark"] == pytest.approx(synthetic.smile_iv(p["strike"], p["forward"]), abs=1e-6)
        assert p["deribit_mark_iv"] == pytest.approx(p["iv_mark"], abs=1e-6)
    # The smile is skewed: low strikes carry more vol than high strikes.
    assert points[0]["iv_mark"] > points[-1]["iv_mark"]


def test_term_structure_has_atm_vol_per_expiry(client: TestClient) -> None:
    rows = get(client, "/term-structure?minutes=10")
    assert len(rows) == 2
    for r in rows:
        assert abs(r["atm_strike"] - r["forward"]) <= 1_250  # nearest strike on a 2,500 grid
        assert r["atm_iv"] == pytest.approx(synthetic.smile_iv(r["atm_strike"], r["forward"]), abs=1e-6)
    assert rows[0]["days_to_expiry"] < rows[1]["days_to_expiry"]


def test_latest_tick_has_our_greeks_next_to_deribits(client: TestClient, far_expiry: datetime) -> None:
    name = synthetic.instrument_name(far_expiry, 90_000, "call")
    tick = get(client, f"/instruments/{name}/latest")
    assert tick["instrument_name"] == name
    for greek in ("delta", "gamma", "vega", "theta"):
        assert tick[greek] == pytest.approx(tick[f"deribit_{greek}"], rel=1e-6)


def test_unknown_instrument_is_404(client: TestClient) -> None:
    assert client.get("/instruments/BTC-NOPE/latest").status_code == 404


def test_iv_history_bars_from_materialised_view(client: TestClient, far_expiry: datetime) -> None:
    name = synthetic.instrument_name(far_expiry, 90_000, "put")
    bars = get(client, f"/instruments/{name}/iv-history?hours=2")
    assert 89 <= len(bars) <= 91  # 90 minutes of history
    assert sum(b["ticks"] for b in bars) == 180  # one tick every 30 s
    for b in bars:
        assert (
            b["iv_low"]
            <= min(b["iv_open"], b["iv_close"])
            <= max(b["iv_open"], b["iv_close"])
            <= b["iv_high"]
        )


def test_realised_vs_implied(client: TestClient, far_expiry: datetime) -> None:
    rows = get(client, f"/vol/realised-vs-implied?expiry={far_expiry.date()}&hours=2&window=30")
    assert len(rows) >= 88
    assert all(r["realised_vol"] is None for r in rows[:9])  # too few returns yet
    later = rows[30:]
    # The synthetic index moves at 45% vol; a 30-minute estimate is noisy but in range.
    assert all(0.1 < r["realised_vol"] < 1.5 for r in later)
    assert all(r["atm_iv"] is not None for r in later)


def test_trades_are_matched_to_the_quote_before_them(client: TestClient) -> None:
    trades = get(client, "/trades/recent?minutes=120&limit=1000")
    quoted = [t for t in trades if t["instrument_name"] != "BTC-UNQUOTED-150000-C"]
    unquoted = [t for t in trades if t["instrument_name"] == "BTC-UNQUOTED-150000-C"]
    assert len(quoted) == 45
    for t in quoted:
        assert t["quote_age_ms"] == 3_000  # trades happen 3 s after each round of quotes
        assert t["delta"] is not None
        sign = 1 if t["direction"] == "buy" else -1
        assert t["delta_traded"] == pytest.approx(sign * t["amount"] * t["delta"])
    # No quote for this option: the ASOF LEFT JOIN leaves NULLs rather than zeros.
    assert len(unquoted) == 1
    assert unquoted[0]["delta"] is None and unquoted[0]["quote_age_ms"] is None


def test_trade_flow(client: TestClient) -> None:
    rows = get(client, "/trades/flow?minutes=120")
    assert sum(r["trades"] for r in rows) == 46
    assert sum(r["trades_without_quote"] for r in rows) == 1
    for r in rows:
        assert r["contracts"] == pytest.approx(r["bought"] + r["sold"])


def test_latency(client: TestClient) -> None:
    latency = get(client, "/pipeline/latency?minutes=120")
    assert latency["ticks"] == 6_480
    assert latency["exchange_to_ingest_ms"] == pytest.approx([25.0, 25.0, 25.0])
    assert latency["ingest_to_process_ms"] == pytest.approx([2.0, 2.0, 2.0])


def test_model_check_agrees_with_deribit(client: TestClient) -> None:
    rows = get(client, "/pipeline/model-check?minutes=120")
    assert sum(r["ticks"] for r in rows) == 6_480
    for r in rows:
        assert r["solver_failures"] == 0
        assert r["mean_abs_iv_diff_pts"] < 1e-3


def test_storage(client: TestClient) -> None:
    tables = {t["table"]: t for t in get(client, "/pipeline/storage")}
    assert {"option_ticks", "option_trades", "option_iv_1m", "index_1m"} <= tables.keys()
    assert tables["option_ticks"]["rows"] == 6_480


def test_reference_endpoints(client: TestClient, far_expiry: datetime) -> None:
    assert len(get(client, "/expiries")) == 2
    calls = get(client, f"/instruments?expiry={far_expiry.date()}&option_type=call")
    assert len(calls) == 9 and all(c["option_type"] == "call" for c in calls)


def test_dashboard_is_served(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "<title>Options Analytics</title>" in response.text
