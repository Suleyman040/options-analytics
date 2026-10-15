"""Deribit clients against local fakes: an httpx mock and a real websocket server."""

import asyncio
import json
from typing import Any

import httpx
from conftest import load_fixture
from websockets.asyncio.server import ServerConnection, serve

from options_analytics.deribit import DeribitRest, DeribitStream


def test_rest_client_parses_instruments_and_index() -> None:
    btc_option = load_fixture("instrument_BTC-30OCT26-87000-C.json")
    usdc_option = {**btc_option, "instrument_name": "BTC_USDC-30OCT26-87000-C", "settlement_currency": "USDC"}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/get_instruments"):
            assert request.url.params["kind"] == "option"
            return httpx.Response(200, json={"jsonrpc": "2.0", "result": [btc_option, usdc_option]})
        if request.url.path.endswith("/get_index_price"):
            assert request.url.params["index_name"] == "btc_usd"
            return httpx.Response(200, json={"jsonrpc": "2.0", "result": {"index_price": 86224.18}})
        return httpx.Response(404)

    rest = DeribitRest("https://test", client=httpx.Client(transport=httpx.MockTransport(handler)))
    instruments = rest.get_instruments("BTC")
    assert [i.instrument_name for i in instruments] == ["BTC-30OCT26-87000-C"]  # USDC-settled one dropped
    assert rest.get_index_price("BTC") == 86224.18


class FakeDeribit:
    """Just enough of Deribit's websocket API: heartbeats, subscribe, unsubscribe."""

    def __init__(self, drop_first_connection: bool = False):
        self.drop_first_connection = drop_first_connection
        self.connections = 0
        self.requests: list[tuple[int, str, Any]] = []

    async def handler(self, ws: ServerConnection) -> None:
        self.connections += 1
        conn = self.connections
        async for raw in ws:
            msg = json.loads(raw)
            self.requests.append((conn, msg["method"], msg.get("params")))
            if msg["method"] == "public/test" and self.drop_first_connection and conn == 1:
                await ws.close()  # drop the connection once the client has answered a heartbeat
            elif msg["method"] == "public/set_heartbeat":
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": "ok"}))
                await ws.send(
                    json.dumps({"jsonrpc": "2.0", "method": "heartbeat", "params": {"type": "test_request"}})
                )
            elif msg["method"] in ("public/subscribe", "public/unsubscribe"):
                channels = msg["params"]["channels"]
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": channels}))
                if msg["method"] == "public/subscribe":
                    for channel in channels:
                        data = {"connection": conn}
                        await ws.send(
                            json.dumps(
                                {"method": "subscription", "params": {"channel": channel, "data": data}}
                            )
                        )


async def test_stream_answers_heartbeats_and_reconnects() -> None:
    fake = FakeDeribit(drop_first_connection=True)
    got: list[tuple[str, int]] = []
    stop = asyncio.Event()

    def on_message(channel: str, data: Any, received_us: int) -> None:
        assert received_us > 0
        got.append((channel, data["connection"]))
        if len(got) == 4:
            stop.set()

    async def channels() -> list[str]:
        return ["ticker.A.100ms", "ticker.B.100ms"]

    async with serve(fake.handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        stream = DeribitStream(f"ws://127.0.0.1:{port}", refresh_s=3600, max_backoff_s=0.2)
        await asyncio.wait_for(stream.run(channels, on_message, stop), timeout=15)

    assert got == [("ticker.A.100ms", 1), ("ticker.B.100ms", 1), ("ticker.A.100ms", 2), ("ticker.B.100ms", 2)]
    assert stream.stats["reconnects"] == 1
    assert "public/test" in {method for _, method, _ in fake.requests}, "heartbeat test_request not answered"


async def test_stream_refresh_subscribes_new_and_drops_old_channels() -> None:
    fake = FakeDeribit()
    calls = 0
    stop = asyncio.Event()

    async def channels() -> list[str]:
        nonlocal calls
        calls += 1
        return ["ticker.A.100ms", "ticker.B.100ms"] if calls == 1 else ["ticker.B.100ms", "ticker.C.100ms"]

    def on_message(channel: str, data: Any, received_us: int) -> None:
        if channel == "ticker.C.100ms":
            stop.set()

    async with serve(fake.handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        stream = DeribitStream(f"ws://127.0.0.1:{port}", refresh_s=0.2)
        await asyncio.wait_for(stream.run(channels, on_message, stop), timeout=15)

    changes = [(method, params["channels"]) for _, method, params in fake.requests if "subscribe" in method]
    assert ("public/subscribe", ["ticker.C.100ms"]) in changes
    assert ("public/unsubscribe", ["ticker.A.100ms"]) in changes
