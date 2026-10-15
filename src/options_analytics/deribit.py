"""
Deribit API clients.

- DeribitRest: plain HTTP calls for reference data (instrument list, index price).
- DeribitStream: a websocket connection that subscribes to channels and keeps
  itself alive: it answers heartbeats, reconnects with exponential backoff,
  and re-checks which channels it should be subscribed to as options list and
  expire.

Deribit's API is JSON-RPC 2.0: every request has an id and a method, and
subscription data arrives as {"method": "subscription", "params": {...}}.
"""

import asyncio
import contextlib
import json
import logging
import random
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from options_analytics.instruments import Instrument, instrument_from_deribit

log = logging.getLogger(__name__)

# Called with (channel, data, received_us) for every subscription message.
MessageHandler = Callable[[str, Any, int], None]
# Returns the channels the stream should currently be subscribed to.
ChannelSource = Callable[[], Awaitable[list[str]]]

SUBSCRIBE_CHUNK = 100  # channels per subscribe request


class DeribitError(RuntimeError):
    pass


class DeribitRest:
    def __init__(self, base_url: str, timeout: float = 10.0, client: httpx.Client | None = None):
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.Client(timeout=timeout)

    def _get(self, method: str, **params: Any) -> Any:
        response = self._client.get(f"{self.base_url}/public/{method}", params=params)
        response.raise_for_status()
        body = response.json()
        if "error" in body:
            raise DeribitError(f"{method}: {body['error']}")
        return body["result"]

    def get_instruments(self, currency: str) -> list[Instrument]:
        """All live options settled in `currency`."""
        raw = self._get("get_instruments", currency=currency, kind="option", expired="false")
        return [
            instrument_from_deribit(item)
            for item in raw
            if item.get("kind") == "option" and item.get("settlement_currency", currency) == currency
        ]

    def get_index_price(self, currency: str) -> float:
        """Current spot index, e.g. btc_usd."""
        result = self._get("get_index_price", index_name=f"{currency.lower()}_usd")
        return float(result["index_price"])

    def close(self) -> None:
        self._client.close()


class DeribitStream:
    def __init__(
        self,
        url: str,
        heartbeat_s: int = 30,
        refresh_s: float = 900,
        max_backoff_s: float = 60,
    ):
        self.url = url
        self.heartbeat_s = heartbeat_s
        self.refresh_s = refresh_s
        self.max_backoff_s = max_backoff_s
        self.stats: Counter[str] = Counter()
        self._next_id = 0

    async def run(self, channels: ChannelSource, on_message: MessageHandler, stop: asyncio.Event) -> None:
        """Stay connected and subscribed until `stop` is set."""
        backoff = 1.0
        while not stop.is_set():
            started = time.monotonic()
            try:
                await self._session(channels, on_message, stop)
            except (OSError, WebSocketException, TimeoutError, httpx.HTTPError, DeribitError) as exc:
                log.warning("connection lost: %r", exc)
            if stop.is_set():
                break
            # A connection that stayed up for a minute was healthy: start backoff again.
            if time.monotonic() - started > 60:
                backoff = 1.0
            delay = backoff * random.uniform(0.5, 1.0)  # jitter avoids reconnect storms
            log.info("reconnecting in %.1fs", delay)
            self.stats["reconnects"] += 1
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)
            backoff = min(backoff * 2, self.max_backoff_s)

    async def _session(
        self, channels: ChannelSource, on_message: MessageHandler, stop: asyncio.Event
    ) -> None:
        async with connect(self.url, max_size=2**24, ping_interval=20, open_timeout=15) as ws:
            log.info("connected to %s", self.url)
            await self._send(ws, "public/set_heartbeat", {"interval": self.heartbeat_s})
            current = set(await channels())
            await self._change_subscriptions(ws, "public/subscribe", current)

            async def close_on_stop() -> None:
                await stop.wait()
                await ws.close()

            tasks = [
                asyncio.create_task(close_on_stop()),
                asyncio.create_task(self._refresh_loop(ws, channels, current)),
            ]
            try:
                async for raw in ws:
                    received_us = time.time_ns() // 1000
                    await self._handle(ws, raw, received_us, on_message)
            finally:
                for task in tasks:
                    task.cancel()

    async def _handle(
        self, ws: ClientConnection, raw: str | bytes, received_us: int, on_message: MessageHandler
    ) -> None:
        msg = json.loads(raw)
        method = msg.get("method")
        if method == "subscription":
            params = msg["params"]
            self.stats["messages"] += 1
            on_message(params["channel"], params["data"], received_us)
        elif method == "heartbeat":
            # Deribit sends test_request and expects public/test straight back,
            # otherwise it closes the connection.
            if msg["params"]["type"] == "test_request":
                await self._send(ws, "public/test", {})
                self.stats["heartbeats"] += 1
        elif "error" in msg:
            log.error("request %s failed: %s", msg.get("id"), msg["error"])
        elif isinstance(msg.get("result"), list):
            log.debug("request %s ok: %d channels", msg.get("id"), len(msg["result"]))

    async def _refresh_loop(self, ws: ClientConnection, channels: ChannelSource, current: set[str]) -> None:
        """Every refresh_s, subscribe to new options and drop expired or far-away ones."""
        while True:
            await asyncio.sleep(self.refresh_s)
            try:
                wanted = set(await channels())
            except (httpx.HTTPError, DeribitError) as exc:
                log.warning("could not refresh channels: %r", exc)
                continue
            added, removed = wanted - current, current - wanted
            if added:
                await self._change_subscriptions(ws, "public/subscribe", added)
            if removed:
                await self._change_subscriptions(ws, "public/unsubscribe", removed)
            current.clear()
            current.update(wanted)
            if added or removed:
                log.info("subscriptions updated: +%d -%d (now %d)", len(added), len(removed), len(current))

    async def _change_subscriptions(self, ws: ClientConnection, method: str, channels: set[str]) -> None:
        ordered = sorted(channels)
        for i in range(0, len(ordered), SUBSCRIBE_CHUNK):
            await self._send(ws, method, {"channels": ordered[i : i + SUBSCRIBE_CHUNK]})
        log.info("%s: %d channels", method, len(ordered))

    async def _send(self, ws: ClientConnection, method: str, params: dict[str, Any]) -> None:
        self._next_id += 1
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params}))
