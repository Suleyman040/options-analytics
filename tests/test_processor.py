"""The processor loop with a fake Kafka consumer and a fake ClickHouse."""

import threading
from collections.abc import Sequence
from typing import Any

from conftest import load_fixture

from options_analytics import kafka_io
from options_analytics.config import Settings
from options_analytics.instruments import instrument_from_deribit
from options_analytics.processor import Processor, ReferenceCache

SETTINGS = Settings(batch_size=1_000, flush_interval_s=0)
TICKER = load_fixture("ticker_BTC-30OCT26-87000-C.json")
INSTRUMENT = instrument_from_deribit(load_fixture("instrument_BTC-30OCT26-87000-C.json"))
TRADE = {
    "trade_seq": 1,
    "trade_id": "1",
    "timestamp": TICKER["timestamp"],
    "tick_direction": 0,
    "price": 0.0325,
    "mark_price": 0.0321,
    "iv": 34.0,
    "instrument_name": INSTRUMENT.instrument_name,
    "index_price": 86224.18,
    "direction": "sell",
    "amount": 1.0,
}


class FakeMessage:
    def __init__(self, topic: str, value: bytes | None, offset: int):
        self._topic, self._value, self._offset = topic, value, offset

    def topic(self) -> str:
        return self._topic

    def value(self) -> bytes | None:
        return self._value

    def offset(self) -> int:
        return self._offset

    def error(self) -> None:
        return None


class FakeConsumer:
    """Hands out one batch of messages, then tells the processor to stop."""

    def __init__(self, messages: list[FakeMessage], stop: threading.Event):
        self.messages, self.stop, self.commits = messages, stop, 0

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        batch, self.messages = self.messages, []
        if not batch:
            self.stop.set()
        return batch

    def commit(self, asynchronous: bool = True) -> None:
        self.commits += 1


class FakeClickHouse:
    def __init__(self) -> None:
        self.inserts: dict[str, list[Sequence[Any]]] = {}

    def insert(self, table: str, rows: Sequence[Sequence[Any]], columns: Sequence[str]) -> None:
        self.inserts.setdefault(table, []).extend(rows)

    def command(self, sql: str) -> None: ...

    def query(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        return []


def envelope(data: Any) -> bytes:
    return kafka_io.encode_envelope(data, received_us=TICKER["timestamp"] * 1000 + 30_000)


def test_processor_writes_batches_and_commits_offsets() -> None:
    stop = threading.Event()
    messages = [
        FakeMessage(SETTINGS.ticker_topic, envelope(TICKER), 0),
        FakeMessage(SETTINGS.ticker_topic, envelope({**TICKER, "timestamp": TICKER["timestamp"] + 100}), 1),
        FakeMessage(SETTINGS.trade_topic, envelope(TRADE), 0),
        FakeMessage(SETTINGS.ticker_topic, envelope({**TICKER, "instrument_name": "BTC-UNKNOWN"}), 2),
        FakeMessage(SETTINGS.ticker_topic, b"not json", 3),
    ]
    consumer = FakeConsumer(messages, stop)
    ch = FakeClickHouse()
    processor = Processor(SETTINGS, consumer, ch, ReferenceCache(lambda: [INSTRUMENT]))  # type: ignore[arg-type]
    processor.run(stop)

    assert len(ch.inserts["option_ticks"]) == 2
    assert len(ch.inserts["option_trades"]) == 1
    assert consumer.commits >= 1
    assert processor.stats["unknown_instrument"] == 1
    assert processor.stats["bad_messages"] == 1
    # The second tick for the same option was solved from a warm start.
    assert processor.last_iv[INSTRUMENT.instrument_name] > 0


def test_reference_cache_reloads_on_unknown_names_but_not_too_often() -> None:
    loads = 0

    def load() -> list[Any]:
        nonlocal loads
        loads += 1
        return [INSTRUMENT]

    cache = ReferenceCache(load, min_reload_s=60)
    assert cache.get(INSTRUMENT.instrument_name) == INSTRUMENT  # first miss triggers a load
    assert cache.get("BTC-NEW") is None
    assert cache.get("BTC-NEW") is None
    assert loads == 1  # throttled
