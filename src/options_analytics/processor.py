"""
Processor: Kafka -> implied vol and Greeks -> ClickHouse.  `python -m options_analytics.processor`

For each message it looks up the option's strike and expiry (reference data
from Postgres, cached in memory), solves for implied vol, computes Greeks, and
buffers the row. Buffers are written to ClickHouse in batches: ClickHouse is
built for few large inserts, and thousands of single-row inserts per second
would create thousands of tiny parts on disk for it to merge.

Kafka offsets are committed only after a batch is safely in ClickHouse. If the
process crashes mid-batch, it re-reads those messages on restart: at-least-once
delivery. The ReplacingMergeTree tables remove the resulting duplicates.
"""

import logging
import signal
import threading
import time
from collections import Counter
from collections.abc import Callable, Sequence
from typing import Any

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from options_analytics import kafka_io
from options_analytics.clickhouse_store import ClickHouseLike, ClickHouseStore
from options_analytics.config import Settings, get_settings
from options_analytics.instruments import Instrument
from options_analytics.logs import setup_logging
from options_analytics.migrate import retry
from options_analytics.postgres_store import PostgresStore
from options_analytics.transform import TICK_COLUMNS, TRADE_COLUMNS, TickRow, TradeRow, ticker_row, trade_row

log = logging.getLogger(__name__)


class ReferenceCache:
    """Instrument lookups from memory, reloaded from Postgres when a new name appears."""

    def __init__(self, load: Callable[[], list[Instrument]], min_reload_s: float = 30):
        self._load = load
        self._min_reload_s = min_reload_s
        self._by_name: dict[str, Instrument] = {}
        self._loaded_at = float("-inf")

    def reload(self) -> None:
        self._by_name = {i.instrument_name: i for i in self._load()}
        self._loaded_at = time.monotonic()

    def get(self, name: str) -> Instrument | None:
        inst = self._by_name.get(name)
        # Deribit lists new strikes during the day. Reload at most every 30 s
        # so a burst of unknown names can't hammer Postgres.
        if inst is None and time.monotonic() - self._loaded_at > self._min_reload_s:
            self.reload()
            inst = self._by_name.get(name)
        return inst

    def __len__(self) -> int:
        return len(self._by_name)


class Processor:
    def __init__(self, settings: Settings, consumer: Consumer, ch: ClickHouseLike, reference: ReferenceCache):
        self.settings = settings
        self.consumer = consumer
        self.ch = ch
        self.reference = reference
        self.ticks: list[TickRow] = []
        self.trades: list[TradeRow] = []
        self.uncommitted = 0
        self.last_iv: dict[str, float] = {}  # warm start for the IV solver
        self.stats: Counter[str] = Counter()

    def handle(self, msg: Message) -> None:
        value = msg.value()
        if value is None:
            self.stats["empty_messages"] += 1
            return
        data, received_us = kafka_io.decode_envelope(value)
        processed_us = time.time_ns() // 1000
        name = data["instrument_name"]
        inst = self.reference.get(name)
        if inst is None:
            self.stats["unknown_instrument"] += 1
            return
        if msg.topic() == self.settings.ticker_topic:
            row = ticker_row(data, inst, received_us, processed_us, self.last_iv.get(name))
            if row is None:
                self.stats["unpriceable"] += 1
                return
            if row.iv_mark is not None:
                self.last_iv[name] = row.iv_mark
            else:
                self.stats["iv_failures"] += 1
            self.ticks.append(row)
        else:
            self.trades.append(trade_row(data, inst, received_us, processed_us))

    def flush(self) -> None:
        if self.ticks:
            self._insert("option_ticks", self.ticks, TICK_COLUMNS)
            self.stats["ticks_written"] += len(self.ticks)
        if self.trades:
            self._insert("option_trades", self.trades, TRADE_COLUMNS)
            self.stats["trades_written"] += len(self.trades)
        if self.uncommitted:
            try:
                self.consumer.commit(asynchronous=False)
            except KafkaException as exc:
                log.warning("offset commit failed: %s", exc)
        self.ticks.clear()
        self.trades.clear()
        self.uncommitted = 0

    def _insert(
        self, table: str, rows: Sequence[Sequence[Any]], columns: Sequence[str], attempts: int = 5
    ) -> None:
        for attempt in range(1, attempts + 1):
            try:
                self.ch.insert(table, rows, columns)
                return
            except Exception as exc:
                if attempt == attempts:
                    raise  # crash; offsets weren't committed, so nothing is lost
                log.warning("insert into %s failed (%d/%d): %s", table, attempt, attempts, exc)
                time.sleep(2**attempt)

    def on_revoke(self, _consumer: Consumer, _partitions: list[TopicPartition]) -> None:
        """Another processor is taking over some partitions: write what we have first."""
        self.flush()

    def run(self, stop: threading.Event, report_every_s: float = 10) -> None:
        last_flush = last_report = time.monotonic()
        while not stop.is_set():
            # consume() returns when it has 1,000 messages or after 50 ms, whichever
            # comes first. A longer timeout means fuller batches but more latency.
            for msg in self.consumer.consume(num_messages=1000, timeout=0.05):
                if msg.error():
                    log.warning("consumer error: %s", msg.error())
                    continue
                self.uncommitted += 1
                try:
                    self.handle(msg)
                except Exception:
                    # One malformed message shouldn't stop the pipeline.
                    self.stats["bad_messages"] += 1
                    log.exception("could not process message at offset %s", msg.offset())

            now = time.monotonic()
            buffered = len(self.ticks) + len(self.trades)
            if buffered >= self.settings.batch_size or (
                self.uncommitted and now - last_flush >= self.settings.flush_interval_s
            ):
                self.flush()
                last_flush = now
            if now - last_report >= report_every_s:
                log.info("processor: %s", dict(self.stats))
                last_report = now
        self.flush()


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    kafka_io.wait_for_kafka(settings.kafka_bootstrap_servers)
    ch = retry(lambda: ClickHouseStore.from_settings(settings), "ClickHouse")
    pg = retry(lambda: PostgresStore(settings.postgres_dsn), "Postgres")

    reference = ReferenceCache(lambda: pg.load_instruments(settings.currency))
    reference.reload()
    while not len(reference):
        log.info("waiting for reference data (is the reference job running?)")
        time.sleep(5)
        reference.reload()
    log.info("loaded %d instruments", len(reference))

    # Create the topics if the ingestor hasn't yet: a consumer subscribed to a
    # missing topic may not notice it appear for minutes.
    kafka_io.ensure_topics(
        settings.kafka_bootstrap_servers,
        {settings.ticker_topic: settings.topic_partitions, settings.trade_topic: settings.topic_partitions},
    )
    consumer = kafka_io.make_consumer(settings.kafka_bootstrap_servers, settings.consumer_group)
    processor = Processor(settings, consumer, ch, reference)
    consumer.subscribe([settings.ticker_topic, settings.trade_topic], on_revoke=processor.on_revoke)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    try:
        processor.run(stop)
    finally:
        consumer.close()
        pg.close()
        log.info("processor stopped: %s", dict(processor.stats))


if __name__ == "__main__":
    main()
