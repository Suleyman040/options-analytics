"""
Ingestor: Deribit websocket -> Kafka.  `python -m options_analytics.ingestor`

It does as little as possible: receive, timestamp, publish. All the maths
happens downstream in the processor. Keeping the ingestor thin means it can
keep up with bursts, and if the processor or ClickHouse goes down, messages
wait safely in Kafka instead of being lost.
"""

import asyncio
import contextlib
import logging
import signal
import time
from typing import Any

from confluent_kafka import Producer

from options_analytics import kafka_io
from options_analytics.config import Settings, get_settings
from options_analytics.deribit import DeribitRest, DeribitStream
from options_analytics.instruments import select_instruments
from options_analytics.logs import setup_logging

log = logging.getLogger(__name__)


class Ingestor:
    def __init__(self, settings: Settings, producer: Producer, rest: DeribitRest):
        self.settings = settings
        self.producer = producer
        self.rest = rest
        self.stream = DeribitStream(settings.deribit_ws_url, refresh_s=settings.selection_refresh_s)

    async def channels(self) -> list[str]:
        """Pick which options to stream right now and return their channel names."""
        s = self.settings
        instruments = await asyncio.to_thread(self.rest.get_instruments, s.currency)
        index_price = await asyncio.to_thread(self.rest.get_index_price, s.currency)
        chosen = select_instruments(
            instruments, index_price, int(time.time() * 1000), s.max_expiries, s.strike_range
        )
        expiries = sorted({i.expiry.date().isoformat() for i in chosen})
        log.info(
            "streaming %d of %d %s options (index %.0f, expiries %s)",
            len(chosen),
            len(instruments),
            s.currency,
            index_price,
            ", ".join(expiries),
        )
        tickers = [f"ticker.{i.instrument_name}.{s.ticker_interval}" for i in chosen]
        # One channel carries every option trade for the currency.
        return [*tickers, f"trades.option.{s.currency}.100ms"]

    def on_message(self, channel: str, data: Any, received_us: int) -> None:
        if channel.startswith("ticker."):
            value = kafka_io.encode_envelope(data, received_us)
            kafka_io.publish(self.producer, self.settings.ticker_topic, data["instrument_name"], value)
        elif channel.startswith("trades."):
            for trade in data:  # trades arrive as a list
                value = kafka_io.encode_envelope(trade, received_us)
                kafka_io.publish(self.producer, self.settings.trade_topic, trade["instrument_name"], value)

    async def report(self, every_s: float = 10) -> None:
        last = 0
        while True:
            await asyncio.sleep(every_s)
            total = self.stream.stats["messages"]
            log.info(
                "%.0f msg/s | delivered %d | kafka queue %d | delivery errors %d | reconnects %d",
                (total - last) / every_s,
                kafka_io.stats["delivered"],
                len(self.producer),
                kafka_io.stats["delivery_errors"],
                self.stream.stats["reconnects"],
            )
            last = total

    async def run(self, stop: asyncio.Event) -> None:
        reporter = asyncio.create_task(self.report())
        try:
            await self.stream.run(self.channels, self.on_message, stop)
        finally:
            reporter.cancel()
            remaining = self.producer.flush(10)
            log.info("ingestor stopped (%d messages not flushed)", remaining)


async def amain() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    await asyncio.to_thread(kafka_io.wait_for_kafka, settings.kafka_bootstrap_servers)
    kafka_io.ensure_topics(
        settings.kafka_bootstrap_servers,
        {settings.ticker_topic: settings.topic_partitions, settings.trade_topic: settings.topic_partitions},
    )
    ingestor = Ingestor(
        settings,
        kafka_io.make_producer(settings.kafka_bootstrap_servers),
        DeribitRest(settings.deribit_rest_url),
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Not supported on Windows; there Ctrl+C raises KeyboardInterrupt instead.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    await ingestor.run(stop)


def main() -> None:
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(amain())


if __name__ == "__main__":
    main()
