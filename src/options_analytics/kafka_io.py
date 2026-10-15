"""
Kafka helpers: topic setup, a producer and a consumer configured for this pipeline.

Messages are keyed by instrument name. Kafka sends every message with the same
key to the same partition, and keeps each partition in order, so updates for
one option are always processed in the order they arrived, even with several
partitions (and several processor instances) working in parallel.

Each message value is a small JSON envelope: the raw Deribit payload plus the
time the ingestor received it, which the latency metrics need.
"""

import json
import logging
import time
from collections import Counter
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

log = logging.getLogger(__name__)

stats: Counter[str] = Counter()


def ensure_topics(bootstrap: str, topics: dict[str, int], retention_hours: int = 24) -> None:
    """Create topics if they don't exist yet (name -> partition count)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    new = [
        NewTopic(
            name,
            num_partitions=partitions,
            replication_factor=1,  # one broker locally; production would use 3
            config={"retention.ms": str(retention_hours * 3600 * 1000)},
        )
        for name, partitions in topics.items()
    ]
    for name, future in admin.create_topics(new).items():
        try:
            future.result()
            log.info("created topic %s", name)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def make_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "options-ingestor",
            # Wait for the broker to confirm writes, and let the producer retry
            # without creating duplicates or reordering messages.
            "acks": "all",
            "enable.idempotence": True,
            # Batch messages for up to 20 ms and compress each batch.
            "linger.ms": 20,
            "compression.type": "lz4",
            "enable.metrics.push": False,
        }
    )


def _on_delivery(err: KafkaError | None, _msg: Message) -> None:
    if err is not None:
        stats["delivery_errors"] += 1
        log.error("delivery failed: %s", err)
    else:
        stats["delivered"] += 1


def publish(producer: Producer, topic: str, key: str, value: bytes) -> None:
    """Queue one message. If the local queue is full, wait for it to drain (backpressure)."""
    while True:
        try:
            producer.produce(topic, key=key.encode(), value=value, on_delivery=_on_delivery)
            break
        except BufferError:
            stats["queue_full"] += 1
            producer.poll(0.1)
    producer.poll(0)  # serve delivery callbacks


def make_consumer(bootstrap: str, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            # Offsets are committed by hand, only after rows are safely in
            # ClickHouse. A crash replays from the last commit: at-least-once.
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.metrics.push": False,
        }
    )


def encode_envelope(data: Any, received_us: int) -> bytes:
    return json.dumps({"received_us": received_us, "data": data}, separators=(",", ":")).encode()


def decode_envelope(value: bytes) -> tuple[Any, int]:
    envelope = json.loads(value)
    return envelope["data"], int(envelope["received_us"])


def wait_for_kafka(bootstrap: str, timeout_s: float = 120) -> None:
    """Block until the broker answers, so services can start before Kafka is ready."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            admin.list_topics(timeout=5)
            return
        except KafkaException as exc:
            if time.monotonic() > deadline:
                raise
            log.info("waiting for Kafka at %s (%s)", bootstrap, exc)
            time.sleep(2)
