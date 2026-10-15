"""Logging setup shared by every service."""

import logging


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # librdkafka and httpx are chatty at INFO.
    logging.getLogger("httpx").setLevel(logging.WARNING)
