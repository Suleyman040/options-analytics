"""
All settings in one place, read from environment variables (or a .env file).

Defaults point at services on localhost, which is what you get when you run
the Python code on your own machine against `docker compose up` infrastructure.
Inside Docker Compose the same variables are overridden with service names
(e.g. KAFKA_BOOTSTRAP_SERVERS=kafka:19092). See .env.example.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Market data ---------------------------------------------------------
    currency: str = "BTC"
    deribit_rest_url: str = "https://www.deribit.com/api/v2"
    deribit_ws_url: str = "wss://www.deribit.com/ws/api/v2"
    # Which options to stream: the nearest N expiries, strikes within ±X of spot.
    # Streaming the whole chain (~900 options) works but stores far more data.
    max_expiries: int = 4
    strike_range: float = 0.25
    # Deribit aggregates ticker updates: "100ms" (fast) or "agg2" (every 2 s).
    ticker_interval: str = "100ms"
    # How often the ingestor re-checks which options to subscribe to.
    selection_refresh_s: int = 900

    # --- Kafka ---------------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:9092"
    ticker_topic: str = "deribit.option.tickers"
    trade_topic: str = "deribit.option.trades"
    topic_partitions: int = 6
    consumer_group: str = "options-processor"

    # --- ClickHouse ----------------------------------------------------------
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "app"
    clickhouse_password: str = "app"
    clickhouse_database: str = "options"

    # --- Postgres ------------------------------------------------------------
    postgres_dsn: str = "postgresql://app:app@localhost:5432/options"

    # --- Processor -----------------------------------------------------------
    # Flush to ClickHouse when the buffer reaches batch_size rows or is
    # flush_interval_s old, whichever comes first.
    batch_size: int = 5000
    flush_interval_s: float = 1.0

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
