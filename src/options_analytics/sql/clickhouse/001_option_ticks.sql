-- Every ticker update for every streamed option, with our IV and Greeks next to Deribit's.
--
-- Design notes:
-- * ORDER BY is the primary key and the on-disk sort order. Queries filter by currency,
--   then expiry (the smile), then strike and type (one option), then time, so the key
--   goes from coarse to fine in that order and every query can skip most of the data.
-- * ReplacingMergeTree drops rows with the same ORDER BY key when parts merge. Kafka
--   delivery is at-least-once, so a replay after a crash would otherwise store duplicates.
-- * Daily partitions plus a TTL let ClickHouse drop whole days at once (ttl_only_drop_parts).
-- * LowCardinality stores repeated strings like instrument names as a dictionary.
-- * Nullable columns hold values that may genuinely be missing: an empty side of the book,
--   or an IV the solver couldn't find because the price broke no-arbitrage bounds.
CREATE TABLE IF NOT EXISTS option_ticks
(
    instrument_name   LowCardinality(String),
    currency          LowCardinality(String),
    expiry            DateTime('UTC'),
    strike            Float64,
    option_type       Enum8('call' = 1, 'put' = 2),

    exchange_ts       DateTime64(3, 'UTC'),
    received_ts       DateTime64(6, 'UTC'),
    processed_ts      DateTime64(6, 'UTC'),
    inserted_ts       DateTime64(6, 'UTC') DEFAULT now64(6),

    time_to_expiry    Float64,
    underlying_price  Float64,
    index_price       Float64,
    bid_price         Nullable(Float64),
    ask_price         Nullable(Float64),
    mark_price        Float64,
    bid_amount        Float64,
    ask_amount        Float64,
    open_interest     Float64,

    iv_mark           Nullable(Float64),
    iv_mid            Nullable(Float64),
    delta             Nullable(Float64),
    gamma             Nullable(Float64),
    vega              Nullable(Float64),
    theta             Nullable(Float64),

    deribit_mark_iv   Nullable(Float64),
    deribit_bid_iv    Nullable(Float64),
    deribit_ask_iv    Nullable(Float64),
    deribit_delta     Nullable(Float64),
    deribit_gamma     Nullable(Float64),
    deribit_vega      Nullable(Float64),
    deribit_theta     Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(exchange_ts)
ORDER BY (currency, expiry, strike, option_type, exchange_ts)
TTL toDateTime(exchange_ts) + INTERVAL 3 DAY
SETTINGS ttl_only_drop_parts = 1;
