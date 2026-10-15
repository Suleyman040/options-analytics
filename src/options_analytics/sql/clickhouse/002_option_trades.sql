-- Every option trade on Deribit for the currency (the whole chain, not just streamed strikes).
-- trade_id is in the key so replays deduplicate. direction is the taker's side.
CREATE TABLE IF NOT EXISTS option_trades
(
    trade_id          String,
    trade_seq         UInt64,
    instrument_name   LowCardinality(String),
    currency          LowCardinality(String),
    expiry            DateTime('UTC'),
    strike            Float64,
    option_type       Enum8('call' = 1, 'put' = 2),

    trade_ts          DateTime64(3, 'UTC'),
    received_ts       DateTime64(6, 'UTC'),
    processed_ts      DateTime64(6, 'UTC'),
    inserted_ts       DateTime64(6, 'UTC') DEFAULT now64(6),

    price             Float64,
    amount            Float64,
    direction         Enum8('buy' = 1, 'sell' = 2),
    deribit_iv        Nullable(Float64),
    mark_price        Float64,
    index_price       Float64,
    tick_direction    Int8,
    block_trade_id    String,
    is_liquidation    UInt8
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(trade_ts)
ORDER BY (currency, expiry, strike, option_type, trade_ts, trade_id)
TTL toDateTime(trade_ts) + INTERVAL 30 DAY
SETTINGS ttl_only_drop_parts = 1;
