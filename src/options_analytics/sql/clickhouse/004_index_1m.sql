-- One-minute closing prices of the spot index (e.g. btc_usd), used for realised volatility.
-- Every option ticker carries the current index price, so this view samples it from there.
CREATE TABLE IF NOT EXISTS index_1m
(
    currency     LowCardinality(String),
    minute       DateTime('UTC'),
    index_close  AggregateFunction(argMax, Float64, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (currency, minute)
TTL minute + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS index_1m_mv TO index_1m AS
SELECT
    currency,
    toStartOfMinute(exchange_ts)            AS minute,
    argMaxState(index_price, exchange_ts)   AS index_close
FROM option_ticks
WHERE index_price > 0
GROUP BY currency, minute;
