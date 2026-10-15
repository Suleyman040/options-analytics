-- One-minute IV bars per option, maintained automatically by a materialised view.
--
-- A ClickHouse materialised view is an insert trigger: every block inserted into
-- option_ticks is aggregated by the SELECT below and the result is inserted into
-- option_iv_1m. History queries then read one row per option per minute instead of up
-- to 600 raw ticks, and bars are kept for 90 days while raw ticks are dropped after 3.
--
-- AggregatingMergeTree stores partial aggregation *states* (e.g. "the latest value
-- seen so far and its timestamp"). Merges combine states, and queries finish them
-- with -Merge functions: argMaxMerge(iv_close). SimpleAggregateFunction is the
-- light version for functions whose state is just the value (min, max, sum).
CREATE TABLE IF NOT EXISTS option_iv_1m
(
    currency          LowCardinality(String),
    expiry            DateTime('UTC'),
    strike            Float64,
    option_type       Enum8('call' = 1, 'put' = 2),
    minute            DateTime('UTC'),

    iv_open           AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    iv_close          AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    iv_low            SimpleAggregateFunction(min, Float64),
    iv_high           SimpleAggregateFunction(max, Float64),
    underlying_close  AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    ticks             SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (currency, expiry, strike, option_type, minute)
TTL minute + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS option_iv_1m_mv TO option_iv_1m AS
SELECT
    currency,
    expiry,
    strike,
    option_type,
    toStartOfMinute(exchange_ts)                       AS minute,
    argMinState(assumeNotNull(iv_mark), exchange_ts)   AS iv_open,
    argMaxState(assumeNotNull(iv_mark), exchange_ts)   AS iv_close,
    min(assumeNotNull(iv_mark))                        AS iv_low,
    max(assumeNotNull(iv_mark))                        AS iv_high,
    argMaxState(underlying_price, exchange_ts)         AS underlying_close,
    toUInt64(count())                                  AS ticks
FROM option_ticks
WHERE iv_mark IS NOT NULL
GROUP BY currency, expiry, strike, option_type, minute;
