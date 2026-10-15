"""
The analytical SQL behind the API, all run on ClickHouse.

Parameters use ClickHouse's {name:Type} syntax and are sent separately from
the query text. tests/test_queries.py runs every query here against a real
ClickHouse engine (chdb) with synthetic data.
"""

# Latest IV per strike for one expiry: the volatility smile.
# argMax(x, ts) = "the value of x on the row with the largest ts", i.e. the latest.
# Market convention: use the out-of-the-money option at each strike (puts below the
# forward, calls above), because OTM options are the liquid ones.
SMILE = """
WITH latest AS (
    SELECT
        strike,
        option_type,
        argMax(instrument_name, exchange_ts)   AS name,
        argMax(underlying_price, exchange_ts)  AS fwd,
        argMax(iv_mark, exchange_ts)           AS last_iv_mark,
        argMax(iv_mid, exchange_ts)            AS last_iv_mid,
        argMax(deribit_mark_iv, exchange_ts)   AS last_deribit_iv,
        argMax(delta, exchange_ts)             AS last_delta,
        max(exchange_ts)                       AS last_ts
    FROM option_ticks
    WHERE currency = {currency:String}
      AND toDate(expiry) = {expiry:Date}
      AND exchange_ts >= now64(3) - INTERVAL {minutes:UInt32} MINUTE
    GROUP BY strike, option_type
)
SELECT
    name                    AS instrument_name,
    strike,
    toString(option_type)   AS option_type,
    fwd                     AS forward,
    last_iv_mark            AS iv_mark,
    last_iv_mid             AS iv_mid,
    last_deribit_iv         AS deribit_mark_iv,
    last_delta              AS delta,
    last_ts                 AS as_of
FROM latest
WHERE (option_type = 'put' AND strike < fwd) OR (option_type = 'call' AND strike >= fwd)
ORDER BY strike
"""

# At-the-money IV for every expiry: the term structure.
# argMin(x, abs(strike - forward)) picks x at the strike nearest the forward.
TERM_STRUCTURE = """
WITH latest AS (
    SELECT
        expiry,
        strike,
        argMax(underlying_price, exchange_ts) AS fwd,
        argMax(iv_mark, exchange_ts)          AS iv
    FROM option_ticks
    WHERE currency = {currency:String}
      AND option_type = 'call'
      AND exchange_ts >= now64(3) - INTERVAL {minutes:UInt32} MINUTE
    GROUP BY expiry, strike
)
SELECT
    expiry,
    dateDiff('second', now(), expiry) / 86400   AS days_to_expiry,
    argMin(fwd, abs(strike - fwd))              AS forward,
    argMin(strike, abs(strike - fwd))           AS atm_strike,
    argMin(iv, abs(strike - fwd))               AS atm_iv
FROM latest
GROUP BY expiry
ORDER BY expiry
"""

# The most recent tick for one option. Filtering on the full ORDER BY prefix
# (currency, expiry, strike, option_type) lets ClickHouse jump straight to it.
LATEST_TICK = """
SELECT
    instrument_name, exchange_ts, time_to_expiry, underlying_price, index_price,
    bid_price, ask_price, mark_price, iv_mark, iv_mid, delta, gamma, vega, theta,
    deribit_mark_iv, deribit_bid_iv, deribit_ask_iv,
    deribit_delta, deribit_gamma, deribit_vega, deribit_theta
FROM option_ticks
WHERE currency = {currency:String}
  AND expiry = toDateTime({expiry_s:UInt32}, 'UTC')
  AND strike = {strike:Float64}
  AND option_type = {option_type:String}
  AND exchange_ts >= now64(3) - INTERVAL 1 DAY
ORDER BY exchange_ts DESC
LIMIT 1
"""

# One-minute IV bars from the materialised view. Parts may not be merged yet,
# so the query finishes the aggregation: -Merge for states, min/max/sum for simple ones.
IV_HISTORY = """
SELECT
    minute,
    argMinMerge(iv_open)            AS iv_open,
    max(iv_high)                    AS iv_high,
    min(iv_low)                     AS iv_low,
    argMaxMerge(iv_close)           AS iv_close,
    argMaxMerge(underlying_close)   AS underlying_close,
    sum(ticks)                      AS ticks
FROM option_iv_1m
WHERE currency = {currency:String}
  AND expiry = toDateTime({expiry_s:UInt32}, 'UTC')
  AND strike = {strike:Float64}
  AND option_type = {option_type:String}
  AND minute >= now() - INTERVAL {hours:UInt32} HOUR
GROUP BY minute
ORDER BY minute
"""

# Realised vs implied volatility, minute by minute.
# Realised: standard deviation of 1-minute log returns of the index over a rolling
# window, annualised with √(minutes per year), since crypto trades 24/7. It stays
# NULL until the window holds enough returns to mean anything.
# Implied: ATM IV for the chosen expiry from the 1-minute bars.
# lagInFrame(px) is the previous minute's price. The first row has no previous
# price, so it's dropped (n > 1).
# Frame sizes can't be query parameters; the API validates them as integers and
# formats them in.
REALISED_VS_IMPLIED = """
WITH
index_bars AS (
    SELECT minute, argMaxMerge(index_close) AS px
    FROM index_1m
    WHERE currency = {{currency:String}} AND minute >= now() - INTERVAL {{hours:UInt32}} HOUR
    GROUP BY minute
),
returns AS (
    SELECT minute,
           log(px / lagInFrame(px, 1, px) OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS r,
           row_number() OVER (ORDER BY minute) AS n
    FROM index_bars
),
realised AS (
    SELECT minute,
           if(count() OVER w >= {min_obs}, stddevPop(r) OVER w * sqrt(525600), NULL) AS realised_vol
    FROM returns
    WHERE n > 1
    WINDOW w AS (ORDER BY minute ROWS BETWEEN {window} PRECEDING AND CURRENT ROW)
),
atm AS (
    SELECT minute, argMin(iv, abs(strike - fwd)) AS atm_iv
    FROM (
        SELECT minute, strike, argMaxMerge(iv_close) AS iv, argMaxMerge(underlying_close) AS fwd
        FROM option_iv_1m
        WHERE currency = {{currency:String}}
          AND toDate(expiry) = {{expiry:Date}}
          AND option_type = 'call'
          AND minute >= now() - INTERVAL {{hours:UInt32}} HOUR
        GROUP BY minute, strike
    )
    GROUP BY minute
)
SELECT r.minute AS minute, r.realised_vol AS realised_vol, a.atm_iv AS atm_iv
FROM realised AS r
LEFT JOIN atm AS a ON r.minute = a.minute
ORDER BY minute
SETTINGS join_use_nulls = 1
"""

# Trades matched to the quote just before each one, with an ASOF JOIN.
# An ASOF JOIN matches on the equality conditions, then takes the closest row
# satisfying the inequality: here the last tick strictly before the trade, i.e.
# the market the trader saw. Pre-filtering the right side by time matters: it's
# loaded into memory for the join.
_TRADES_WITH_QUOTES = """
SELECT
    t.trade_ts                                        AS trade_ts,
    t.trade_id                                        AS trade_id,
    t.instrument_name                                 AS instrument_name,
    t.expiry                                          AS expiry,
    toString(t.direction)                             AS direction,
    t.amount                                          AS amount,
    t.price                                           AS price,
    q.bid_price                                       AS bid_price,
    q.ask_price                                       AS ask_price,
    q.mark_price                                      AS mark_price,
    t.deribit_iv                                      AS trade_iv,
    q.iv_mark                                         AS mark_iv,
    q.delta                                           AS delta,
    if(t.direction = 'buy', 1, -1) * t.amount * q.delta AS delta_traded,
    dateDiff('millisecond', q.exchange_ts, t.trade_ts) AS quote_age_ms
FROM option_trades AS t
ASOF LEFT JOIN (
    SELECT currency, expiry, strike, option_type, exchange_ts,
           bid_price, ask_price, mark_price, iv_mark, delta
    FROM option_ticks
    WHERE currency = {currency:String}
      AND exchange_ts >= now64(3) - INTERVAL {minutes:UInt32} MINUTE - INTERVAL 10 MINUTE
) AS q
ON  t.currency = q.currency AND t.expiry = q.expiry
AND t.strike = q.strike AND t.option_type = q.option_type
AND t.trade_ts > q.exchange_ts
WHERE t.currency = {currency:String}
  AND t.trade_ts >= now64(3) - INTERVAL {minutes:UInt32} MINUTE
"""

RECENT_TRADES = f"""
{_TRADES_WITH_QUOTES}
ORDER BY trade_ts DESC
LIMIT {{limit:UInt32}}
SETTINGS join_use_nulls = 1
"""

# Net delta bought by takers per expiry. Takers lift offers and hit bids, so
# market makers end up holding the opposite position: this is the hedging
# pressure a market maker would watch.
TRADE_FLOW = f"""
SELECT
    expiry,
    count()                                 AS trades,
    sum(amount)                             AS contracts,
    sumIf(amount, direction = 'buy')        AS bought,
    sumIf(amount, direction = 'sell')       AS sold,
    sum(delta_traded)                       AS net_delta,
    countIf(delta_traded IS NULL)           AS trades_without_quote
FROM ({_TRADES_WITH_QUOTES})
GROUP BY expiry
ORDER BY expiry
SETTINGS join_use_nulls = 1
"""

# How long each stage takes, as p50/p90/p99 in milliseconds.
# exchange -> ingest includes network time, Deribit's 100 ms aggregation window
# and any clock difference between Deribit and this machine.
LATENCY = """
SELECT
    count()                                 AS ticks,
    count() / ({minutes:UInt32} * 60)       AS ticks_per_second,
    arrayMap(x -> x / 1000, quantiles(0.5, 0.9, 0.99)(dateDiff('microsecond', exchange_ts, received_ts)))  AS exchange_to_ingest_ms,
    arrayMap(x -> x / 1000, quantiles(0.5, 0.9, 0.99)(dateDiff('microsecond', received_ts, processed_ts))) AS ingest_to_process_ms,
    arrayMap(x -> x / 1000, quantiles(0.5, 0.9, 0.99)(dateDiff('microsecond', processed_ts, inserted_ts))) AS process_to_insert_ms
FROM option_ticks
WHERE currency = {currency:String}
  AND exchange_ts >= now64(3) - INTERVAL {minutes:UInt32} MINUTE
"""

# Does our Black-76 implementation agree with the exchange? Differences in vol
# points (1 point = 1% vol), bucketed by moneyness (strike / forward).
MODEL_CHECK = """
SELECT
    multiIf(moneyness < 0.90, '< 0.90',
            moneyness < 0.97, '0.90 – 0.97',
            moneyness <= 1.03, '0.97 – 1.03 (ATM)',
            moneyness <= 1.10, '1.03 – 1.10',
            '> 1.10')                                       AS bucket,
    count()                                                 AS ticks,
    countIf(iv_mark IS NULL)                                AS solver_failures,
    avg(abs(iv_mark - deribit_mark_iv)) * 100               AS mean_abs_iv_diff_pts,
    quantile(0.99)(abs(iv_mark - deribit_mark_iv)) * 100    AS p99_abs_iv_diff_pts,
    avg(abs(delta - deribit_delta))                         AS mean_abs_delta_diff
FROM (
    SELECT strike / underlying_price AS moneyness, iv_mark, deribit_mark_iv, delta, deribit_delta
    FROM option_ticks
    WHERE currency = {currency:String}
      AND exchange_ts >= now64(3) - INTERVAL {minutes:UInt32} MINUTE
)
GROUP BY bucket
ORDER BY min(moneyness)
"""

TABLE_SIZES = """
SELECT table, sum(rows) AS rows, sum(data_compressed_bytes) AS compressed_bytes,
       sum(data_uncompressed_bytes) AS uncompressed_bytes
FROM system.parts
WHERE database = currentDatabase() AND active
GROUP BY table
ORDER BY table
"""
