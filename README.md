# options-analytics

[![CI](https://github.com/Suleyman040/options-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/Suleyman040/options-analytics/actions/workflows/ci.yml)

A real-time options analytics pipeline in Python. It streams live BTC option
quotes and trades from Deribit through **Kafka**, solves for implied volatility
and Greeks with a Black-76 model, stores the results in **ClickHouse** (with
reference data in **Postgres**), and serves analytics through a **FastAPI**
service and a live dashboard.

- **Pricing:** Black-76 prices and Greeks, and a Newton–bisection implied-vol solver, checked against Deribit's own published IV and Greeks.
- **Streaming:** an async websocket ingestor publishes to Kafka, keyed by instrument so each option's updates stay in order. A processor computes IV and Greeks and writes to ClickHouse in batches, with at-least-once delivery.
- **Storage:** ClickHouse `MergeTree` tables designed around the queries, with materialised views for one-minute bars. Postgres holds the reference data, with constraints.
- **Analytics:** volatility smile, term structure, realised vs implied vol (window functions), trades matched to the prior quote (`ASOF JOIN`), taker flow, pipeline latency and model checks.
- **Engineering:** Docker Compose, about 180 tests (every ClickHouse query runs against a real ClickHouse engine in CI), ruff, mypy `--strict`, GitHub Actions.

<!-- Add a screenshot of the dashboard here once it has run for a while:
![Dashboard](docs/dashboard.png) -->

## Architecture

```mermaid
flowchart LR
    D[(Deribit<br/>websocket + REST)]
    D -- ticker + trades --> I[ingestor<br/>asyncio]
    I -- "keyed by instrument" --> K[[Kafka<br/>tickers · trades]]
    K --> P[processor<br/>IV + Greeks]
    D -- option list --> R[reference job<br/>hourly]
    R --> PG[(Postgres<br/>instruments)]
    PG -- strikes, expiries --> P
    P -- batched inserts --> CH[(ClickHouse<br/>ticks · trades)]
    CH -- materialised views --> MV[(1-minute bars)]
    CH --> A[FastAPI<br/>+ dashboard]
    MV --> A
    PG --> A
```

| Component | What it does | Code |
|---|---|---|
| Ingestor | Subscribes to Deribit tickers for the nearest expiries and to every option trade. It answers heartbeats, reconnects with backoff and refreshes its subscriptions as options list and expire. | [`ingestor.py`](src/options_analytics/ingestor.py), [`deribit.py`](src/options_analytics/deribit.py) |
| Kafka | Buffers between ingestion and processing, so a slow or restarting consumer never loses market data. | [`kafka_io.py`](src/options_analytics/kafka_io.py) |
| Processor | Looks up strike and expiry, solves for IV from the mark price and the bid/ask mid, computes Greeks, and writes batches to ClickHouse. Commits Kafka offsets only after a batch is stored. | [`processor.py`](src/options_analytics/processor.py), [`transform.py`](src/options_analytics/transform.py), [`pricing.py`](src/options_analytics/pricing.py) |
| Reference job | Batch job that upserts Deribit's option list into Postgres and deactivates expired options. | [`reference.py`](src/options_analytics/reference.py), [`postgres_store.py`](src/options_analytics/postgres_store.py) |
| ClickHouse | Tick and trade tables plus materialised views for one-minute IV and index bars. | [`sql/clickhouse/`](src/options_analytics/sql/clickhouse) |
| API | Analytical queries and the dashboard. | [`api.py`](src/options_analytics/api.py), [`queries.py`](src/options_analytics/queries.py) |

## Quick start

You need [Docker Desktop](https://www.docker.com/products/docker-desktop/) (on Windows, with WSL 2).

```bash
git clone https://github.com/Suleyman040/options-analytics.git
cd options-analytics
docker compose up -d --build
```

The first build takes a few minutes. Then:

- **Dashboard:** <http://localhost:8000>, with data appearing within a minute.
- **API docs:** <http://localhost:8000/docs>
- **ClickHouse SQL console:** <http://localhost:8123/play> (user `app`, password `app`)
- **Logs:** `docker compose logs -f ingestor processor`
- **Stop:** `docker compose down`. Add `-v` to also delete the stored data.

What gets streamed is set in `.env` (copy `.env.example`). By default it's the nearest 4 expiries, strikes within ±25% of spot, and 100 ms ticker updates.

## Development

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

pytest                             # unit tests + ClickHouse SQL tests (via chdb)
ruff check . && ruff format --check . && mypy
```

The ClickHouse query tests use [chdb](https://github.com/chdb-io/chdb), which is ClickHouse embedded in Python. They run every query in `queries.py` and every migration on synthetic data whose true vols are known. chdb has no Windows build, so on Windows run those tests against the Docker ClickHouse instead:

```bash
docker compose up -d clickhouse postgres
RUN_CLICKHOUSE_TESTS=1 POSTGRES_TEST_DSN=postgresql://app:app@localhost:5432/options pytest
```

(In PowerShell, set the variables first: `$env:RUN_CLICKHOUSE_TESTS=1`.)

To run the Python services from your own terminal against the Docker infrastructure:

```bash
docker compose up -d kafka clickhouse postgres
python -m options_analytics.migrate
python -m options_analytics.reference
python -m options_analytics.ingestor      # each in its own terminal
python -m options_analytics.processor
uvicorn options_analytics.api:app --reload
```

## API

| Endpoint | Returns |
|---|---|
| `GET /smile?expiry=2026-10-30` | Latest IV per strike for one expiry: ours (from mark and from mid) and Deribit's |
| `GET /term-structure` | At-the-money IV for each expiry |
| `GET /vol/realised-vs-implied?expiry=…` | Rolling realised vol of the index against ATM implied vol, per minute |
| `GET /instruments/{name}/latest` | Latest tick for one option, with our Greeks next to Deribit's |
| `GET /instruments/{name}/iv-history` | One-minute IV bars (open, high, low, close) |
| `GET /trades/recent` | Recent trades, each matched to the quote before it |
| `GET /trades/flow` | Net delta bought by takers, per expiry |
| `GET /pipeline/latency` | Throughput, and p50/p90/p99 latency for each pipeline stage |
| `GET /pipeline/model-check` | Our IV and delta against Deribit's, by moneyness |
| `GET /pipeline/storage` | Rows and compressed size per table |
| `GET /expiries`, `GET /instruments` | Reference data from Postgres |

## Design decisions

**Black-76 rather than Black-Scholes.** Deribit prices each option off the forward for its expiry and quotes premiums in BTC. The pricer works in USD (BTC price × forward) with a zero rate, matching Deribit's convention. The tests reproduce Deribit's published IV and Greeks for a real captured ticker to four decimal places.

**The IV solver is Newton's method inside a bisection bracket.** Far from the money, vega is tiny, so a pure Newton step can overshoot to a negative or huge vol. Each iteration narrows a bracket that must contain the root and falls back to bisection when Newton would leave it. The processor warm-starts each solve from the option's previous IV, so most converge in one or two iterations. Prices outside no-arbitrage bounds (for example, a stale mark below intrinsic value) are stored as `NULL` rather than as a made-up number.

**Kafka between ingestion and processing.** The ingestor only receives, timestamps and publishes, so it keeps up with bursts. If ClickHouse or the processor is down, data waits in Kafka (24-hour retention) instead of being dropped. Messages are keyed by instrument: Kafka keeps each partition in order, so one option's updates are always processed in sequence, even with several processor instances. The producer uses `acks=all`, idempotence, 20 ms linger and lz4 compression.

**Delivery is at-least-once, and duplicates are removed by the storage layer.** The processor commits Kafka offsets only after a batch is in ClickHouse. A crash means re-reading some messages, never losing them. The tables are `ReplacingMergeTree`, keyed so that a replayed tick or trade collapses into the original row when ClickHouse merges parts. Queries that pick the latest value with `argMax` are unaffected by duplicates even before merges happen.

**Batched inserts.** ClickHouse is built for few large inserts. Every insert creates a part on disk that later has to be merged, so thousands of single-row inserts per second would overload it. The processor flushes every 5,000 rows or every second, whichever comes first. That trade-off between throughput and latency is visible in `/pipeline/latency`.

**The ClickHouse `ORDER BY` follows the queries.** Ticks are sorted by `(currency, expiry, strike, option_type, exchange_ts)`. The smile filters on expiry, a single option filters on the whole prefix, and history filters on time within an option, so each query reads a small slice of the sparse primary index. Tables are partitioned by day with a TTL (3 days for raw ticks, 90 days for bars) and `ttl_only_drop_parts`, so old data is dropped a whole partition at a time.

**Materialised views for history.** A ClickHouse materialised view is an insert trigger. Each block written to `option_ticks` is aggregated into one-minute bars in an `AggregatingMergeTree`, which stores partial states such as `argMaxState`, finished at query time with `-Merge` functions. History queries read one row per option per minute instead of up to 600 raw ticks.

**Postgres holds the reference data.** It's small, relational and changes slowly, and constraints catch bad data at write time (`CHECK (strike > 0)`, `CHECK (option_type IN ('call','put'))`). The hourly job upserts the whole chain in one statement using `unnest()`. It skips unchanged rows and counts inserts and updates with `RETURNING (xmax = 0)`. Expired options are deactivated rather than deleted, so they can still be joined to history.

**Trades are matched to quotes with an `ASOF JOIN`.** Each trade is joined to the last tick strictly before it, giving the bid, ask and our delta at the moment the trader acted. From there, `delta_traded` and net taker flow per expiry follow: the hedging pressure a market maker on the other side would be watching.

**The smile uses out-of-the-money options.** At each strike it shows the put below the forward and the call above it, the liquid side, as is market convention. Far out-of-the-money options with no bid, or priced under a few ticks, have almost no vega, so a few cents of price noise moves their IV by whole points.

## Project layout

```
src/options_analytics/
  pricing.py          Black-76, Greeks, implied-vol solver
  instruments.py      reference-data model and which options to stream
  deribit.py          REST client and self-healing websocket stream
  ingestor.py         websocket -> Kafka
  kafka_io.py         topics, producer and consumer settings
  transform.py        raw messages -> rows with IV and Greeks
  processor.py        Kafka -> ClickHouse, batching and offset commits
  reference.py        hourly batch job: Deribit -> Postgres
  postgres_store.py   reference-data queries
  clickhouse_store.py ClickHouse client behind a small interface
  queries.py          the analytical SQL
  api.py              FastAPI service
  migrate.py          creates schemas
  sql/                ClickHouse and Postgres DDL
  static/index.html   dashboard
tests/                pricing, transform, processor, websocket client, every API query
scripts/explore.py    first look at the raw Deribit data
```

## Limitations and next steps

- **Single-node infrastructure.** Production would run three Kafka brokers with replication factor 3, and a replicated ClickHouse cluster.
- **Exchange-to-ingest latency includes clock skew** between Deribit's clock and this machine, plus Deribit's 100 ms aggregation window. True one-way latency needs synchronised clocks (PTP or NTP) or exchange-colocated capture.
- **Schema evolution:** messages are JSON. A schema registry with Avro or Protobuf would make producer/consumer changes safe.
- **Model:** zero rates and no smile fitting. A natural extension is fitting a parametric smile (such as SVI) per expiry and storing the fitted parameters.
- **The API has no authentication.** It's intended for local use.

## License

MIT
