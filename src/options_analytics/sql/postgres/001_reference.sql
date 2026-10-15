-- Reference data: one row per option Deribit has listed.
-- Constraints make bad data fail loudly at write time instead of surfacing later as wrong numbers.
CREATE TABLE IF NOT EXISTS instruments (
    instrument_name   TEXT           PRIMARY KEY,
    currency          TEXT           NOT NULL,
    option_type       TEXT           NOT NULL CHECK (option_type IN ('call', 'put')),
    strike            NUMERIC(20, 8) NOT NULL CHECK (strike > 0),
    expiry            TIMESTAMPTZ    NOT NULL,
    tick_size         NUMERIC(20, 8) NOT NULL CHECK (tick_size > 0),
    contract_size     NUMERIC(20, 8) NOT NULL,
    min_trade_amount  NUMERIC(20, 8) NOT NULL,
    is_active         BOOLEAN        NOT NULL,
    first_seen_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ    NOT NULL DEFAULT now(),
    UNIQUE (currency, expiry, strike, option_type)
);

-- Most lookups are "active options for a currency, by expiry": a partial index covers exactly those rows.
CREATE INDEX IF NOT EXISTS instruments_active_by_expiry
    ON instruments (currency, expiry) WHERE is_active;

-- An audit log of the batch job, so you can see when reference data last changed.
CREATE TABLE IF NOT EXISTS reference_refreshes (
    id                BIGSERIAL   PRIMARY KEY,
    currency          TEXT        NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ NOT NULL,
    instruments_seen  INTEGER     NOT NULL,
    inserted          INTEGER     NOT NULL,
    updated           INTEGER     NOT NULL,
    deactivated       INTEGER     NOT NULL
);
