CREATE TABLE IF NOT EXISTS raw_trades (
    trade_id TEXT NOT NULL,
    price NUMERIC NOT NULL,
    quantity NUMERIC NOT NULL,
    trade_time TIMESTAMPTZ NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_trades_time ON raw_trades(trade_time DESC);

COMMENT ON TABLE raw_trades IS 'Raw crypto trades from Binance API';
