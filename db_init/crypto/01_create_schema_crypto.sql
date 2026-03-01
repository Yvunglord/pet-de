CREATE TABLE IF NOT EXISTS raw_trades (
    symbol TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    price NUMERIC NOT NULL,
    quantity NUMERIC NOT NULL,
    trade_time TIMESTAMPTZ NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_trades_symbol_time ON raw_trades(symbol, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_raw_trades_time_global ON raw_trades(trade_time DESC);

COMMENT ON TABLE raw_trades IS 'Raw crypto trades from Binance WebSocket (normalized)';
COMMENT ON COLUMN raw_trades.symbol IS 'Trading pair symbol, e.g. BTCUSDT';
COMMENT ON COLUMN raw_trades.is_buyer_maker IS 'True if taker was buyer (sell-side liquidity)';

CREATE TABLE IF NOT EXISTS symbol_meta (
    symbol TEXT PRIMARY KEY,           -- "BTCUSDT"
    base_asset TEXT NOT NULL,          -- "BTC"
    quote_asset TEXT NOT NULL,         -- "USDT"
    price_precision INT NOT NULL,      -- кол-во знаков после запятой для цены
    qty_precision INT NOT NULL,        -- кол-во знаков для количества
    min_notional NUMERIC(20, 8) NOT NULL, -- мин. сумма ордера в кворте
    is_active BOOLEAN DEFAULT true,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO symbol_meta (symbol, base_asset, quote_asset, price_precision, qty_precision, min_notional)
VALUES 
    ('BTCUSDT', 'BTC', 'USDT', 2, 6, 10.0),
    ('ETHUSDT', 'ETH', 'USDT', 2, 5, 10.0)
ON CONFLICT (symbol) DO UPDATE SET
    updated_at = CURRENT_TIMESTAMP;

COMMENT ON TABLE symbol_meta IS 'Metadata for trading symbols (denormalization source)';