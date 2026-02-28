CREATE TABLE IF NOT EXISTS bronze_crypto_trades (
    trade_id String,
    price Float64,
    quantity Float64,
    trade_time DateTime64(3),
    is_buyer_maker UInt8,
    loaded_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (trade_time, trade_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS silver_crypto_trades (
    trade_id String,
    price Float64,
    quantity Float64,
    trade_time DateTime64(3),
    hour_start DateTime64(3),
    is_buyer_maker UInt8,
    ingested_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (hour_start, trade_time);

CREATE TABLE IF NOT EXISTS mart_hourly_stats (
    hour_start DateTime64(3),
    total_trades UInt64,
    total_volume Float64,
    avg_price Float64,
    min_price Float64,
    max_price Float64,
    open_price Float64,
    close_price Float64
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(hour_start)
ORDER BY (hour_start);