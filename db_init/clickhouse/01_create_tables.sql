CREATE TABLE IF NOT EXISTS bronze_crypto_trades (
    symbol LowCardinality(String),
    trade_id String,
    
    price Float64,
    quantity Float64,
    trade_time DateTime64(3),
    is_buyer_maker UInt8,
    
    trade_value Float64,
    base_asset LowCardinality(String),
    quote_asset LowCardinality(String),
    is_large_trade UInt8,
    
    kafka_loaded_at DateTime64(3),
    source_loaded_at DateTime64(3),
    ingested_at DateTime64(3) DEFAULT now64(3)
    
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (symbol, trade_time, trade_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS silver_crypto_trades (
    symbol LowCardinality(String),
    base_asset LowCardinality(String),
    quote_asset LowCardinality(String),
    
    trade_id String,
    price Float64,
    quantity Float64,
    trade_time DateTime64(3),
    hour_start DateTime64(3),
    is_buyer_maker UInt8,
    
    trade_value Float64,
    is_large_trade UInt8,
    
    ingested_at DateTime64(3) DEFAULT now64(3)
    
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (symbol, hour_start, trade_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mart_hourly_stats (
    hour_start DateTime64(3),
    symbol LowCardinality(String),
    base_asset LowCardinality(String),
    quote_asset LowCardinality(String),
    
    total_trades SimpleAggregateFunction(sum, UInt64),
    total_volume SimpleAggregateFunction(sum, Float64),
    avg_price SimpleAggregateFunction(avg, Float64),
    min_price SimpleAggregateFunction(min, Float64),
    max_price SimpleAggregateFunction(max, Float64),
    
    open_price Float64,
    close_price Float64,
    
    large_trade_count SimpleAggregateFunction(sum, UInt64),
    buyer_maker_ratio SimpleAggregateFunction(avg, Float64)
    
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(hour_start)
ORDER BY (symbol, hour_start)
SETTINGS index_granularity = 8192;