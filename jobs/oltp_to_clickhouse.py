import logging
import sys
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, decode, from_json, to_timestamp,
    regexp_extract, when, lit
)
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if len(sys.argv) != 5:
    logger.error(
        "Usage: oltp_to_clickhouse.py <target_table> <ch_jdbc_url> <user> <password>"
    )
    sys.exit(1)

target_table = sys.argv[1]
jdbc_url_target = sys.argv[2]
ch_user = sys.argv[3]
ch_password = sys.argv[4]

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "db.crypto.public.raw_trades"
CHECKPOINT_DIR = f"/opt/spark/work-dir/checkpoints/{target_table}"

spark = (
    SparkSession.builder.appName(f"Kafka2ClickHouse_{target_table}")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")
logger.info(f"Spark session created. Reading from {KAFKA_TOPIC}")

# === ОБНОВЛЕННАЯ СХЕМА: добавили symbol ===
trade_schema = StructType([
    StructField("symbol", StringType(), True),  # ← НОВОЕ ПОЛЕ
    StructField("trade_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("trade_time", StringType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("loaded_at", StringType(), True),
])

df_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "5000")
    .option("kafka.request.timeout.ms", "30000")
    .load()
)

logger.info(f"📡 Kafka stream initialized. Schema: {df_kafka.schema}")

df_parsed = df_kafka.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    from_json(decode(col("value"), "UTF-8"), trade_schema).alias("data"),
)

# === ДЕНОРМАЛИЗАЦИЯ: вычисляем дополнительные поля ===
df_flat = df_parsed.select(
    # Базовые поля
    col("data.symbol").alias("symbol"),
    col("data.trade_id").alias("trade_id"),
    col("data.price").cast("Double").alias("price"),
    col("data.quantity").cast("Double").alias("quantity"),
    to_timestamp(col("data.trade_time")).alias("trade_time"),
    col("data.is_buyer_maker").alias("is_buyer_maker"),
    
    # === Денормализованные поля ===
    
    # Объём сделки в кворте (цена * количество)
    (col("price") * col("quantity")).alias("trade_value"),
    
    # Извлекаем base/quote активы из символа: BTCUSDT → BTC, USDT
    regexp_extract(col("symbol"), r"^([A-Z]+)([A-Z]+)$", 1).alias("base_asset"),
    regexp_extract(col("symbol"), r"^([A-Z]+)([A-Z]+)$", 2).alias("quote_asset"),
    
    # Флаг крупной сделки (> $10,000)
    ((col("price") * col("quantity")) > lit(10000)).alias("is_large_trade"),
    
    # Метаданные
    col("kafka_timestamp").alias("kafka_loaded_at"),
    col("data.loaded_at").cast("Timestamp").alias("source_loaded_at"),
    
).filter(
    (col("symbol").isNotNull()) &
    (col("trade_id").isNotNull()) &
    (col("price").isNotNull()) &
    (col("quantity").isNotNull()) &
    (col("trade_time").isNotNull()) &
    (col("price") > 0) &  # валидация: цена положительная
    (col("quantity") > 0)   # валидация: количество положительное
)


def write_to_clickhouse(df: DataFrame, epoch_id: int):
    try:
        count = df.count()
        if count == 0:
            logger.info(f"⏭️  Batch {epoch_id}: no data to write")
            return

        logger.info(f"📤 Batch {epoch_id}: writing {count} rows")

        # === ОБНОВЛЕННАЯ customSchema: добавили новые поля ===
        df.write.format("jdbc").option("url", jdbc_url_target).option(
            "dbtable", f"default.{target_table}"
        ).option("user", ch_user).option("password", ch_password).option(
            "driver", "com.clickhouse.jdbc.ClickHouseDriver"
        ).option(
            "customSchema",
            "symbol String, trade_id String, price Double, quantity Double, "
            "trade_time Timestamp, is_buyer_maker Boolean, "
            "trade_value Double, base_asset String, quote_asset String, "
            "is_large_trade Boolean, kafka_loaded_at Timestamp, source_loaded_at Timestamp",
        ).option("batchsize", "10000").option("isolationLevel", "NONE").option(
            "truncate", "false"
        ).mode("append").save()

        logger.info(f"✅ Batch {epoch_id}: successfully written")

    except Exception as e:
        logger.error(f"❌ Batch {epoch_id}: write failed - {str(e)}")
        raise


query = (
    df_flat.writeStream.outputMode("append")
    .foreachBatch(write_to_clickhouse)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .option("failOnDataLoss", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

logger.info(
    f"🎯 Spark Streaming started! Topic: {KAFKA_TOPIC} "
    f"→ ClickHouse: default.{target_table}"
)
logger.info(f"📁 Checkpoint: {CHECKPOINT_DIR}")

query.awaitTermination()