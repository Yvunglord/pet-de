import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, decode, from_json, lit, to_timestamp
from pyspark.sql.types import (BooleanType, DoubleType, StringType,
                               StructField, StructType, TimestampType)

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

trade_schema = StructType(
    [
        StructField("trade_id", StringType(), True),
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("trade_time", StringType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("loaded_at", StringType(), True),
    ]
)

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

df_flat = df_parsed.select(
    col("data.trade_id").alias("trade_id"),
    col("data.price").cast("Double").alias("price"),
    col("data.quantity").cast("Double").alias("quantity"),
    to_timestamp(col("data.trade_time")).alias("trade_time"),
    col("data.is_buyer_maker").alias("is_buyer_maker"),
    col("kafka_timestamp").alias("loaded_at"),
).filter(
    (col("trade_id").isNotNull())
    & (col("price").isNotNull())
    & (col("trade_time").isNotNull())
)


def write_to_clickhouse(df: DataFrame, epoch_id: int):
    try:
        count = df.count()
        if count == 0:
            logger.info(f"⏭️  Batch {epoch_id}: no data to write")
            return

        logger.info(f"📤 Batch {epoch_id}: writing {count} rows")

        # Явно указываем схему для маппинга типов
        df.write.format("jdbc").option("url", jdbc_url_target).option(
            "dbtable", f"default.{target_table}"
        ).option("user", ch_user).option("password", ch_password).option(
            "driver", "com.clickhouse.jdbc.ClickHouseDriver"
        ).option(
            "customSchema",
            "trade_id String, price Double, quantity Double, trade_time Timestamp, is_buyer_maker Boolean, loaded_at Timestamp",
        ).option(
            "batchsize", "10000"
        ).option(
            "isolationLevel", "NONE"
        ).option(
            "truncate", "false"
        ).mode(
            "append"
        ).save()

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
    f"🎯 Spark Streaming started! Topic: {KAFKA_TOPIC} → ClickHouse: default.{target_table}"
)
logger.info(f"📁 Checkpoint: {CHECKPOINT_DIR}")

query.awaitTermination()
