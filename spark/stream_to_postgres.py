import os
import signal
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    from_json,
    lit,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.storagelevel import StorageLevel
import pyspark.sql.functions as F


# =========================
# Env config
# =========================
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "spark_stream_raw_prices")

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    os.getenv("KAFKA_BOOTSTRAP", "kafka:29092"),
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "commodity_prices")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "commodities")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

STREAM_INSTANCE_ID = os.getenv("STREAM_INSTANCE_ID", "stream1")

PG_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
PG_DRIVER = "org.postgresql.Driver"

CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/home/spark/checkpoints/raw_prices")

TARGET_TABLE = os.getenv("PG_TARGET_TABLE", "public.raw_prices")

STAGING_SCHEMA = os.getenv("PG_STAGING_SCHEMA", "ingest")
STAGING_BASE = os.getenv("PG_STAGING_TABLE_BASE", "raw_prices_ingest")
STAGING_PREFIX = f"{STAGING_SCHEMA}.{STAGING_BASE}_"

DLQ_TABLE = os.getenv("PG_DLQ_TABLE", "monitoring.dead_letter_events")

KAFKA_MAX_OFFSETS_PER_TRIGGER = os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "5000")

# =========================
# Kafka options
# =========================
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest").lower()
if STARTING_OFFSETS not in ("earliest", "latest"):
    STARTING_OFFSETS = "earliest"

FAIL_ON_DATA_LOSS = os.getenv("KAFKA_FAIL_ON_DATA_LOSS", "false").lower()
if FAIL_ON_DATA_LOSS not in ("true", "false"):
    FAIL_ON_DATA_LOSS = "false"

# =========================
# Schema of incoming JSON
# =========================
event_schema = StructType([
    StructField("schema_version", IntegerType(), True),
    StructField("event_id", StringType(), True),
    StructField("commodity", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("ingest_interval_sec", IntegerType(), True),
])


def _exec_sql_via_jdbc(spark: SparkSession, sql_text: str) -> None:
    """
    Execute SQL on Postgres using JVM JDBC (no extra Python libs required).
    Uses the JVM driver already loaded by Spark, avoiding a psycopg2 dependency.
    """
    jvm = spark._sc._jvm
    DriverManager = jvm.java.sql.DriverManager
    Properties = jvm.java.util.Properties

    props = Properties()
    props.setProperty("user", POSTGRES_USER)
    props.setProperty("password", POSTGRES_PASSWORD)

    conn = None
    stmt = None
    try:
        conn = DriverManager.getConnection(PG_URL, props)
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        stmt.execute(sql_text)
        conn.commit()
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if stmt is not None:
            stmt.close()
        if conn is not None:
            conn.close()


def make_foreach_batch(spark: SparkSession):
    """
    foreachBatch handler:
    - persist microbatch once
    - compute good/bad counts with a single agg
    - write bad rows to DLQ (best-effort)
    - write good rows to a per-batch staging table and insert idempotently into target
    - DO NOT drop staging tables here (durability). Cleanup is handled separately.
    """
    def foreach_batch(batch_df, batch_id: int):
        if batch_df.isEmpty():
            print(f"[spark-stream] batch_id={batch_id} rows=0 skip=true", flush=True)
            return

        t0 = time.time()

        jdbc_props = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": PG_DRIVER,
        }

        persisted = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

        counts = (
            persisted
            .agg(
                F.sum(F.when(F.col("error_reason").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("good_rows"),
                F.sum(F.when(F.col("error_reason").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("bad_rows"),
            )
            .collect()[0]
        )
        good_rows = int(counts["good_rows"] or 0)
        bad_rows = int(counts["bad_rows"] or 0)

        bad_batch = (
            persisted
            .filter(col("error_reason").isNotNull())
            .select(
                lit(STREAM_INSTANCE_ID).alias("stream_instance_id"),
                lit(int(batch_id)).alias("batch_id"),
                col("kafka_topic").alias("topic"),
                col("kafka_partition").cast("int").alias("kafka_partition"),
                col("kafka_offset").cast("bigint").alias("kafka_offset"),
                col("error_reason"),
                col("raw_payload"),
            )
        )

        good_batch = (
            persisted
            .filter(col("error_reason").isNull())
            .drop("error_reason", "raw_payload", "kafka_topic")
            .dropDuplicates(["event_id"])  # intra-batch dedup (cross-batch handled by ON CONFLICT)
        )

        # DLQ (best-effort)
        if bad_rows > 0:
            try:
                (
                    bad_batch.write
                    .mode("append")
                    .jdbc(url=PG_URL, table=DLQ_TABLE, properties=jdbc_props)
                )
            except Exception as e:
                print(f"[spark-stream] DLQ write failed batch_id={batch_id} err={e}", flush=True)

        # Staging table -> idempotent insert into target
        if good_rows > 0:
            staging_table = f"{STAGING_PREFIX}{STREAM_INSTANCE_ID}_{batch_id}"

            (
                good_batch.write
                .mode("overwrite")
                .jdbc(url=PG_URL, table=staging_table, properties=jdbc_props)
            )

            insert_sql = f"""
            INSERT INTO {TARGET_TABLE} (event_id, commodity, symbol, price, currency, event_ts, source, ingest_ts, kafka_partition, kafka_offset)
            SELECT
              event_id,
              commodity,
              symbol,
              price,
              currency,
              event_ts,
              source,
              ingest_ts,
              kafka_partition,
              kafka_offset
            FROM {staging_table}
            ON CONFLICT (event_id) DO NOTHING;
            """

            _exec_sql_via_jdbc(spark, insert_sql)

        ms = int((time.time() - t0) * 1000)
        print(
            f"[spark-stream] batch_id={batch_id} good_rows={good_rows} bad_rows={bad_rows} ms={ms}",
            flush=True,
        )

        persisted.unpersist()

    return foreach_batch


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("kafka_to_postgres_raw_prices")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    # Read Kafka stream (with backpressure limit)
    # IMPORTANT: do NOT set kafka.group.id; Spark uses checkpoint to track offsets safely.
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", FAIL_ON_DATA_LOSS)
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    df_base = df_kafka.select(
        expr("CAST(value AS STRING)").alias("raw_payload"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
    )

    df_parsed = df_base.withColumn("e", from_json(col("raw_payload"), event_schema))

    df_clean = (
        df_parsed
        .select(
            "raw_payload",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            col("e.*"),
        )
        .withColumn("event_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("ingest_ts", current_timestamp())
        .drop("timestamp")
    )

    df_with_reason = (
        df_clean
        .withColumn(
            "error_reason",
            when(
                col("event_id").isNull()
                & col("commodity").isNull()
                & col("symbol").isNull()
                & col("price").isNull(),
                lit("JSON_PARSE_ERROR_OR_EMPTY"),
            )
            .when(col("event_id").isNull(), lit("MISSING_FIELD:event_id"))
            .when(col("commodity").isNull(), lit("MISSING_FIELD:commodity"))
            .when(col("symbol").isNull(), lit("MISSING_FIELD:symbol"))
            .when(col("price").isNull(), lit("MISSING_FIELD:price"))
            .when(col("currency").isNull(), lit("MISSING_FIELD:currency"))
            .when(col("source").isNull(), lit("MISSING_FIELD:source"))
            .when(col("event_ts").isNull(), lit("INVALID_FIELD:event_ts"))
            .when(col("price") <= lit(0), lit("INVALID_FIELD:price<=0"))
            .otherwise(lit(None))
        )
    )

    foreach_fn = make_foreach_batch(spark)

    query = (
        df_with_reason.writeStream
        .foreachBatch(foreach_fn)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="60 seconds")
        .outputMode("update")
        .start()
    )

    def _graceful_stop(signum, frame):
        try:
            query.stop()
        finally:
            spark.stop()
            sys.exit(0)

    signal.signal(signal.SIGTERM, _graceful_stop)
    signal.signal(signal.SIGINT, _graceful_stop)

    query.awaitTermination()