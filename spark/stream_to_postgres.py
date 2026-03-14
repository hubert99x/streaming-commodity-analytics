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
import pyspark.sql.functions as F

from validation import PRICE_BOUNDS, SUPPORTED_SCHEMA_VERSION


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

STAGING_TABLE = os.getenv("PG_STAGING_TABLE", "ingest.raw_prices_staging")

DLQ_TABLE = os.getenv("PG_DLQ_TABLE", "monitoring.dead_letter_events")

DLQ_STAGING_TABLE = os.getenv("PG_DLQ_STAGING_TABLE", "ingest.dlq_staging")

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


def _get_jdbc_driver(spark: SparkSession):
    """Get a PostgreSQL JDBC driver instance via Spark's classloader."""
    jvm = spark._sc._jvm
    tcl = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    driver_class = jvm.java.lang.Class.forName("org.postgresql.Driver", True, tcl)
    return driver_class.newInstance()


def _get_jdbc_props(spark: SparkSession):
    """Build JDBC connection properties with connect/socket timeouts."""
    jvm = spark._sc._jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", POSTGRES_USER)
    props.setProperty("password", POSTGRES_PASSWORD)
    props.setProperty("connectTimeout", "10")   # 10 seconds to establish connection
    props.setProperty("socketTimeout", "30")     # 30 seconds for query execution
    return props


def _open_jdbc_conn(spark: SparkSession):
    """Open a raw JDBC connection via Spark's JVM driver."""
    driver = _get_jdbc_driver(spark)
    props = _get_jdbc_props(spark)
    return driver.connect(PG_URL, props)


def _exec_sql_via_jdbc(spark: SparkSession, sql_text: str) -> None:
    """
    Execute SQL on Postgres using JVM JDBC (no extra Python libs required).
    Uses the JVM driver already loaded by Spark, avoiding a psycopg2 dependency.
    """
    conn = None
    stmt = None
    try:
        conn = _open_jdbc_conn(spark)
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


def _exec_merge_returning_count(spark: SparkSession, sql_text: str) -> int:
    """Execute an INSERT ... ON CONFLICT and return the number of actually inserted rows."""
    conn = None
    stmt = None
    try:
        conn = _open_jdbc_conn(spark)
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        count = stmt.executeUpdate(sql_text)
        conn.commit()
        return count
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if stmt is not None:
            stmt.close()
        if conn is not None:
            conn.close()


def _ensure_staging_tables(spark: SparkSession):
    """Create persistent staging tables and DLQ unique constraint once (idempotent)."""
    _exec_sql_via_jdbc(spark, f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
        event_id        TEXT,
        commodity       TEXT,
        symbol          TEXT,
        price           DOUBLE PRECISION,
        currency        TEXT,
        event_ts        TIMESTAMP,
        source          TEXT,
        ingest_ts       TIMESTAMP,
        kafka_partition INTEGER,
        kafka_offset    BIGINT,
        schema_version  INTEGER,
        ingest_interval_sec INTEGER
    );
    """)
    _exec_sql_via_jdbc(spark, f"""
    CREATE TABLE IF NOT EXISTS {DLQ_STAGING_TABLE} (
        stream_instance_id TEXT,
        batch_id           INTEGER,
        topic              TEXT,
        kafka_partition    INTEGER,
        kafka_offset       BIGINT,
        error_reason       TEXT,
        raw_payload        TEXT
    );
    """)
    # DLQ unique constraint (uq_dlq_event) must be created by DB admin — see ops/sql/create_indexes.sql


def _staging_cycle(spark, jdbc_props, batch_df, *, staging_table, merge_sql, lock_key):
    """
    Atomic staging cycle: TRUNCATE → JDBC write → MERGE, guarded by a
    PostgreSQL session-level advisory lock to prevent races if two Spark
    instances overlap during container restarts.

    Returns the number of rows actually inserted by the merge (excludes ON CONFLICT skips).
    """
    lock_conn = _open_jdbc_conn(spark)
    lock_stmt = lock_conn.createStatement()
    inserted = 0
    try:
        lock_stmt.execute(f"SELECT pg_advisory_lock({lock_key})")

        _exec_sql_via_jdbc(spark, f"TRUNCATE {staging_table};")

        (
            batch_df.write
            .mode("append")
            .jdbc(url=PG_URL, table=staging_table, properties=jdbc_props)
        )

        inserted = _exec_merge_returning_count(spark, merge_sql)
    finally:
        unlock_ok = False
        for attempt in range(3):
            try:
                lock_stmt.execute(f"SELECT pg_advisory_unlock({lock_key})")
                unlock_ok = True
                break
            except Exception as e:
                if attempt == 2:
                    print(
                        f"[spark-stream] WARNING: pg_advisory_unlock({lock_key}) failed after 3 attempts: {e}",
                        flush=True,
                    )
        lock_stmt.close()
        lock_conn.close()
    return inserted


def make_foreach_batch(spark: SparkSession):
    """
    foreachBatch handler:
    - compute good/bad counts with a single agg
    - write bad rows to DLQ (best-effort)
    - TRUNCATE persistent staging table, write good rows, then INSERT ... ON CONFLICT
    """
    staging_ready = False

    def foreach_batch(batch_df, batch_id: int):
        nonlocal staging_ready
        if not staging_ready:
            _ensure_staging_tables(spark)
            staging_ready = True

        if batch_df.isEmpty():
            print(f"[spark-stream] batch_id={batch_id} rows=0 skip=true", flush=True)
            return

        t0 = time.time()

        jdbc_props = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": PG_DRIVER,
        }

        counts = (
            batch_df
            .agg(
                F.sum(F.when(F.col("error_reason").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("good_rows"),
                F.sum(F.when(F.col("error_reason").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("bad_rows"),
            )
            .collect()[0]
        )
        good_rows = int(counts["good_rows"] or 0)
        bad_rows = int(counts["bad_rows"] or 0)

        bad_batch = (
            batch_df
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
            batch_df
            .filter(col("error_reason").isNull())
            .drop("error_reason", "raw_payload", "kafka_topic")
        )

        # DLQ (best-effort, idempotent via staging + ON CONFLICT)
        # Advisory lock (key 2) prevents concurrent batches from colliding on DLQ staging
        dlq_write_failed = 0
        if bad_rows > 0:
            try:
                _staging_cycle(
                    spark, jdbc_props, bad_batch,
                    staging_table=DLQ_STAGING_TABLE,
                    merge_sql=f"""
                    INSERT INTO {DLQ_TABLE} (stream_instance_id, batch_id, topic, kafka_partition, kafka_offset, error_reason, raw_payload)
                    SELECT stream_instance_id, batch_id, topic, kafka_partition, kafka_offset, error_reason, raw_payload
                    FROM {DLQ_STAGING_TABLE}
                    ON CONFLICT (stream_instance_id, batch_id, kafka_partition, kafka_offset) DO NOTHING;
                    """,
                    lock_key=2,
                )
            except Exception as e:
                dlq_write_failed = bad_rows
                print(
                    f"[spark-stream] DLQ_WRITE_FAILURE batch_id={batch_id} lost_records={bad_rows} err={e}",
                    flush=True,
                )

        # Merge good rows into target via staging table
        # Advisory lock (key 1) prevents concurrent batches from colliding on staging
        inserted = 0
        conflict_skipped = 0
        if good_rows > 0:
            inserted = _staging_cycle(
                spark, jdbc_props, good_batch,
                staging_table=STAGING_TABLE,
                merge_sql=f"""
                INSERT INTO {TARGET_TABLE} (event_id, commodity, symbol, price, currency, event_ts, source, ingest_ts, kafka_partition, kafka_offset)
                SELECT event_id, commodity, symbol, price, currency, event_ts, source, ingest_ts, kafka_partition, kafka_offset
                FROM {STAGING_TABLE}
                ON CONFLICT (event_id) DO NOTHING;
                """,
                lock_key=1,
            )
            conflict_skipped = good_rows - inserted

        ms = int((time.time() - t0) * 1000)
        print(
            f"[spark-stream] batch_id={batch_id} good_rows={good_rows} inserted={inserted} "
            f"conflict_skipped={conflict_skipped} bad_rows={bad_rows} "
            f"dlq_write_failed={dlq_write_failed} ms={ms}",
            flush=True,
        )

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

    # Build validation chain — price bounds are driven by validation.PRICE_BOUNDS
    validation_chain = (
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
        .when(col("schema_version") != lit(SUPPORTED_SCHEMA_VERSION), lit("UNSUPPORTED_SCHEMA_VERSION"))
    )

    # Per-commodity sanity bounds from shared PRICE_BOUNDS (single source of truth)
    for symbol, (lo, hi) in PRICE_BOUNDS.items():
        validation_chain = validation_chain.when(
            (col("symbol") == symbol)
            & ((col("price") < lit(lo)) | (col("price") > lit(hi))),
            lit("INVALID_FIELD:price_out_of_range"),
        )

    validation_chain = validation_chain.otherwise(lit(None))

    df_with_reason = df_clean.withColumn("error_reason", validation_chain)

    foreach_fn = make_foreach_batch(spark)

    query = (
        df_with_reason.writeStream
        .foreachBatch(foreach_fn)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="300 seconds")
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