"""
Kafka consumer lag monitor.

Polls broker watermark offsets and compares them against the last
processed offset in public.raw_prices (since Spark uses checkpoint-based
offsets, not consumer group commits). Logs lag to monitoring.kafka_lag.
"""

import contextlib
import os
import signal
import time

import psycopg2
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "commodity_prices")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "spark_stream_raw_prices")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

POLL_INTERVAL = int(os.getenv("KAFKA_LAG_POLL_SEC", "60"))

_running = True

# Kafka clients are built once and reused across polls. Rebuilding them every
# minute meant a fresh connection and a metadata fetch on every measurement.
_admin = None
_consumer = None


def get_connection():
    """Open a new Postgres connection."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def write_lag(total_lag, max_lag):
    """Insert a lag measurement into monitoring.kafka_lag."""
    # closing() around the connection: `with conn` only ends the transaction,
    # so a failing query would otherwise leave the socket open on every poll.
    with contextlib.closing(get_connection()) as conn, conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO monitoring.kafka_lag
            (group_id, topic, ts_utc, total_lag, max_partition_lag)
            VALUES (%s,%s,now(),%s,%s)
            """,
            (CONSUMER_GROUP, KAFKA_TOPIC, total_lag, max_lag)
        )


def get_processed_offsets():
    """Return {partition_id: max_kafka_offset} from raw_prices.

    Queries the target table instead of Kafka consumer groups because
    Spark uses checkpoint-based offsets, not consumer group commits.
    """
    with contextlib.closing(get_connection()) as conn, conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT kafka_partition, MAX(kafka_offset)
            FROM public.raw_prices
            GROUP BY kafka_partition
            """
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def partition_lag(high: int, low: int, last_processed) -> int:
    """
    Number of messages still waiting on one partition.

    `last_processed` is the highest offset already stored in raw_prices, or None
    when the table holds no rows for this partition. In that case the count
    starts at the log start offset rather than at zero: after the 90-day
    retention removes the last row of a quiet partition, counting from zero
    would report the entire partition as unprocessed and trip the critical lag
    alert, even though Kafka has nothing left to deliver.
    """
    next_expected = low if last_processed is None else max(last_processed + 1, low)
    return max(0, high - next_expected)


def get_clients():
    """Return the shared (admin, consumer) pair, creating it on first use."""
    global _admin, _consumer
    if _admin is None:
        _admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    if _consumer is None:
        # Never subscribes and never commits: it exists only to read broker
        # watermark offsets, so it stays outside the consumer group protocol.
        _consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "__kafka_lag_inspector__"})
    return _admin, _consumer


def close_clients():
    """Drop the shared clients so the next poll rebuilds them."""
    global _admin, _consumer
    if _consumer is not None:
        # Already tearing the client down, a close failure changes nothing.
        with contextlib.suppress(Exception):
            _consumer.close()
    _admin = None
    _consumer = None


def get_lag():
    """Calculate total and max-partition lag by comparing broker watermarks to processed offsets."""
    admin, consumer = get_clients()
    cluster_md = admin.list_topics(timeout=10)
    partition_ids = list(cluster_md.topics[KAFKA_TOPIC].partitions.keys())

    processed = get_processed_offsets()

    total_lag = 0
    max_lag = 0

    for p in partition_ids:
        # low = oldest offset still retained, high = next offset to be assigned
        low, high = consumer.get_watermark_offsets(TopicPartition(KAFKA_TOPIC, p), timeout=5)
        lag = partition_lag(high, low, processed.get(p))
        total_lag += lag
        max_lag = max(max_lag, lag)

    return total_lag, max_lag


def _handle_stop(signum, _frame):
    global _running
    _running = False
    print(f"[kafka-lag] received signal {signum}, stopping...")


def main():
    """Poll Kafka lag at regular intervals and log to Postgres."""
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    print(f"[kafka-lag] bootstrap={KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC} group={CONSUMER_GROUP}")

    while _running:
        try:
            total_lag, max_lag = get_lag()
            write_lag(total_lag, max_lag)
            print(f"[kafka-lag] total_lag={total_lag} max_partition_lag={max_lag}")
        # Broad on purpose: a broker hiccup or a failed insert must not kill the
        # monitor, otherwise lag stops being recorded exactly when it matters.
        except Exception as e:  # noqa: BLE001
            print(f"[kafka-lag] error: {e}")
            # The shared clients may be the broken part, so rebuild them next poll.
            close_clients()

        # Sleep in 1-second ticks so SIGTERM does not wait out the whole interval
        slept = 0
        while _running and slept < POLL_INTERVAL:
            time.sleep(1)
            slept += 1

    close_clients()
    print("[kafka-lag] stopped.")


if __name__ == "__main__":
    main()
