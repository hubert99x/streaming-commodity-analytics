import os
import time
import psycopg2
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


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def write_lag(total_lag, max_lag):

    conn = get_connection()

    with conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO monitoring.kafka_lag
                (group_id, topic, ts_utc, total_lag, max_partition_lag)
                VALUES (%s,%s,now(),%s,%s)
                """,
                (CONSUMER_GROUP, KAFKA_TOPIC, total_lag, max_lag)
            )

    conn.close()


def get_lag():

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})

    cluster_md = admin.list_topics(timeout=10)

    partitions = cluster_md.topics[KAFKA_TOPIC].partitions

    total_lag = 0
    max_lag = 0

    for p in partitions:

        lag = 0

        total_lag += lag

        if lag > max_lag:
            max_lag = lag

    return total_lag, max_lag


def main():

    print(f"[kafka-lag] bootstrap={KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC} group={CONSUMER_GROUP}")

    while True:

        try:

            total_lag, max_lag = get_lag()

            write_lag(total_lag, max_lag)

            print(f"[kafka-lag] total_lag={total_lag} max_partition_lag={max_lag}")

        except Exception as e:

            print(f"[kafka-lag] error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()