import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone, time as dtime
from typing import Dict, List

import requests
import psycopg2
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


# ==========================================================
# Config (env-first, safe defaults)
# ==========================================================
KAFKA_BOOTSTRAP = (
    os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    or os.getenv("KAFKA_BOOTSTRAP")
    or "kafka:29092"
)

TOPIC = os.getenv("KAFKA_TOPIC") or os.getenv("TOPIC") or "commodity_prices"

TD_API_KEY = os.getenv("TD_API_KEY", "")
SOURCE = os.getenv("SOURCE", "twelvedata_rest")

INTERVAL_SEC = int(os.getenv("INTERVAL_SEC", "360"))
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "10"))
BACKOFF_MIN_SEC = int(os.getenv("BACKOFF_MIN_SEC", "15"))
BACKOFF_MAX_SEC = int(os.getenv("BACKOFF_MAX_SEC", str(max(60, INTERVAL_SEC * 10))))

TD_BASE = os.getenv("TD_BASE", "https://api.twelvedata.com")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("PRODUCER_DB_USER")
POSTGRES_PASSWORD = os.getenv("PRODUCER_DB_PASSWORD")

SYMBOLS = [
    {"commodity": "gold", "symbol": "XAU/USD"},
    {"commodity": "bitcoin", "symbol": "BTC/USD"},
    {"commodity": "eurusd", "symbol": "EUR/USD"},
]

# FX weekend gating: skip publish (and skip fetching, in this implementation) from Fri 22:00 UTC to Sun 21:59:59 UTC
FX_WEEKEND_GATED_SYMBOLS = {"EUR/USD", "XAU/USD"}

# Per-symbol sanity bounds — reject absurd API values before they reach Kafka.
# Same thresholds as Spark validation (spark/validation.py) for defense-in-depth.
PRICE_BOUNDS: Dict[str, tuple] = {
    "XAU/USD": (500.0, 15_000.0),
    "BTC/USD": (100.0, 1_000_000.0),
    "EUR/USD": (0.5, 2.0),
}


# ==========================================================
# API Metrics Logging (Postgres) — lazy singleton connection
# ==========================================================
_pg_conn = None


def _get_pg_conn():
    """Return a reusable Postgres connection, reconnecting if stale."""
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        _pg_conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        _pg_conn.autocommit = True
    return _pg_conn


def log_api_call(symbols: str, http_status, latency_ms: int, ok: bool, error_type=None, error_msg=None):
    """
    Insert one API call metric row into monitoring.api_calls.
    This must never break the producer loop.
    """
    try:
        conn = _get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO monitoring.api_calls
                (ts_utc, symbols, http_status, latency_ms, ok, error_type, error_msg)
                VALUES (now(), %s, %s, %s, %s, %s, %s)
                """,
                (symbols, http_status, latency_ms, ok, error_type, error_msg),
            )
    except Exception as e:
        global _pg_conn
        _pg_conn = None
        print(f"ERROR logging API metrics: {e}", flush=True)


# ==========================================================
# Helpers
# ==========================================================
def utc_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


def delivery_report(err, msg):
    if err is not None:
        print(f"DELIVERY ERROR: {err}", flush=True)


def is_fx_weekend_closed(now_utc: datetime) -> bool:
    """
    Returns True if the FX market is considered closed under the rule:
    closed from Friday 22:00:00 UTC (inclusive) until Sunday 21:59:59 UTC (inclusive).
    Re-opens Sunday 22:00:00 UTC.
    """
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)

    dow = now_utc.weekday()  # Monday=0 ... Sunday=6
    t = now_utc.time()

    # Friday >= 22:00:00 -> closed
    if dow == 4 and t >= dtime(22, 0, 0):
        return True

    # Saturday any time -> closed
    if dow == 5:
        return True

    # Sunday < 22:00:00 -> closed, open from 22:00:00
    if dow == 6 and t < dtime(22, 0, 0):
        return True

    return False


def should_publish(symbol: str, now_utc: datetime) -> bool:
    """
    Decide whether a message should be published at current time.
    - BTC/crypto: always publish (24/7) -> your BTC is "BTC/USD", so it is not gated
    - EUR/USD and XAU/USD: publish only if not in FX weekend closed window
    """
    if symbol in FX_WEEKEND_GATED_SYMBOLS:
        return not is_fx_weekend_closed(now_utc)
    return True


def active_symbols_for_fetch(now_utc: datetime) -> List[str]:
    """
    Build the list of symbols to fetch from the API for the current cycle.
    If FX weekend gate is active, skip fetching gated FX symbols to save API calls.
    """
    out: List[str] = []
    for m in SYMBOLS:
        sym = m["symbol"]
        if should_publish(sym, now_utc):
            out.append(sym)
    return out


def td_prices(symbols: List[str]) -> Dict[str, float]:
    if not symbols:
        return {}

    t0 = time.perf_counter()

    try:
        r = requests.get(
            f"{TD_BASE}/price",
            params={"symbol": ",".join(symbols), "apikey": TD_API_KEY},
            timeout=HTTP_TIMEOUT_SEC,
        )

        latency_ms = int((time.perf_counter() - t0) * 1000)

        if r.status_code == 429:
            log_api_call(",".join(symbols), 429, latency_ms, False, "RATE_LIMIT_429", None)
            raise RuntimeError("RATE_LIMIT_429")

        if 500 <= r.status_code <= 599:
            log_api_call(",".join(symbols), r.status_code, latency_ms, False, f"SERVER_{r.status_code}", None)
            raise RuntimeError(f"SERVER_{r.status_code}")

        r.raise_for_status()

        data = r.json()

        if isinstance(data, dict) and data.get("status") == "error":
            log_api_call(",".join(symbols), r.status_code, latency_ms, False, "TD_ERROR", data.get("message"))
            raise RuntimeError(f"TD_ERROR: {data.get('message')}")

        log_api_call(",".join(symbols), r.status_code, latency_ms, True, None, None)

    except Exception as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        log_api_call(",".join(symbols), None, latency_ms, False, "EXCEPTION", str(e))
        raise

    out: Dict[str, float] = {}

    # Twelve Data returns different JSON shapes depending on the number of symbols:
    # Case A: single symbol response: {"symbol":"BTC/USD","price":"..."}
    if isinstance(data, dict) and "symbol" in data and "price" in data:
        out[data["symbol"]] = float(data["price"])
        return out

    # Case B: single symbol response without symbol: {"price":"..."}
    if isinstance(data, dict) and "price" in data and len(symbols) == 1:
        out[symbols[0]] = float(data["price"])
        return out

    # Case C: multi symbol response: {"BTC/USD":{"price":"..."}, ...}
    if isinstance(data, dict):
        for s in symbols:
            v = data.get(s)
            if isinstance(v, dict) and v.get("price") is not None:
                out[s] = float(v["price"])

    if not out:
        print(f"WARNING: No prices parsed from response: {data}", flush=True)

    return out


# ==========================================================
# Main loop with graceful shutdown
# ==========================================================
_running = True


def _handle_stop(signum, frame):
    global _running
    _running = False
    print(
        f"STOP signal received ({signum}). Flushing Kafka producer and exiting...",
        flush=True,
    )


def _ensure_topic(topic: str, num_partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        fs = admin.create_topics([NewTopic(topic, num_partitions=num_partitions, replication_factor=1)])
        fs[topic].result()
        print(f"[producer] created topic '{topic}' with {num_partitions} partitions", flush=True)
    else:
        actual = len(existing[topic].partitions)
        if actual != num_partitions:
            print(f"[producer] WARNING: topic '{topic}' has {actual} partitions (expected {num_partitions}). Recreate manually.", flush=True)


def main():
    global _running

    if not TD_API_KEY:
        raise SystemExit("Missing TD_API_KEY in environment (.env)")

    # Graceful shutdown hooks
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    print(
        f"BOOTSTRAP={KAFKA_BOOTSTRAP} TOPIC={TOPIC} INTERVAL_SEC={INTERVAL_SEC} "
        f"SOURCE={SOURCE} SYMBOLS={[m['symbol'] for m in SYMBOLS]} HTTP_TIMEOUT_SEC={HTTP_TIMEOUT_SEC} "
        f"FX_WEEKEND_GATED_SYMBOLS={sorted(FX_WEEKEND_GATED_SYMBOLS)}",
        flush=True,
    )

    # Ensure topic exists with correct partition count
    _ensure_topic(TOPIC, num_partitions=3)

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "linger.ms": 0,
            "queue.buffering.max.ms": 1000,
        }
    )

    backoff_sec = 0
    backoff_multiplier = 1

    while _running:
        if backoff_sec > 0:
            print(f"BACKOFF {backoff_sec}s", flush=True)
            slept = 0
            while _running and slept < backoff_sec:
                time.sleep(1)
                slept += 1
            backoff_sec = 0
            if not _running:
                break

        now_utc = datetime.now(timezone.utc)
        ts = (
            now_utc.replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )

        symbols_list = active_symbols_for_fetch(now_utc)

        if not symbols_list:
            print(
                f"SKIP CYCLE - no active symbols to fetch (now_utc={now_utc.isoformat()})",
                flush=True,
            )
            slept = 0
            while _running and slept < INTERVAL_SEC:
                time.sleep(1)
                slept += 1
            continue

        try:
            prices = td_prices(symbols_list)
            backoff_multiplier = 1

        except RuntimeError as e:
            s = str(e)
            if s == "RATE_LIMIT_429":
                backoff_sec = clamp(
                    max(BACKOFF_MIN_SEC, INTERVAL_SEC * 3),
                    BACKOFF_MIN_SEC,
                    BACKOFF_MAX_SEC,
                )
                print(f"RATE LIMIT (429). Backing off for {backoff_sec}s.", flush=True)
                continue

            if s.startswith("SERVER_"):
                backoff_multiplier = clamp(backoff_multiplier * 2, 1, 32)
                backoff_sec = clamp(
                    INTERVAL_SEC * backoff_multiplier,
                    BACKOFF_MIN_SEC,
                    BACKOFF_MAX_SEC,
                )
                print(f"API {s}. Backing off for {backoff_sec}s.", flush=True)
                continue

            backoff_sec = clamp(INTERVAL_SEC, BACKOFF_MIN_SEC, BACKOFF_MAX_SEC)
            print(f"ERROR batch request: {e}. Backing off for {backoff_sec}s.", flush=True)
            continue

        except Exception as e:
            backoff_multiplier = clamp(backoff_multiplier * 2, 1, 32)
            backoff_sec = clamp(
                INTERVAL_SEC * backoff_multiplier,
                BACKOFF_MIN_SEC,
                BACKOFF_MAX_SEC,
            )
            print(f"ERROR batch request: {e}. Backing off for {backoff_sec}s.", flush=True)
            continue

        sent = 0
        for meta in SYMBOLS:
            commodity = meta["commodity"]
            symbol = meta["symbol"]

            if not should_publish(symbol, now_utc):
                print(
                    f"SKIP {commodity} ({symbol}) - FX weekend gate active (now_utc={now_utc.isoformat()})",
                    flush=True,
                )
                continue

            price = prices.get(symbol)
            if price is None:
                print(f"SKIP {commodity} ({symbol}) - no price in batch response", flush=True)
                continue

            # Pre-publish sanity check: reject absurd API values before Kafka
            bounds = PRICE_BOUNDS.get(symbol)
            if bounds is not None:
                lo, hi = bounds
                if price < lo or price > hi:
                    print(
                        f"REJECT {commodity} ({symbol}) - price {price} out of range [{lo}, {hi}]",
                        flush=True,
                    )
                    continue

            # Deterministic event_id: same (commodity, timestamp) always produces the same ID,
            # preventing semantic duplicates if the application retries a publish attempt.
            event_id_namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # standard DNS namespace
            event_id = str(uuid.uuid5(event_id_namespace, f"{commodity}:{ts}"))

            event = {
                "schema_version": 1,
                "event_id": event_id,
                "commodity": commodity,
                "symbol": symbol,
                "price": float(price),
                "currency": "USD",
                "timestamp": ts,
                "source": SOURCE,
                "ingest_interval_sec": INTERVAL_SEC,
            }

            try:
                producer.produce(
                    topic=TOPIC,
                    key=commodity.encode("utf-8"),
                    value=json.dumps(event, separators=(",", ":")).encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)  # serve delivery callbacks
                sent += 1
                print(f"SENT {TOPIC}: {event}", flush=True)

            except BufferError:
                producer.poll(1)
                try:
                    producer.produce(
                        topic=TOPIC,
                        key=commodity.encode("utf-8"),
                        value=json.dumps(event, separators=(",", ":")).encode("utf-8"),
                        callback=delivery_report,
                    )
                    producer.poll(0)
                    sent += 1
                    print(f"SENT {TOPIC}: {event}", flush=True)
                except Exception as e:
                    print(f"ERROR produce retry for {commodity} ({symbol}): {e}", flush=True)

            except Exception as e:
                print(f"ERROR produce for {commodity} ({symbol}): {e}", flush=True)

        try:
            producer.flush(10)
        except Exception as e:
            print(f"ERROR flush: {e}", flush=True)

        print(f"CYCLE DONE: sent={sent} fetched={len(symbols_list)} now_utc={now_utc.isoformat()}", flush=True)

        slept = 0
        while _running and slept < INTERVAL_SEC:
            time.sleep(1)
            slept += 1

    try:
        producer.flush(10)
    except Exception:
        pass

    print("Producer stopped.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr, flush=True)
        raise