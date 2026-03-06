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

# Polling interval between API pulls (seconds)
INTERVAL_SEC = int(os.getenv("INTERVAL_SEC", "360"))

# HTTP config
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "10"))
# Backoff caps
BACKOFF_MIN_SEC = int(os.getenv("BACKOFF_MIN_SEC", "15"))
BACKOFF_MAX_SEC = int(os.getenv("BACKOFF_MAX_SEC", str(max(60, INTERVAL_SEC * 10))))

TD_BASE = os.getenv("TD_BASE", "https://api.twelvedata.com")

# Postgres config (for API metrics logging)
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


# ==========================================================
# API Metrics Logging (Postgres)
# ==========================================================
def log_api_call(symbols: str, http_status, latency_ms: int, ok: bool, error_type=None, error_msg=None):
    """
    Insert one API call metric row into monitoring.api_calls.
    This must never break the producer loop.
    """
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO monitoring.api_calls
                    (ts_utc, symbols, http_status, latency_ms, ok, error_type, error_msg)
                    VALUES (now(), %s, %s, %s, %s, %s, %s)
                    """,
                    (symbols, http_status, latency_ms, ok, error_type, error_msg),
                )
        conn.close()
    except Exception as e:
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
        # Keep it short and explicit; msg may be large
        print(f"DELIVERY ERROR: {err}", flush=True)


def is_fx_weekend_closed(now_utc: datetime) -> bool:
    """
    Returns True if the FX market is considered closed under the rule:
    closed from Friday 22:00:00 UTC (inclusive) until Sunday 21:59:59 UTC (inclusive).
    Re-opens Sunday 22:00:00 UTC.
    """
    if now_utc.tzinfo is None:
        # Always operate in UTC; enforce timezone-aware datetime
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

        # SUCCESS
        log_api_call(",".join(symbols), r.status_code, latency_ms, True, None, None)

    except Exception as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        log_api_call(",".join(symbols), None, latency_ms, False, "EXCEPTION", str(e))
        raise

    out: Dict[str, float] = {}

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

    # If parsing succeeded but no prices extracted, log full payload for debugging
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

    # Production-safer Kafka producer settings
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            # light batching (helps throughput, still low latency)
            "linger.ms": 50,
            # avoid infinite blocking on full queue
            "queue.buffering.max.ms": 1000,
        }
    )

    # Backoff state
    backoff_sec = 0
    backoff_multiplier = 1  # grows on repeated errors, resets on success

    while _running:
        # Apply any pending backoff
        if backoff_sec > 0:
            print(f"BACKOFF {backoff_sec}s", flush=True)
            slept = 0
            # sleep in 1s chunks so SIGTERM stops quickly
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

        # If FX gate is active, you may end up fetching only BTC/USD
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
            # Success -> reset backoff growth
            backoff_multiplier = 1

        except RuntimeError as e:
            s = str(e)
            if s == "RATE_LIMIT_429":
                # Stronger backoff on rate limit
                backoff_sec = clamp(
                    max(BACKOFF_MIN_SEC, INTERVAL_SEC * 3),
                    BACKOFF_MIN_SEC,
                    BACKOFF_MAX_SEC,
                )
                print(f"RATE LIMIT (429). Backing off for {backoff_sec}s.", flush=True)
                continue

            if s.startswith("SERVER_"):
                # Exponential backoff for server errors
                backoff_multiplier = clamp(backoff_multiplier * 2, 1, 32)
                backoff_sec = clamp(
                    INTERVAL_SEC * backoff_multiplier,
                    BACKOFF_MIN_SEC,
                    BACKOFF_MAX_SEC,
                )
                print(f"API {s}. Backing off for {backoff_sec}s.", flush=True)
                continue

            # Unknown runtime error
            backoff_sec = clamp(INTERVAL_SEC, BACKOFF_MIN_SEC, BACKOFF_MAX_SEC)
            print(f"ERROR batch request: {e}. Backing off for {backoff_sec}s.", flush=True)
            continue

        except Exception as e:
            # Network timeouts, JSON parse, etc.
            backoff_multiplier = clamp(backoff_multiplier * 2, 1, 32)
            backoff_sec = clamp(
                INTERVAL_SEC * backoff_multiplier,
                BACKOFF_MIN_SEC,
                BACKOFF_MAX_SEC,
            )
            print(f"ERROR batch request: {e}. Backing off for {backoff_sec}s.", flush=True)
            continue

        # Produce one event per commodity (publish only for currently allowed symbols)
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

            event = {
                "schema_version": 1,
                "event_id": str(uuid.uuid4()),
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
                # Local queue full: poll/flush a bit and retry once
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

        # Flush to ensure messages are delivered before sleeping.
        try:
            producer.flush(10)
        except Exception as e:
            print(f"ERROR flush: {e}", flush=True)

        print(f"CYCLE DONE: sent={sent} fetched={len(symbols_list)} now_utc={now_utc.isoformat()}", flush=True)

        # Sleep until next cycle (interruptible)
        slept = 0
        while _running and slept < INTERVAL_SEC:
            time.sleep(1)
            slept += 1

    # Final flush on shutdown
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