"""
Pure-Python validation rules for commodity price events.

These mirror the PySpark column-expression validation in stream_to_postgres.py
but can be tested without a SparkSession. Both modules share PRICE_BOUNDS as
the single source of truth for per-commodity price sanity limits.

The same bounds are also used by producer.py for pre-publish validation
(defense-in-depth: reject bad values before they enter Kafka).
"""

from typing import Dict, Optional, Tuple

# Per-commodity price bounds: (min_inclusive, max_inclusive)
# These values are intentionally wide to reject only clearly erroneous prices
# (e.g. API glitches returning 0 or astronomically high values).
PRICE_BOUNDS: Dict[str, Tuple[float, float]] = {
    "XAU/USD": (500.0, 15_000.0),      # Gold: ~$2000/oz typical (2024-2026)
    "BTC/USD": (100.0, 1_000_000.0),    # Bitcoin: ~$70k typical (2024-2026)
    "EUR/USD": (0.5, 2.0),              # EUR/USD: ~1.08 typical (2024-2026)
}

# Only schema_version=1 is accepted; future versions would need migration logic
SUPPORTED_SCHEMA_VERSION = 1


def validate_event(event: dict) -> Optional[str]:
    """
    Validate a parsed price event dict.

    Returns an error reason string if the event is invalid, or None if valid.
    Checks are ordered to match the PySpark validation chain exactly.
    """
    event_id = event.get("event_id")
    commodity = event.get("commodity")
    symbol = event.get("symbol")
    price = event.get("price")
    currency = event.get("currency")
    source = event.get("source")
    timestamp = event.get("timestamp")
    schema_version = event.get("schema_version")

    # All core fields missing → JSON was unparseable or empty string
    if event_id is None and commodity is None and symbol is None and price is None:
        return "JSON_PARSE_ERROR_OR_EMPTY"

    if event_id is None:
        return "MISSING_FIELD:event_id"
    if commodity is None:
        return "MISSING_FIELD:commodity"
    if symbol is None:
        return "MISSING_FIELD:symbol"
    if price is None:
        return "MISSING_FIELD:price"
    if currency is None:
        return "MISSING_FIELD:currency"
    if source is None:
        return "MISSING_FIELD:source"
    if timestamp is None:
        return "INVALID_FIELD:event_ts"

    if price <= 0:
        return "INVALID_FIELD:price<=0"

    if schema_version != SUPPORTED_SCHEMA_VERSION:
        return "UNSUPPORTED_SCHEMA_VERSION"

    bounds = PRICE_BOUNDS.get(symbol)
    if bounds is not None:
        lo, hi = bounds
        if price < lo or price > hi:
            return "INVALID_FIELD:price_out_of_range"

    return None
