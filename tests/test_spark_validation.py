"""Tests for Spark validation rules (pure Python, no SparkSession needed)."""

import pytest

from spark.validation import validate_event


def _make_event(**overrides):
    """Build a valid event dict, overriding specific fields."""
    base = {
        "schema_version": 1,
        "event_id": "abc-123",
        "commodity": "gold",
        "symbol": "XAU/USD",
        "price": 2150.0,
        "currency": "USD",
        "timestamp": "2026-03-13T10:00:00Z",
        "source": "twelvedata_rest",
        "ingest_interval_sec": 360,
    }
    base.update(overrides)
    return base


# ---- Valid events ----

class TestValidEvents:
    def test_valid_gold(self):
        assert validate_event(_make_event()) is None

    def test_valid_btc(self):
        e = _make_event(commodity="bitcoin", symbol="BTC/USD", price=65000.0)
        assert validate_event(e) is None

    def test_valid_eurusd(self):
        e = _make_event(commodity="eurusd", symbol="EUR/USD", price=1.085)
        assert validate_event(e) is None


# ---- Missing fields ----

class TestMissingFields:
    def test_all_core_fields_missing_is_parse_error(self):
        e = _make_event(event_id=None, commodity=None, symbol=None, price=None)
        assert validate_event(e) == "JSON_PARSE_ERROR_OR_EMPTY"

    def test_missing_event_id(self):
        assert validate_event(_make_event(event_id=None)) == "MISSING_FIELD:event_id"

    def test_missing_commodity(self):
        assert validate_event(_make_event(commodity=None)) == "MISSING_FIELD:commodity"

    def test_missing_symbol(self):
        assert validate_event(_make_event(symbol=None)) == "MISSING_FIELD:symbol"

    def test_missing_price(self):
        assert validate_event(_make_event(price=None)) == "MISSING_FIELD:price"

    def test_missing_currency(self):
        assert validate_event(_make_event(currency=None)) == "MISSING_FIELD:currency"

    def test_missing_source(self):
        assert validate_event(_make_event(source=None)) == "MISSING_FIELD:source"

    def test_missing_timestamp(self):
        assert validate_event(_make_event(timestamp=None)) == "INVALID_FIELD:event_ts"


# ---- Invalid values ----

class TestInvalidValues:
    def test_zero_price(self):
        assert validate_event(_make_event(price=0.0)) == "INVALID_FIELD:price<=0"

    def test_negative_price(self):
        assert validate_event(_make_event(price=-10.0)) == "INVALID_FIELD:price<=0"

    def test_unsupported_schema_version(self):
        assert validate_event(_make_event(schema_version=2)) == "UNSUPPORTED_SCHEMA_VERSION"

    def test_schema_version_none(self):
        assert validate_event(_make_event(schema_version=None)) == "UNSUPPORTED_SCHEMA_VERSION"


# ---- Per-commodity price bounds ----

class TestPriceBounds:
    @pytest.mark.parametrize("price", [499.99, 15000.01])
    def test_gold_out_of_range(self, price):
        e = _make_event(symbol="XAU/USD", price=price)
        assert validate_event(e) == "INVALID_FIELD:price_out_of_range"

    def test_gold_at_lower_bound(self):
        assert validate_event(_make_event(symbol="XAU/USD", price=500.0)) is None

    def test_gold_at_upper_bound(self):
        assert validate_event(_make_event(symbol="XAU/USD", price=15000.0)) is None

    @pytest.mark.parametrize("price", [99.99, 1000000.01])
    def test_btc_out_of_range(self, price):
        e = _make_event(commodity="bitcoin", symbol="BTC/USD", price=price)
        assert validate_event(e) == "INVALID_FIELD:price_out_of_range"

    @pytest.mark.parametrize("price", [0.49, 2.01])
    def test_eurusd_out_of_range(self, price):
        e = _make_event(commodity="eurusd", symbol="EUR/USD", price=price)
        assert validate_event(e) == "INVALID_FIELD:price_out_of_range"

    def test_unknown_symbol_no_bounds_check(self):
        """Unknown symbols pass (no bounds defined — validated elsewhere)."""
        e = _make_event(symbol="UNKNOWN/X", price=999999.0)
        assert validate_event(e) is None


# ---- Priority ordering (first match wins) ----

class TestValidationPriority:
    def test_missing_field_before_bad_price(self):
        """event_id=None should be caught before price<=0."""
        e = _make_event(event_id=None, price=-5.0)
        assert validate_event(e) == "MISSING_FIELD:event_id"

    def test_bad_price_before_schema_version(self):
        """price<=0 should be caught before unsupported schema version."""
        e = _make_event(price=-1.0, schema_version=2)
        assert validate_event(e) == "INVALID_FIELD:price<=0"

    def test_schema_version_before_bounds(self):
        """Schema version check comes before price range bounds."""
        e = _make_event(schema_version=99, price=0.01, symbol="EUR/USD")
        assert validate_event(e) == "UNSUPPORTED_SCHEMA_VERSION"
