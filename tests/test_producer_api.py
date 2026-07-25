"""
Tests for the producer's Twelve Data API layer: response parsing, error
classification, API-call accounting and the exponential backoff state machine.
"""

from unittest.mock import MagicMock, patch

import pytest

from producer import producer
from producer.producer import next_backoff, td_prices
from spark.validation import PRICE_BOUNDS as SPARK_PRICE_BOUNDS


def _response(status=200, body=None):
    """Build a fake requests.Response with the given status and JSON body."""
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = body if body is not None else {}
    if status >= 400:
        resp.raise_for_status.side_effect = RuntimeError(f"HTTP {status}")
    else:
        resp.raise_for_status.return_value = None
    return resp


def _call(symbols, status=200, body=None, get_side_effect=None):
    """Run td_prices against a mocked API. Returns (prices, logged_calls)."""
    logged = []
    get = get_side_effect or (lambda *a, **k: _response(status, body))
    with patch.object(producer, "log_api_call", lambda *a: logged.append(a)), \
         patch.object(producer.requests, "get", side_effect=get):
        try:
            prices = td_prices(symbols)
        except Exception as exc:
            prices = exc
    return prices, logged


# ---- Response shapes (Twelve Data varies by symbol count) ----

def test_parses_response_with_symbol_and_price():
    prices, _ = _call(["BTC/USD"], body={"symbol": "BTC/USD", "price": "65000.5"})
    assert prices == {"BTC/USD": 65000.5}


def test_parses_response_with_price_only_for_single_symbol():
    prices, _ = _call(["EUR/USD"], body={"price": "1.085"})
    assert prices == {"EUR/USD": 1.085}


def test_parses_grouped_multi_symbol_response():
    body = {"BTC/USD": {"price": "65000"}, "XAU/USD": {"price": "4500"}}
    prices, _ = _call(["BTC/USD", "XAU/USD"], body=body)
    assert prices == {"BTC/USD": 65000.0, "XAU/USD": 4500.0}


def test_keeps_symbols_present_in_a_partial_response():
    body = {"BTC/USD": {"price": "65000"}, "XAU/USD": {}}
    prices, _ = _call(["BTC/USD", "XAU/USD"], body=body)
    assert prices == {"BTC/USD": 65000.0}


def test_skips_symbol_with_unparseable_price():
    prices, _ = _call(["BTC/USD"], body={"symbol": "BTC/USD", "price": "n/a"})
    assert prices == {}


def test_returns_empty_dict_without_calling_api_for_no_symbols():
    prices, logged = _call([])
    assert prices == {}
    assert logged == []


# ---- Error classification ----

def test_rate_limit_raises_rate_limit_error():
    prices, _ = _call(["BTC/USD"], status=429)
    assert isinstance(prices, RuntimeError)
    assert str(prices) == "RATE_LIMIT_429"


def test_server_error_raises_server_error():
    prices, _ = _call(["BTC/USD"], status=503)
    assert isinstance(prices, RuntimeError)
    assert str(prices) == "SERVER_503"


def test_payload_level_error_raises_td_error():
    prices, _ = _call(["BTC/USD"], body={"status": "error", "message": "bad key"})
    assert isinstance(prices, RuntimeError)
    assert str(prices).startswith("TD_ERROR")


# ---- One monitoring.api_calls row per request ----

@pytest.mark.parametrize(
    "kwargs, expected_error_type",
    [
        ({"status": 429}, "RATE_LIMIT_429"),
        ({"status": 503}, "SERVER_503"),
        ({"body": {"status": "error", "message": "bad key"}}, "TD_ERROR"),
        ({"status": 401}, "EXCEPTION"),
        ({"body": {"symbol": "BTC/USD", "price": "65000"}}, None),
    ],
)
def test_logs_exactly_one_api_call_per_request(kwargs, expected_error_type):
    _, logged = _call(["BTC/USD"], **kwargs)
    assert len(logged) == 1
    assert logged[0][4] == expected_error_type


def test_logs_network_failure_once():
    def boom(*_args, **_kwargs):
        raise OSError("connection timed out")

    prices, logged = _call(["BTC/USD"], get_side_effect=boom)
    assert isinstance(prices, OSError)
    assert len(logged) == 1
    assert logged[0][4] == "EXCEPTION"


def test_marks_response_without_usable_price_as_failed():
    """A 200 that yields no price must not count as a healthy call."""
    _, logged = _call(["BTC/USD"], body={})
    assert len(logged) == 1
    ok, error_type = logged[0][3], logged[0][4]
    assert ok is False
    assert error_type == "EMPTY_PARSE"


# ---- Exponential backoff ----

def test_rate_limit_waits_three_polling_intervals():
    backoff, multiplier = next_backoff("RATE_LIMIT_429", 1)
    assert backoff == producer.INTERVAL_SEC * 3
    assert multiplier == 1, "429 should not escalate the multiplier"


def test_payload_error_waits_one_polling_interval():
    backoff, multiplier = next_backoff("TD_ERROR", 4)
    assert backoff == producer.INTERVAL_SEC
    assert multiplier == 4


def test_multiplier_doubles_on_consecutive_server_errors():
    multiplier = 1
    seen = []
    for _ in range(6):
        _, multiplier = next_backoff("SERVER", multiplier)
        seen.append(multiplier)
    assert seen == [2, 4, 8, 16, 32, 32], "multiplier must double and cap at 32"


def test_backoff_never_exceeds_configured_maximum():
    multiplier = 32
    backoff, _ = next_backoff("SERVER", multiplier)
    assert backoff == producer.BACKOFF_MAX_SEC


def test_backoff_never_drops_below_configured_minimum():
    backoff, _ = next_backoff("TD_ERROR", 1)
    assert backoff >= producer.BACKOFF_MIN_SEC


# ---- Price bounds stay in sync across the two validation layers ----

def test_producer_price_bounds_match_spark_validation():
    """
    The producer image does not ship spark/, so it keeps its own copy of the
    bounds. This test is what keeps the two definitions from drifting apart.
    """
    assert producer.PRICE_BOUNDS == SPARK_PRICE_BOUNDS
