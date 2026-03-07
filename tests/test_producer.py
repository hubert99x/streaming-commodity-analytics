from datetime import datetime, timezone

from producer.producer import (
    active_symbols_for_fetch,
    clamp,
    is_fx_weekend_closed,
    should_publish,
)


def test_clamp_returns_value_inside_range():
    assert clamp(5, 1, 10) == 5


def test_clamp_returns_lower_bound_when_value_is_too_small():
    assert clamp(-5, 1, 10) == 1


def test_clamp_returns_upper_bound_when_value_is_too_large():
    assert clamp(99, 1, 10) == 10


def test_fx_is_closed_on_saturday():
    dt = datetime(2026, 3, 7, 12, 0, 0, tzinfo=timezone.utc)
    assert is_fx_weekend_closed(dt) is True


def test_fx_is_closed_on_friday_after_22_utc():
    dt = datetime(2026, 3, 6, 22, 30, 0, tzinfo=timezone.utc)
    assert is_fx_weekend_closed(dt) is True


def test_fx_is_open_on_monday():
    dt = datetime(2026, 3, 9, 10, 0, 0, tzinfo=timezone.utc)
    assert is_fx_weekend_closed(dt) is False


def test_should_publish_btc_on_weekend():
    dt = datetime(2026, 3, 7, 12, 0, 0, tzinfo=timezone.utc)
    assert should_publish("BTC/USD", dt) is True


def test_should_not_publish_eurusd_on_weekend():
    dt = datetime(2026, 3, 7, 12, 0, 0, tzinfo=timezone.utc)
    assert should_publish("EUR/USD", dt) is False


def test_should_not_publish_xauusd_on_weekend():
    dt = datetime(2026, 3, 7, 12, 0, 0, tzinfo=timezone.utc)
    assert should_publish("XAU/USD", dt) is False


def test_active_symbols_for_fetch_on_weekend():
    dt = datetime(2026, 3, 7, 12, 0, 0, tzinfo=timezone.utc)
    symbols = active_symbols_for_fetch(dt)

    assert "BTC/USD" in symbols
    assert "EUR/USD" not in symbols
    assert "XAU/USD" not in symbols


def test_active_symbols_for_fetch_when_market_is_open():
    dt = datetime(2026, 3, 9, 10, 0, 0, tzinfo=timezone.utc)
    symbols = active_symbols_for_fetch(dt)

    assert "BTC/USD" in symbols
    assert "EUR/USD" in symbols
    assert "XAU/USD" in symbols