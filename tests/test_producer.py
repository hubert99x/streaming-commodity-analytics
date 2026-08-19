from datetime import datetime, timedelta, timezone

import pytest

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


# ---- Exact edges of the closing window (Fri 22:00:00 - Sun 21:59:59 UTC) ----

@pytest.mark.parametrize(
    "dt, closed",
    [
        # Friday 2026-03-06
        (datetime(2026, 3, 6, 21, 59, 59, tzinfo=timezone.utc), False),
        (datetime(2026, 3, 6, 22, 0, 0, tzinfo=timezone.utc), True),
        # Sunday 2026-03-08
        (datetime(2026, 3, 8, 21, 59, 59, tzinfo=timezone.utc), True),
        (datetime(2026, 3, 8, 22, 0, 0, tzinfo=timezone.utc), False),
    ],
)
def test_fx_gate_boundary_instants(dt, closed):
    assert is_fx_weekend_closed(dt) is closed


def test_fx_gate_treats_naive_datetime_as_utc():
    # The missing tzinfo is the point of this test, so DTZ001 does not apply.
    naive = datetime(2026, 3, 7, 12, 0, 0)  # noqa: DTZ001
    assert is_fx_weekend_closed(naive) is True


def test_fx_gate_converts_non_utc_timezone_before_comparing():
    """21:00 in UTC+2 is 19:00 UTC on Friday, so the market is still open."""
    aware = datetime(2026, 3, 6, 21, 0, 0, tzinfo=timezone(timedelta(hours=2)))
    assert is_fx_weekend_closed(aware) is False


def test_fx_gate_closes_when_non_utc_time_crosses_the_boundary():
    """01:00 Saturday in UTC+2 is 23:00 Friday UTC, so the market is closed."""
    aware = datetime(2026, 3, 7, 1, 0, 0, tzinfo=timezone(timedelta(hours=2)))
    assert is_fx_weekend_closed(aware) is True