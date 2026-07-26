"""Tests for the Kafka consumer lag arithmetic."""

import importlib.util
import pathlib

import pytest

# kafka-lag lives in a directory whose name is not a valid module name
_spec = importlib.util.spec_from_file_location(
    "kafka_lag",
    pathlib.Path(__file__).resolve().parents[1] / "ops" / "kafka-lag" / "kafka_lag.py",
)
kafka_lag = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(kafka_lag)

partition_lag = kafka_lag.partition_lag


def test_no_lag_when_everything_is_processed():
    assert partition_lag(high=100, low=0, last_processed=99) == 0


def test_lag_counts_messages_after_the_last_processed_offset():
    assert partition_lag(high=100, low=0, last_processed=90) == 9


def test_full_partition_is_pending_when_nothing_processed_yet():
    assert partition_lag(high=100, low=0, last_processed=None) == 100


def test_empty_partition_reports_no_lag_without_processed_rows():
    """
    An empty partition has low == high. Once retention drops the last raw_prices
    row for such a partition, lag must stay 0 instead of jumping to the high
    watermark and firing the critical alert.
    """
    assert partition_lag(high=9562, low=9562, last_processed=None) == 0


def test_expired_messages_are_not_counted_as_pending():
    """Offsets below the log start offset are gone from Kafka, not pending."""
    assert partition_lag(high=9600, low=9562, last_processed=None) == 38


def test_stale_processed_offset_below_log_start_is_ignored():
    """A processed offset older than the retained window must not inflate lag."""
    assert partition_lag(high=9600, low=9562, last_processed=100) == 38


@pytest.mark.parametrize("last_processed", [None, 0, 50, 99, 500])
def test_lag_is_never_negative(last_processed):
    assert partition_lag(high=100, low=0, last_processed=last_processed) >= 0
