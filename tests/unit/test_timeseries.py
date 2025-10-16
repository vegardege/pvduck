from datetime import datetime, timedelta

import pytest

from pvduck.timeseries import TimestampOrder, timeseries


def test_basic() -> None:
    """Make sure the time series generator works with the default
    parameters."""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 2)
    result = timeseries(start_date, end_date)

    assert len(result) == 25
    assert all(isinstance(ts, datetime) for ts in result)
    assert all(ts >= start_date and ts <= end_date for ts in result)

    unbounded = timeseries(datetime.now() - timedelta(days=1))
    assert max(unbounded) == datetime.now().replace(
        minute=0, second=0, microsecond=0
    )


def test_erroneous_input() -> None:
    """Make sure we can't generate a time series with invalid parameters."""
    # Invalid date range raises an error
    start_date = datetime(2024, 1, 2)
    end_date = datetime(2024, 1, 1)

    with pytest.raises(ValueError):
        timeseries(start_date, end_date)

    # Sample rate below 0.0 is allowed, but returns nothing
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 2)

    result = timeseries(start_date, end_date, sample_rate=-1.0)
    assert len(result) == 0

    # Sample rate above 1.0 is allowed, but returns everything
    result = timeseries(start_date, end_date, sample_rate=2.0)
    assert len(result) == 25


def test_sample_rate() -> None:
    """Sample rate determines how many timestamps are returned."""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 2)

    sample_all = timeseries(start_date, end_date, sample_rate=1.0)
    sample_half = timeseries(start_date, end_date, sample_rate=0.5)
    sample_one_tenth = timeseries(start_date, end_date, sample_rate=0.1)

    assert len(sample_all) == 25
    assert len(sample_all) > len(sample_half) > len(sample_one_tenth) > 0


def test_order() -> None:
    """Make sure the result sets are sorted properly."""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 2)

    # Chronological order
    chronological = timeseries(
        start_date, end_date, order=TimestampOrder.CHRONOLOGICAL
    )
    assert chronological[0] == start_date
    assert chronological[-1] == end_date
    assert chronological == sorted(chronological)

    # Reverse chronological order
    reverse_chronological = timeseries(
        start_date, end_date, order=TimestampOrder.REVERSE_CHRONOLOGICAL
    )
    assert reverse_chronological[0] == end_date
    assert reverse_chronological[-1] == start_date
    assert reverse_chronological == sorted(chronological, reverse=True)

    # Random order
    random_order = timeseries(
        start_date, end_date, order=TimestampOrder.RANDOM
    )
    assert random_order != chronological
    assert random_order != reverse_chronological
    assert (
        set(random_order) == set(chronological) == set(reverse_chronological)
    )


def test_seed() -> None:
    """Ensure randomness is deterministic, but changes with changed seed."""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 2)

    ts1 = timeseries(start_date, end_date, seed=42)
    ts2 = timeseries(start_date, end_date, seed=1337)
    ts3 = timeseries(start_date, end_date, seed=42)

    # All time series should have the same elements, but in different orders
    # based on the seed.
    assert len(ts1) == len(ts2) == len(ts3)
    assert set(ts1) == set(ts2) == set(ts3)

    assert ts1 == ts3
    assert ts2 != ts1

    # If we sample, seeds will result in different time series
    ts4 = timeseries(start_date, end_date, seed=42, sample_rate=0.5)
    ts5 = timeseries(start_date, end_date, seed=1337, sample_rate=0.5)
    ts6 = timeseries(start_date, end_date, seed=42, sample_rate=0.5)

    assert set(ts4) != set(ts5)
    assert set(ts4) == set(ts6)

    assert ts4 == ts6
    assert ts5 != ts4


def test_expansions() -> None:
    """When seed is the same, expanding should make the time series longer,
    but not change history."""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 2)
    end_date_expanded = datetime(2024, 1, 3)

    # Without sampling, the time series should just grow
    ts_initial = timeseries(
        start_date,
        end_date,
        order=TimestampOrder.CHRONOLOGICAL,
    )
    ts_expanded = timeseries(
        start_date,
        end_date_expanded,
        order=TimestampOrder.CHRONOLOGICAL,
    )

    assert len(ts_initial) < len(ts_expanded)
    assert ts_initial == ts_expanded[: len(ts_initial)]

    # With sampling, the expanded time series should be a superset of the
    # initial time series, even with random ordering.
    ts_initial = timeseries(
        start_date,
        end_date,
        sample_rate=0.5,
    )
    ts_expanded = timeseries(
        start_date,
        end_date_expanded,
        sample_rate=0.5,
    )
    assert len(ts_initial) < len(ts_expanded)
    assert set(ts_initial).issubset(set(ts_expanded))
