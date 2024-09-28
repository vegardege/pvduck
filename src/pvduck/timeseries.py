import hashlib
import random
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional


class TimestampOrder(Enum):
    CHRONOLOGICAL = "chronological"
    REVERSE_CHRONOLOGICAL = "reverse_chronological"
    RANDOM = "random"


def timeseries(
    start_date: datetime,
    end_date: Optional[datetime] = None,
    sample_rate: float = 1.0,
    seed: int | float | str | bytes | bytearray | None = None,
    order: TimestampOrder = TimestampOrder.RANDOM,
) -> list[datetime]:
    """Generate datetimes corresponding to Wikimedia dump files between the two
    dates. If `sample_rate` is less than 1.0, a random subset is chosen.

    Timestamps are ranked based on the `seed`, allowing you to change the date
    interval and/or increase the `sample_rate` without invalidating any
    previously generated timestamps.

    This means that if you expand the date range or increase the sample rate
    without invalidating previously generated timestamps. Look at the tests
    for a clearer idea of how this works.

    Args:
        start_date (datetime): The first valid datetime.
        end_date (Optional[datetime]): The last valid datetime.
            If None, the current date will be used.
        sample_rate (float): The probability of including a specific timestamp
            in the date range.
        seed (int | float | str | bytes | bytearray | None):
            The random seed to use for sampling.
        order (TimestampOrder): The order the datetimes are returned in.
            Can be `chronological`, `reverse_chronological`, or `random`.

    Yields:
        datetime: A datetime in the range.
    """
    # Ensure date interval is valid and round to nearest hour, which is
    # the granularity of dump files from Wikimedia.
    if end_date is None:
        end_date = datetime.now()
    if end_date < start_date:
        raise ValueError("end_date must be greater than start_date")

    start_date = start_date.replace(minute=0, second=0, microsecond=0)
    end_date = end_date.replace(minute=0, second=0, microsecond=0)

    # Generate all timestamps
    timestamps: list[tuple[datetime, float]] = []
    while start_date <= end_date:
        score = _timestamp_score(start_date, seed)
        if score < sample_rate:
            timestamps.append((start_date, score))
        start_date += timedelta(hours=1)

    # Determine order of returned timestamps
    if order == TimestampOrder.RANDOM:
        timestamps.sort(key=lambda x: x[1])
    elif order == TimestampOrder.REVERSE_CHRONOLOGICAL:
        timestamps.reverse()

    # Generate timestamps
    return [dt for dt, _ in timestamps]


def _timestamp_score(
    timestamp: datetime,
    seed: int | float | str | bytes | bytearray | None = None,
) -> float:
    """Create a score for the timestamp.

    The score of a timestamp is random, but deterministic for a single seed.
    This ensures consistency in how timestamps are ranked even if we change
    the date interval or increase the sample rate.

    A new seed will completely change the order, allowing a fresh sample.
    This should be avoided when working with the same file, as we intentionally
    want to avoid changing history when we expand the range of timestamps.

    Args:
        timestamp (datetime): The timestamp to score.
        seed (int | float | str | bytes | bytearray | None):
            The random seed to use for scoring.

    Returns:
        float: The score of the timestamp in the range [0.0, 1.0)
    """
    input = f"{timestamp.isoformat()}|{seed!r}"

    hash_digest = hashlib.md5(input.encode()).hexdigest()
    hash_int = int(hash_digest, 16)

    return random.Random(hash_int).random()
