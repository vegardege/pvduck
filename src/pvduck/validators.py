from datetime import date, datetime
from typing import Any, Optional


def mandatory_datetime(input: Any) -> datetime:
    """Validate an input date and convert it to a datetime object.

    Args:
        input (Any): The input date to validate.

    Returns:
        Optional[datetime]: The validated datetime object or None if the input
            is an empty string.
    """
    if not isinstance(input, date):
        raise ValueError(f"Invalid date format: {input}")

    return datetime.combine(input, datetime.min.time())


def optional_datetime(input: Any) -> Optional[datetime]:
    """Validate an input date and convert it to a datetime object.

    Args:
        input (Any): The input date to validate.

    Returns:
        Optional[datetime]: The validated datetime object or None if the input
            is an empty string.
    """
    if not input:
        return None

    return mandatory_datetime(input)
