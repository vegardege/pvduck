from datetime import date, datetime
from typing import Optional


def mandatory_datetime(input: date) -> datetime:
    """Validate an input date and convert it to a datetime object.

    Args:
        input (date): The input date to validate.

    Returns:
        Optional[datetime]: The validated datetime object or None if the input
            is an empty string.
    """
    return datetime.combine(input, datetime.min.time())


def optional_datetime(input: date) -> Optional[datetime]:
    """Validate an input date and convert it to a datetime object.

    Args:
        input (date): The input date to validate.

    Returns:
        Optional[datetime]: The validated datetime object or None if the input
            is an empty string.
    """
    if not input:
        return None

    return mandatory_datetime(input)
