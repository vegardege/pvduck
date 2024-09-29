from datetime import date, datetime

import pytest

from pvduck.validators import mandatory_datetime, optional_datetime


def test_mandatory_datetime():
    """Test mandatory_datetime function."""
    assert mandatory_datetime(date(2024, 9, 1)) == datetime(2024, 9, 1)
    assert mandatory_datetime(datetime(2024, 9, 1)) == datetime(2024, 9, 1)
    with pytest.raises(ValueError):
        mandatory_datetime("2024-09-01")
    with pytest.raises(ValueError):
        mandatory_datetime(None)


def test_optional_datetime():
    """Test optional_datetime function."""
    assert optional_datetime(date(2024, 9, 1)) == datetime(2024, 9, 1)
    assert optional_datetime(datetime(2024, 9, 1)) == datetime(2024, 9, 1)
    assert optional_datetime("") is None
    assert optional_datetime(None) is None
    with pytest.raises(ValueError):
        optional_datetime("2024-09-01")
