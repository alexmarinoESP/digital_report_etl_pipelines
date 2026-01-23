"""Date utility functions extracted from working old code."""
import sys
from datetime import datetime, timedelta
from typing import Tuple
from loguru import logger


def convert_unix_to_datetime(timestamp: int, unit: str = "ms") -> datetime:
    """
    Convert Unix timestamp to datetime.

    Args:
        timestamp: Unix timestamp
        unit: "ms" for milliseconds, "s" for seconds

    Returns:
        datetime object
    """
    divisor = 1000 if unit == "ms" else 1
    return datetime.fromtimestamp(timestamp / divisor)


def build_date_from_parts(year: int, month: int, day: int) -> datetime:
    """
    Build datetime from separate year, month, day components.

    Args:
        year: Year
        month: Month (1-12)
        day: Day (1-31)

    Returns:
        datetime object
    """
    return datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")


def get_range_dates(days: int) -> Tuple[str, str]:
    """
    Calculate date range from days ago to today.

    Args:
        days: Number of days to look back

    Returns:
        Tuple of (since_date, until_date) in YYYY-MM-DD format
    """
    if not isinstance(days, int):
        logger.error("Days params must be int")
        sys.exit(-1)

    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")

    return since, until


def format_linkedin_date(year: int, month: int, day: int) -> str:
    """
    Format date for LinkedIn API format: (start:(year:YYYY,month:M,day:D))

    Args:
        year: Year
        month: Month (1-12)
        day: Day (1-31)

    Returns:
        LinkedIn date format string
    """
    return f"(start:(year:{year},month:{month},day:{day}))"