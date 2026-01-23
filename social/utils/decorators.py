"""Shared decorator functions extracted from working old code."""
import time
from functools import wraps
from typing import Callable
from loguru import logger


def sleeper(secs: int):
    """
    Decorator to sleep after function execution.

    Args:
        secs: Seconds to sleep

    Returns:
        Decorated function
    """

    def wrapper(func: Callable):
        @wraps(func)
        def wrapper_func(*args, **kwargs):
            value = func(*args, **kwargs)
            logger.info(f"Sleeping for {secs} seconds")
            time.sleep(secs)
            return value

        return wrapper_func

    return wrapper


def log_df_dimension(f: Callable):
    """
    Decorator to log DataFrame dimensions after conversion.

    Args:
        f: Function that returns DataFrame

    Returns:
        Decorated function
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        df = f(*args, **kwargs)
        logger.info(
            f"Converted response into pd.DataFrame.\n" f"Number of rows {df.shape[0]}"
        )
        return df

    return wrapper


def retry(max_tries: int = 3, sleep_secs: int = 3610):
    """
    Retry decorator with fixed backoff.

    Args:
        max_tries: Maximum number of attempts
        sleep_secs: Seconds to sleep between retries

    Returns:
        Decorated function
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_tries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_tries:
                        logger.error(
                            f"All {max_tries} attempts failed for {func.__name__}"
                        )
                        raise
                    logger.warning(f"Attempt {attempt} failed: {e}")
                    logger.info(f"Retrying in {sleep_secs} seconds")
                    time.sleep(sleep_secs)

        return wrapper

    return decorator


def retrybackoffexp(max_tries: int = 3, base_secs: int = 30):
    """
    Retry decorator with exponential backoff.

    Args:
        max_tries: Maximum number of attempts
        base_secs: Base seconds for exponential calculation

    Returns:
        Decorated function
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_tries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_tries:
                        logger.error(
                            f"All {max_tries} attempts failed for {func.__name__}"
                        )
                        return None
                    wait_time = base_secs ** (attempt + 1)
                    logger.warning(f"Attempt {attempt} failed: {e}")
                    logger.info(f"Exponential backoff: Retrying in {wait_time} seconds")
                    time.sleep(wait_time)

        return wrapper

    return decorator