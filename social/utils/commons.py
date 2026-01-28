"""
Common utility functions for social module.
"""

import os
import re
import sys
import time
from datetime import datetime, timedelta
from typing import Any, AnyStr, Callable, Dict, List, Tuple, Union

import emoji
import joblib
import pandas as pd
from loguru import logger
from requests import Response


def find_between(s, first, last):
    """Find substring between two delimiters."""
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ""


def check_limit(check):
    """
    Check Facebook API rate limit from response headers.

    Args:
        check: Facebook API response

    Returns:
        CPU usage percentage
    """
    if isinstance(check, list):
        if check:
            check = check[-1]
        else:
            logger.warning("Empty response list")
            return 10

    if check is None or not hasattr(check, "_headers") or check._headers is None:
        logger.warning("Response has no headers")
        return 10

    try:
        cpu = find_between(check._headers["x-business-use-case-usage"], 'total_cputime":', "}")[:4]
        cpu = re.search(r"[\d]+", cpu).group(0)
        return int(cpu)
    except (ValueError, KeyError):
        try:
            cpu = find_between(check._headers["x-app-usage"], 'total_cputime":', "}")[:4]
            cpu = re.search(r"[\d]+", cpu).group(0)
            return int(cpu)
        except (ValueError, KeyError, AttributeError) as e:
            logger.exception(f"Can't find cputime: {e}")
            return 10


def is_production_env():
    """Check if running in production (non-Windows) environment."""
    return sys.platform != "win32"


def dump_files(objs: List[Any], path: AnyStr, files: List[AnyStr]) -> None:
    """Dump objects to pickle files."""
    path_f = [os.path.join(path, f) for f in files]
    for o, p in zip(objs, path_f):
        joblib.dump(o, p)


def read_files(path: AnyStr, files: List[AnyStr]) -> Union[Tuple, Any]:
    """Read pickle files."""
    path_f = [os.path.join(path, f) for f in files]
    f = map(joblib.load, path_f)
    obj = tuple(f)
    return obj[0] if len(obj) == 1 else obj


def sleeper(secs):
    """Decorator to add sleep after function execution."""
    def wrapper(func: Callable):
        def wrapper_func(*args, **kwargs):
            value = func(*args, **kwargs)
            logger.info(f"Sleeping for {secs} seconds")
            time.sleep(secs)
            return value
        return wrapper_func
    return wrapper


def log_df_dimension(f):
    """Decorator to log DataFrame dimensions."""
    def wrapper(*args, **kwargs):
        df = f(*args, **kwargs)
        logger.info(f"Converted to DataFrame: {df.shape[0]} rows")
        return df
    return wrapper


def retry(fun, *args, **kwargs):
    """
    Retry decorator with fixed delay.

    Args:
        fun: Function to retry

    Returns:
        Decorated function
    """
    def wrapper(*args, **kwargs):
        max_tries = 3
        n_sec = 3610
        for tries in range(1, max_tries):
            try:
                return fun(*args, **kwargs)
            except Exception as e:
                logger.warning(f"Request failed: {e}")
                logger.info(f"Retrying in {n_sec} secs")
                time.sleep(n_sec)
        return None
    return wrapper


def retrybackoffexp(fun, *args, **kwargs):
    """
    Retry decorator with exponential backoff.

    Args:
        fun: Function to retry

    Returns:
        Decorated function
    """
    def wrapper(*args, **kwargs):
        max_tries = 3
        n_sec = 30
        for tries in range(1, max_tries):
            try:
                return fun(*args, **kwargs)
            except Exception as e:
                logger.warning(f"Request failed: {e}")
                logger.info(f"Retrying in {n_sec**(tries+1)} seconds")
                time.sleep(n_sec ** (tries + 1))
        logger.error(f"All {max_tries} retry attempts failed")
        return None
    return wrapper


def deEmojify(text: str) -> str:
    """Remove emojis from text."""
    try:
        return "".join(c for c in text if c not in emoji.UNICODE_EMOJI)
    except TypeError:
        logger.debug("NaN found when stripping emoji")
        return text


def remove_emojis(data: str) -> str:
    """Remove emoji characters using regex."""
    emoj = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002500-\U00002BEF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642"
        "\u2600-\u2B55"
        "\u200d"
        "\u23cf"
        "\u23e9"
        "\u231a"
        "\ufe0f"
        "\u3030"
        "]+",
        re.UNICODE,
    )
    return re.sub(emoj, "", data)


def fix_id_type(df: pd.DataFrame) -> pd.DataFrame:
    """Fix ID column types to string."""
    df = df.reset_index(drop=True)

    check_id_cols = [x for x in df.columns if "id" in x]
    for col in check_id_cols:
        df[col] = df[col].astype(str)

    for col in df.columns:
        if df[col].dropna().shape[0] > 0:
            first_val = df[col].dropna().iloc[0]
            if isinstance(first_val, (dict, list)):
                df[col] = df[col].astype(str)

    return df


def check_array_length(dic: dict) -> dict:
    """Ensure all dictionary values have same length."""
    dict_check = {k: len(v) for k, v in dic.items()}
    max_len = max(dict_check.values()) if dict_check else 0

    for k, v in dict_check.items():
        if v < max_len:
            # Pad with enough None values to reach max_len
            dic[k] = dic.get(k, []) + [None] * (max_len - v)

    return dic


def _handle_nested_dic_internal(outputdict, dic, nested_element):
    """Internal handler for nested dictionaries."""
    for key, value in dic.items():
        if isinstance(value, dict):
            if key in nested_element:
                for k2, v2 in value.items():
                    if isinstance(v2, dict):
                        for k3, v3 in v2.items():
                            if isinstance(v3, list):
                                for v3_key, v3_inner in v3[0].items():
                                    outputdict[v3_key] = outputdict.get(v3_key, []) + [v3_inner]
                            else:
                                full_key = f"{key}_{k2}_{k3}"
                                outputdict[full_key] = outputdict.get(full_key, []) + [v3]
                    else:
                        full_key = f"{key}_{k2}"
                        outputdict[full_key] = outputdict.get(full_key, []) + [v2]
            else:
                for k2, v2 in value.items():
                    if isinstance(v2, list):
                        outputdict[k2] = outputdict.get(k2, []) + [v2[0]]
                    else:
                        outputdict[k2] = outputdict.get(k2, []) + [v2]
        else:
            if isinstance(value, list):
                outputdict[key] = outputdict.get(key, []) + [value[0]]
            else:
                outputdict[key] = outputdict.get(key, []) + [value]

    return outputdict


def handle_nested_response(alist: List, nested_element: List) -> pd.DataFrame:
    """Handle nested dictionary response."""
    outputdict = {}

    for dic in alist:
        if isinstance(dic, dict):
            outputdict = _handle_nested_dic_internal(outputdict, dic, nested_element)
        else:
            for a in dic:
                outputdict = _handle_nested_dic_internal(outputdict, a, nested_element)
        outputdict = check_array_length(outputdict)

    return pd.DataFrame(dict([(k, pd.Series(v)) for k, v in outputdict.items()]))


def handle_simple_response(response: Union[dict, List]) -> pd.DataFrame:
    """Handle simple (non-nested) response."""
    if isinstance(response, List):
        dfs = []
        for idx, r in enumerate(response):
            try:
                dfs.append(pd.DataFrame(r))
            except ValueError:
                dfs.append(pd.DataFrame(r, index=[idx]))
        return pd.concat(dfs)
    else:
        try:
            return pd.DataFrame(response)
        except ValueError:
            return pd.DataFrame(response, index=[0])


def get_range_dates(days: int) -> Tuple[str, str]:
    """Get date range as strings."""
    if not isinstance(days, int):
        logger.error("Days must be int")
        sys.exit(-1)

    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")

    return since, until


def check_array_length(dic: dict) -> dict:
    """Ensure all dictionary values have same length by padding with None."""
    dict_check = {k: len(v) for k, v in dic.items()}
    max_len = max(dict_check.values()) if dict_check else 0

    for k, v in dict_check.items():
        if v < max_len:
            # Pad with enough None values to reach max_len
            dic[k] = dic.get(k, []) + [None] * (max_len - v)

    return dic


def _handle_nested_dic_internal(outputdict, dic, nested_element):
    """Internal handler for nested dictionaries - flattens nested structures."""
    for key, value in dic.items():
        if isinstance(value, dict):
            if key in nested_element:
                # This is a nested element we want to flatten
                for k2, v2 in value.items():
                    if isinstance(v2, dict):
                        # Nested 3 levels deep
                        for k3, v3 in v2.items():
                            if isinstance(v3, list):
                                # Handle list values
                                for v3_key, v3_inner_value in v3[0].items():
                                    outputdict[v3_key] = outputdict.get(v3_key, []) + [v3_inner_value]
                            else:
                                # Create flattened key: totalBudget_amount_...
                                full_key = f"{key}_{k2}_{k3}"
                                outputdict[full_key] = outputdict.get(full_key, []) + [v3]
                    else:
                        # Nested 2 levels: totalBudget_amount
                        full_key = f"{key}_{k2}"
                        outputdict[full_key] = outputdict.get(full_key, []) + [v2]
            else:
                # Not in nested_element list - extract values directly
                for k2, v2 in value.items():
                    if isinstance(v2, list):
                        outputdict[k2] = outputdict.get(k2, []) + [v2[0]]
                    else:
                        outputdict[k2] = outputdict.get(k2, []) + [v2]
        else:
            # Simple value
            if isinstance(value, list):
                outputdict[key] = outputdict.get(key, []) + [value[0]]
            else:
                outputdict[key] = outputdict.get(key, []) + [value]

    return outputdict


def handle_nested_response(alist: List, nested_element: List) -> pd.DataFrame:
    """
    Handle nested dictionary response and flatten nested structures.

    This function converts nested API responses into a flat DataFrame suitable for database insertion.
    Nested elements specified in the nested_element list will be flattened with underscore-separated keys.

    Args:
        alist: List of dictionaries from API response
        nested_element: List of keys that contain nested dictionaries to flatten

    Returns:
        DataFrame with flattened columns

    Examples:
        Input: {'dailyBudget': {'currencyCode': 'EUR', 'amount': 10}}
        With nested_element=['dailyBudget']
        Output columns: dailyBudget_currencyCode, dailyBudget_amount
        Values: 'EUR', 10
    """
    outputdict = {}

    for dic in alist:
        if isinstance(dic, dict):
            outputdict = _handle_nested_dic_internal(outputdict, dic, nested_element)
        else:
            # Handle list of lists
            for a in dic:
                outputdict = _handle_nested_dic_internal(outputdict, a, nested_element)

        # Ensure all arrays have same length
        outputdict = check_array_length(outputdict)

    return pd.DataFrame(dict([(k, pd.Series(v)) for k, v in outputdict.items()]))


def handle_simple_response(response: Union[dict, List]) -> pd.DataFrame:
    """Handle simple (non-nested) response by converting directly to DataFrame."""
    if isinstance(response, List):
        dfs = []
        for idx, r in enumerate(response):
            try:
                dfs.append(pd.DataFrame(r))
            except ValueError:
                dfs.append(pd.DataFrame(r, index=[idx]))
        return pd.concat(dfs, ignore_index=True)
    else:
        try:
            return pd.DataFrame(response)
        except ValueError:
            return pd.DataFrame(response, index=[0])


def extract_targeting_criteria(campaigns: List[Dict]) -> pd.DataFrame:
    """
    Extract audience_id from LinkedIn campaign targetingCriteria.

    This function parses the nested targetingCriteria structure to extract
    audience segment IDs for campaign-audience relationships.

    Args:
        campaigns: List of campaign dictionaries from LinkedIn API

    Returns:
        DataFrame with columns: id, audience_id

    Examples:
        Input campaign with targetingCriteria containing audience segments
        Output: DataFrame with campaign id and extracted audience_id
    """
    audiences = [
        "urn:li:adTargetingFacet:audienceMatchingSegments",
        "urn:li:adTargetingFacet:dynamicSegments",
    ]
    segments = []
    ids = []

    for campaign in campaigns:
        try:
            targeting = campaign.get("targetingCriteria", {})
            target = targeting.get("include", {}).get("and", [])

            # DEBUG: Log first campaign's targeting structure
            if campaign == campaigns[0]:
                logger.debug(f"First campaign targetingCriteria keys: {targeting.keys() if targeting else 'None'}")
                logger.debug(f"First campaign include.and length: {len(target)}")

            # Extract elements from targeting
            # Each item in "and" has an "or" key with a dict of facets
            elements_target = [item.get("or", {}) for item in target if isinstance(item, dict)]

            # Look for audience facets
            segment = []
            for aud_facet in audiences:
                for elem in elements_target:
                    # elem is a dict like {"urn:li:adTargetingFacet:dynamicSegments": ["urn:li:adSegment:123", ...]}
                    if aud_facet in elem:
                        val = elem.get(aud_facet)
                        if val:
                            # val is a list of URNs, take first one
                            segment.append(val[0] if isinstance(val, list) else val)

            segment = list(filter(None, segment))

            if len(segment) > 0:
                # Extract segment ID from URN
                seg = segment[0]
                if "urn:li:adSegment:" in seg:
                    seg = seg.split("urn:li:adSegment:")[1]
                    segments.append(seg)
                else:
                    segments.append(None)
            else:
                segments.append(None)

            ids.append(campaign.get("id"))

        except Exception as e:
            logger.debug(f"Could not extract targeting for campaign: {e}")
            ids.append(campaign.get("id"))
            segments.append(None)

    df = pd.DataFrame({"id": ids, "audience_id": segments})
    non_null_count = df["audience_id"].notna().sum()
    logger.debug(f"extract_targeting_criteria: {len(df)} campaigns, {non_null_count} with audience_id")
    return df
