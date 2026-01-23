"""Shared processing utility functions extracted from working old code."""
import re
import emoji
import pandas as pd
from typing import List, Union, Optional, Any, Dict
from loguru import logger


def deEmojify(text: str) -> str:
    """
    Remove emoji characters from text.

    Args:
        text: Text string to clean

    Returns:
        Text without emojis
    """
    try:
        return "".join(c for c in text if c not in emoji.UNICODE_EMOJI)
    except TypeError as e:
        logger.debug("Nan found when trying to strip float. Going on.")
        return text


def remove_emojis(data: str) -> str:
    """
    Remove emojis using regex patterns.

    Args:
        data: Text string to clean

    Returns:
        Text without emojis
    """
    emoj = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002500-\U00002BEF"  # chinese char
        "\U00002702-\U000027B0"
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
        "\ufe0f"  # dingbats
        "\u3030"
        "]+",
        re.UNICODE,
    )
    return re.sub(emoj, "", data)


def remove_pipe_chars(text: str) -> str:
    """Remove pipe characters from text."""
    if text:
        return text.replace("|", "-")
    return text


def fix_id_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure ID columns are strings and cast dict/list columns to strings.

    Args:
        df: DataFrame to process

    Returns:
        DataFrame with fixed types
    """
    df = df.reset_index(drop=True)

    # Convert ID columns to strings
    check_id_cols = [x for x in df.columns if "id" in x]
    for id_col in check_id_cols:
        df[id_col] = df[id_col].astype(str)

    # Convert dict/list columns to strings
    for col in df.columns:
        if not df[col].dropna().empty:
            first_value = df[col].dropna().iloc[0]
            if isinstance(first_value, (dict, list)):
                logger.debug(f"Casted string column {col}")
                df[col] = df[col].astype(str)

    return df


def check_array_length(dic: Dict[str, List]) -> Dict[str, List]:
    """
    Check if values of dictionary are the same length. Otherwise add None element.

    Args:
        dic: Dictionary with list values

    Returns:
        Dictionary with balanced list lengths
    """
    dict_check = {k: len(v) for k, v in dic.items()}
    check = all(value == max(dict_check.values()) for value in dict_check.values())

    if not check:
        dict_check_fill = {
            k: v for k, v in dict_check.items() if v < max(dict_check.values())
        }.keys()
        for k in dict_check_fill:
            dic[k] = dic.get(k, []) + [None]

    return dic


def _handle_nested_dic_internal(
    outputdict: Dict, dic: Dict, nested_element: List[str]
) -> Dict:
    """Internal helper for handling nested dictionaries."""
    for key, value in dic.items():
        if isinstance(value, dict):
            if key in nested_element:
                for k2, v2 in value.items():
                    if isinstance(v2, dict):
                        for k3, v3 in v2.items():
                            if isinstance(v3, list):
                                for v3_key, v3_inner_value in v3[0].items():
                                    outputdict[v3_key] = outputdict.get(v3_key, []) + [
                                        v3_inner_value
                                    ]
                            else:
                                outputdict[key + "_" + k2 + "_" + k3] = outputdict.get(
                                    key + "_" + k2 + "_" + k3, []
                                ) + [v3]
                    else:
                        outputdict[key + "_" + k2] = outputdict.get(
                            key + "_" + k2, []
                        ) + [v2]
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


def handle_nested_response(
    alist: List[Dict], nested_element: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Convert nested dictionary response to DataFrame, flattening nested elements.

    Args:
        alist: List of dictionaries (API responses)
        nested_element: List of keys that contain nested dictionaries to flatten

    Returns:
        DataFrame with flattened columns

    Examples:
        Input: {'dailyBudget': {'currencyCode': 'EUR', 'amount': 10}}
        Output: Columns: dailyBudget_currencyCode, dailyBudget_amount
                Values: EUR, 10
    """
    outputdict = {}

    for dic in alist:
        if isinstance(dic, dict):
            outputdict = _handle_nested_dic_internal(
                outputdict=outputdict, nested_element=nested_element or [], dic=dic
            )
        else:
            for a in dic:
                outputdict = _handle_nested_dic_internal(
                    outputdict=outputdict, nested_element=nested_element or [], dic=a
                )

        outputdict = check_array_length(outputdict)

    return pd.DataFrame(dict([(k, pd.Series(v)) for k, v in outputdict.items()]))


def handle_simple_response(response: Union[dict, List[Dict]]) -> pd.DataFrame:
    """
    Convert simple response (dict or list of dicts) to DataFrame.

    Args:
        response: API response

    Returns:
        DataFrame
    """
    if isinstance(response, list):
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


def build_fields(fields: Union[List[str], None]) -> Union[str, None]:
    """
    Join list of fields into comma-separated string.

    Args:
        fields: List of field names

    Returns:
        Comma-separated string or None
    """
    if fields:
        return ",".join(fields)
    else:
        return None


def filter_data(alist: List[Dict], fields_to_retain: List[str]) -> List[Dict]:
    """
    Filter list of dictionaries to retain only specified fields.

    Args:
        alist: List of dictionaries
        fields_to_retain: List of keys to keep

    Returns:
        Filtered list of dictionaries
    """
    if fields_to_retain:
        alist_new = []
        for el in alist:
            tmp = {k: v for k, v in el.items() if k in fields_to_retain}
            alist_new.append(tmp)
        return alist_new
    else:
        return alist


def chunks(lst: List, n: int):
    """
    Yield successive n-sized chunks from list.

    Args:
        lst: List to chunk
        n: Chunk size

    Yields:
        Chunks of size n
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]