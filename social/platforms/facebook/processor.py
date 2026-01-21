"""
Facebook Ads data processor.
Handles data transformation and cleaning for Facebook Ads data.
"""

import json
from datetime import datetime
from typing import List, Dict

import numpy as np
import pandas as pd
from loguru import logger

from social.platforms.facebook import accounts_company
from social.utils.commons import deEmojify


class FBAdsProcess:
    """
    Processor for Facebook Ads data.
    Transforms raw API responses into clean DataFrames.
    """

    def __init__(self, response: pd.DataFrame):
        """
        Initialize processor with response data.

        Args:
            response: DataFrame from Facebook API
        """
        self.response = response

    def get_df(self) -> pd.DataFrame:
        """Return the processed DataFrame."""
        return self.response

    def utf_8_encoding(self, cols: List) -> None:
        """
        Encode columns to UTF-8 and remove emojis.

        Args:
            cols: List of column names to encode
        """
        for c in cols:
            try:
                self.response[c] = self.response[c].replace(np.nan, "")
                self.response[c] = self.response[c].apply(
                    lambda x: x.replace("|", "")
                )
                self.response[c] = self.response[c].apply(
                    lambda x: deEmojify(x) if x is not None else x
                )
                self.response[c] = self.response[c].apply(
                    lambda x: x.encode("utf-8") if x is not None else x
                )
            except KeyError as e:
                logger.info(f"Column missing, adding: {e.args[0]}")
                self.response[e.args[0]] = None

    def rename_columns(self, col_dict: Dict) -> None:
        """Rename columns according to mapping."""
        self.response.rename(columns=col_dict, inplace=True)

    def convert_actions_to_df(self, **kwargs) -> None:
        """
        Convert actions field (list of dicts) to long-format DataFrame.
        Each row becomes an action_type with its value.
        """
        response_list = []
        i = 0
        for ad_id, action in zip(self.response["ad_id"], self.response["actions"]):
            if isinstance(action, list):
                act_tmp = pd.DataFrame(action)
                act_tmp["ad_id"] = ad_id
                response_list.append(act_tmp)
            else:
                response_list.append(
                    pd.DataFrame({
                        "action_target_id": np.nan,
                        "action_type": np.nan,
                        "value": np.nan,
                        "ad_id": ad_id,
                    }, index=[i])
                )
                i += 1

        actions = pd.concat(response_list)
        self.response = actions.reset_index(drop=True)

    def add_companyid(self, **kwargs) -> None:
        """Add company ID based on account ID mapping."""
        companies = []
        for i in self.response["account_id"]:
            companies.append(accounts_company.get(str(i), None))
        self.response["companyid"] = companies

    def drop_columns(self, cols: List) -> None:
        """Drop specified columns."""
        self.response.drop(columns=cols, inplace=True)

    def deal_with_date(self, cols: List) -> None:
        """
        Handle date fields - convert to datetime.

        Args:
            cols: List of date column names
        """
        for c in cols:
            self.response[c] = self.response[c].replace(np.NaN, 0).replace(0, None)
            self.response[c] = self.response[c].apply(
                lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S%z")
                if not (isinstance(x, float) or x is None)
                else x
            )

    def explode_df(self, column_explode: List) -> None:
        """Expand DataFrame by exploding specified columns."""
        self.response = self.response.explode(column=column_explode)
        self.response.dropna(inplace=True)

    def extract_pixel_rule(self, **kwargs) -> None:
        """Extract pixel rule from conversion data."""
        ad_set_rule = []
        for id, rule in zip(self.response["conversion_id"], self.response["rule"]):
            if isinstance(rule, str):
                jrule = json.loads(rule)
                event_rule = jrule["and"][0].get("event", {}).get("eq")
                all_url_rule = jrule["and"][1].get("or", {})
                ad_set_rule.append({
                    "RULE": event_rule,
                    "CONV_TRIGGER": all_url_rule,
                })
            else:
                ad_set_rule.append(np.nan)
        self.response["pixel_rule"] = ad_set_rule

    def nan_conversion(self, **kwargs) -> None:
        """Fill NaN in action_type with 'nan' string."""
        self.response["action_type"] = self.response["action_type"].fillna("nan")

    def extract_custom_conversion_id(self, **kwargs) -> None:
        """Extract custom conversion ID from action type."""
        id_split = self.response["action_type"].str.split(
            "offsite_conversion.custom.", expand=True
        )
        try:
            self.response["conversion_id"] = id_split[1]
        except KeyError:
            self.response["conversion_id"] = None

    def modify_name(self, cols: List) -> None:
        """Remove pipe characters from specified columns."""
        for c in cols:
            if c in self.response.columns:
                self.response[c] = self.response[c].str.replace("|", "-")

    def add_row_loaded_date(self, **kwargs) -> None:
        """Add row_loaded_date column with current timestamp."""
        self.response["row_loaded_date"] = datetime.now()
