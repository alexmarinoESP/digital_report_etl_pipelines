"""
Google Ads data processor.
Handles data transformation and cleaning for Google Ads data.
"""

import logging
import re
import sys
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd

from social.services.alert_service import send_alert
from social.platforms.google import manager_to_company
from social.utils.commons import remove_emojis, deEmojify


class GoogleAdsProcess:
    """
    Processor for Google Ads data.
    Transforms raw API responses into clean DataFrames.
    """

    def __init__(self, response: pd.DataFrame):
        """
        Initialize processor with response data.

        Args:
            response: DataFrame from Google Ads API
        """
        self.response = response

    def get_df(self) -> pd.DataFrame:
        """Return the processed DataFrame."""
        return self.response

    def handle_columns(self, **ignored) -> None:
        """
        Clean column names.
        Removes prefixes and normalizes names.
        """
        self.response.columns = [
            i.split(".", 1)[1] if "customer" not in i and "." in i else i
            for i in list(self.response.columns)
        ]
        self.response.columns = [
            i.replace(".", "_") for i in list(self.response.columns)
        ]
        cols = [c for c in self.response.columns if "resource" not in c]
        self.response = self.response[cols]
        self.response.columns = [i.lower() for i in self.response.columns]

    def add_company(self, **ignored) -> None:
        """Add COMPANYID based on manager_id mapping."""
        self.response["COMPANYID"] = [
            manager_to_company.get(str(i))
            for i in self.response["manager_id"]
        ]

    def add_row_loaded_date(self, **ignored) -> None:
        """Add row_loaded_date column."""
        self.response["row_loaded_date"] = datetime.now()

    def convert_costs(self, col: List, **ignored) -> None:
        """
        Convert cost columns from micros to actual values.

        Args:
            col: List of cost column names
        """
        if not self.response.empty:
            for c in col:
                try:
                    self.response[c] = self.response[c].fillna(0)
                    self.response[c] = self.response[c].astype(np.int64)
                    self.response[c] = self.response[c].apply(lambda x: x / 1_000_000)
                except KeyError:
                    logging.info(f"Column {c} not in data")

    def deal_with_date(self, cols: List, **ignored) -> None:
        """Convert date columns to datetime."""
        if not self.response.empty:
            for c in cols:
                self.response[c] = self.response[c].apply(
                    lambda x: datetime.strptime(x, "%Y-%m-%d")
                )

    def rename_columns(self, **ignored) -> None:
        """Rename columns to standard names."""
        if not self.response.empty:
            self.response.rename(
                columns={
                    "startdate": "start_date",
                    "enddate": "end_date",
                    "servingstatus": "serving_status",
                    "activeviewctr": "active_view_ctr",
                    "customer_id": "customer_id_google",
                    "customer.id": "customer_id_google",
                    "placementtype": "placement_type",
                    "resourcename": "resource_name",
                    "resourceName": "resource_name",
                    "timeZone": "time_zone",
                    "displayname": "display_name",
                    "descriptiveName": "descriptive_name",
                    "targeturl": "target_url",
                    "costMicros": "cost_micros",
                    "costmicros": "cost_micros",
                    "clientcustomer": "client_customer",
                    "clientCustomer": "client_customer",
                    "timezone": "time_zone",
                    "descriptivename": "descriptive_name",
                    "currencycode": "currency_code",
                    "currencyCode": "currency_code",
                    "campaign.id": "campaign_id",
                    "adGroup.id": "adgroup_id",
                    "ad.id": "ad_id",
                },
                inplace=True,
            )

    def modify_name(self, cols: List, **ignored) -> None:
        """Remove pipe characters from columns."""
        if not self.response.empty:
            for c in cols:
                self.response[c] = self.response[c].str.replace("|", "-", regex=False)

    def fill_view_ctr_nan(self, cols: List) -> None:
        """Fill NaN values with 0 in specified columns."""
        for c in cols:
            self.response[c].fillna(0, inplace=True)

    def drop_duplicates(self, **kwargs) -> None:
        """Drop duplicate rows."""
        self.response.drop_duplicates(inplace=True)

    def dropna_value(self, **kwargs) -> None:
        """Drop rows with NaN values."""
        self.response.dropna(inplace=True)
        f = sys._getframe(0).f_code.co_name
        if self.response.empty:
            send_alert(
                subject="Social posts: Google ads",
                content=f"Dataframe empty after {f} in {self.__class__.__name__}",
            )

    def aggregate_by_keys(self, **kwargs) -> None:
        """
        Aggregate data by ad_id and device.
        Removes duplicates and sums metrics.
        """
        if not self.response.empty:
            logging.info(f"Columns before aggregation: {list(self.response.columns)}")

            required_cols = ["ad_id", "device", "cost_micros", "clicks"]
            missing_cols = [c for c in required_cols if c not in self.response.columns]

            if missing_cols:
                logging.warning(f"Missing columns: {missing_cols}")
                return

            try:
                if "cost_micros" in self.response.columns:
                    self.response["cost_micros"] = self.response["cost_micros"].astype(float)

                initial_rows = self.response.shape[0]
                self.response = self.response.drop_duplicates(
                    subset=["ad_id", "device"],
                    keep="first",
                )
                final_rows = self.response.shape[0]

                if initial_rows != final_rows:
                    logging.warning(f"Removed {initial_rows - final_rows} duplicates")

                agg_dict = {"cost_micros": "sum", "clicks": "sum"}

                if "customer_id_google" in self.response.columns:
                    agg_dict["customer_id_google"] = "first"

                self.response = (
                    self.response.groupby(["ad_id", "device"])
                    .agg(agg_dict)
                    .reset_index()
                )

                logging.info(f"Aggregated to {self.response.shape[0]} combinations")

            except Exception as e:
                logging.error(f"Aggregation error: {e}")

    def limit_placement(self, **kwargs) -> None:
        """Limit placement to top 25 by impressions per ad."""
        if not self.response.empty:
            self.response["impressions"] = self.response["impressions"].astype(int)
            self.response = (
                self.response.groupby(["id"])
                .apply(
                    lambda x: x.sort_values(by="impressions", ascending=False).head(25)
                )
                .reset_index(drop=True)
            )

    def replace_nat(self, **kwargs) -> None:
        """Replace NaT/NaN with None."""
        self.response.replace({np.nan: None}, inplace=True)

    def delete_nan_string(self, **kwargs) -> None:
        """Replace 'nan' string with None."""
        self.response.replace({"nan": None}, inplace=True)

    def remove_emoji(self, cols: List, **ignored) -> None:
        """Remove emoji characters from columns."""
        for c in cols:
            self.response[c] = self.response[c].apply(
                lambda x: deEmojify(x) if x is not None else x
            )
            self.response[c] = self.response[c].apply(
                lambda x: remove_emojis(x) if x is not None else x
            )

    def remove_non_latin(self, cols: List, **ignored) -> None:
        """Remove non-latin characters from columns."""
        for c in cols:
            self.response[c] = self.response[c].apply(
                lambda x: re.sub(r"[^\x00-\x7f]", "", x) if x is not None else x
            )

    def remove_piping(self, cols: List, **ignored) -> None:
        """Remove pipe characters from columns."""
        for c in cols:
            self.response[c] = self.response[c].apply(
                lambda x: re.sub(r"\|", " ", x) if x is not None else x
            )

    def clean_audience_string(self, **ignored) -> None:
        """Clean audience display names."""
        try:
            self.response["display_name"] = [
                re.sub(r'["*_+]', " ", str(d))
                for d in self.response["display_name"].tolist()
            ]
            self.response["display_name"] = [
                re.sub(r"\|", "", str(d))
                for d in self.response["display_name"].tolist()
            ]
        except Exception as e:
            logging.info(f"Error cleaning audience: {e}")
