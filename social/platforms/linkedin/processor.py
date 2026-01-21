"""
LinkedIn Ads data processor.
Handles data transformation and cleaning for LinkedIn Ads data.
"""

import math
import re
from datetime import datetime
from typing import List, AnyStr

import pandas as pd
from loguru import logger

from social.platforms.linkedin import company_account


class LinkedinProcess:
    """
    Processor for LinkedIn Ads data.
    Transforms raw API responses into clean DataFrames.
    """

    def __init__(self, response: pd.DataFrame):
        """
        Initialize processor with response data.

        Args:
            response: DataFrame from LinkedIn API
        """
        self.response = response

    def get_df(self) -> pd.DataFrame:
        """Return the processed DataFrame."""
        return self.response

    def convert_nat_to_nan(self, columns: List) -> None:
        """Convert NaT to None in specified columns."""
        for c in columns:
            self.response[c] = self.response[c].replace({pd.NaT: None})

    def extract_id_from_urn(self, cols: List) -> None:
        """Extract numeric ID from URN strings."""
        for col in cols:
            self.response[col] = self.response[col].apply(
                lambda x: re.findall(r"\d+", x)[0]
            )

    def build_date_field(
        self,
        fields_date: List = ["year", "month", "day"],
        begin_end: List = ["start", "end"],
        exclude: bool = True,
    ) -> None:
        """
        Combine separate date fields into single date columns.

        Args:
            fields_date: Date field components
            begin_end: Start/end labels
            exclude: Whether to drop end date
        """
        for timerange in begin_end:
            cols = [f"dateRange_{timerange}_{i}" for i in fields_date]
            self.response["date_" + timerange] = self.response[cols].apply(
                lambda x: "-".join(x.astype(str)), axis=1
            )
            self.response["date_" + timerange] = self.response["date_" + timerange].apply(
                lambda x: datetime.strptime(x, "%Y-%m-%d")
            )
            self.response.drop(columns=cols, inplace=True)

        if exclude:
            self.response.drop(columns="date_end", inplace=True)
            self.response.rename(columns={"date_start": "date"}, inplace=True)

    def modify_name(self, cols: List, **ignored) -> None:
        """Remove pipe characters from columns."""
        for c in cols:
            self.response[c] = self.response[c].str.replace("|", "-")

    def rename_column(self, **kwargs) -> None:
        """Rename columns according to mapping."""
        d_ren = kwargs.get("renaming")
        self.response.rename(columns=d_ren, inplace=True)

    def add_company(self, **ignored) -> None:
        """Add company ID based on account mapping."""
        companies = []
        for idx, row in self.response.iterrows():
            companies.append(company_account.get(row["id"], 1))
        self.response["companyid"] = companies

    def add_row_loaded_date(self, **ignored) -> None:
        """Add row_loaded_date column."""
        self.response["row_loaded_date"] = datetime.now()

    def convert_string(self, columns: List) -> None:
        """Convert columns to string type."""
        for c in columns:
            self.response[c] = self.response[c].astype(str)

    def modify_urn_li_sponsoredAccount(self, **ignored) -> None:
        """Extract account ID from URN."""
        self.response.account = self.response.account.apply(
            lambda x: x.split("urn:li:sponsoredAccount:")[1]
        )

    def response_decoration(
        self, field: AnyStr, new_col_name: AnyStr, **kwargs
    ) -> None:
        """
        Extract ID from URN fields.

        Args:
            field: Source field name
            new_col_name: New column name
        """
        if new_col_name:
            try:
                self.response[new_col_name] = self.response[field].apply(
                    lambda x: re.search(r"\d+", x).group(0)
                    if not isinstance(x, float)
                    else x
                )
                self.response.drop(columns=[field], inplace=True)
            except KeyError:
                raise KeyError("new_col_name not specified correctly")
        else:
            self.response[field] = self.response[field].apply(
                lambda x: re.search(r"\d+", x).group(0)
                if not (isinstance(x, float) or x is None)
                else x
            )

    def convert_unix_timestamp_to_date(self, columns: List, **ignored) -> None:
        """Convert Unix timestamp (milliseconds) to datetime."""
        for c in columns:
            try:
                self.response[c] = self.response[c].apply(
                    lambda x: datetime.fromtimestamp(x / 1000)
                    if not math.isnan(x)
                    else x
                )
                self.response[c] = pd.to_datetime(self.response[c])
            except ValueError as e:
                logger.error(e)

    def replace_nan_with_zero(self, columns: List) -> None:
        """Replace NaN with 0 in specified columns."""
        existing_columns = [c for c in columns if c in self.response.columns]
        if existing_columns:
            self.response[existing_columns] = self.response[existing_columns].fillna(0)
