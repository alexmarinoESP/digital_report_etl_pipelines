import math
import re
from datetime import datetime
from typing import List, AnyStr

import pandas as pd
from loguru import logger

from social.platforms.linkedin import company_account


class LinkedinProcess:
    def __init__(self, response: pd.DataFrame):
        self.response = response

    def get_df(self):
        return self.response

    def convert_nat_to_nan(self, columns: List):
        for c in columns:
            self.response[c] = self.response[c].replace({pd.NaT: None})

    def extract_id_from_urn(self, cols: List):
        """
        Extract ids from urn

        Args:
            col:

        Returns:

        """

        for col in cols:
            ids = self.response[col].apply(lambda x: re.findall(r"\d+", x)[0])
            self.response[col] = ids

    def build_date_field(
        self,
        fields_date: List = ["year", "month", "day"],
        begin_end: List = ["start", "end"],
        exclude: bool = True,
    ):
        """
        Response from api gives date in separate field ( day, month, year)
        Combine those field into one column

        Args:
            fields_date: list with name of the field
            begin_end: start,end
            df:
            exclude: drop old columns

        Returns:

        """

        for timerange in begin_end:
            cols = ["dateRange_{}_{}".format(
                timerange, i) for i in fields_date]
            self.response["date_" + timerange] = self.response[cols].apply(
                lambda x: "-".join(x.astype(str)), axis=1
            )
            self.response["date_" + timerange] = pd.to_datetime(
                self.response["date_" + timerange], format="%Y-%m-%d"
            )
            self.response.drop(columns=cols, inplace=True)

        if exclude:
            self.response.drop(columns="date_end", inplace=True)
            self.response.rename(columns={"date_start": "date"}, inplace=True)

    def modify_name(self, cols: List, **ignored):
        for c in cols:
            self.response[c] = self.response[c].str.replace("|", "-")

    def rename_column(self, **kwargs):
        d_ren = kwargs.get("renaming")
        self.response.rename(columns=d_ren, inplace=True)

    def add_company(self, **ignored):
        companies = []
        for idx, row in self.response.iterrows():
            companies.append(company_account.get(row["id"], 1))

        self.response["companyid"] = companies

    def add_row_loaded_date(self, **ignored):
        self.response["row_loaded_date"] = datetime.now()

    def convert_string(self, columns):
        for c in columns:
            self.response[c] = self.response[c].astype(str)

    def modify_urn_li_sponsoredAccount(self, **ignored):
        self.response["account"] = self.response["account"].apply(
            lambda x: str(x.split("urn:li:sponsoredAccount:")[1])
        )

    def response_decoration(self, field: AnyStr, new_col_name: AnyStr, **kwargs):
        """
        Extract id from urns
        Args:
            df:
            field:
            **kwargs:

        Returns:

        """

        if new_col_name:
            try:
                self.response[new_col_name] = self.response[field].apply(
                    lambda x: re.search("\d+", x).group(0)
                    if not isinstance(x, float)
                    else x
                )
                self.response.drop(columns=[field], inplace=True)
            except KeyError:
                raise (
                    "KeyError, Probably new_col_name isn't specified"
                    "correctly. It should have keys equal to fields"
                    "and value new column name you want to assign"
                )

        else:
            self.response[field] = self.response[field].apply(
                lambda x: re.search("\d+", x).group(0)
                if not (isinstance(x, float) or x is None)
                else x
            )

    def convert_unix_timestamp_to_date(self, columns: List, **ignored):
        """
        Date are in unix format ( integer). Convert to date
        Args:
            df:

        Returns:

        """
        for c in columns:
            try:
                self.response[c] = pd.to_datetime(
                    self.response[c], unit='ms', errors='coerce'
                )
                # df[c].replace({np.nan: None}, inplace=True)
                # NaT type not recognized in Vertica

            except ValueError as e:
                logger.error(e)

    def replace_nan_with_zero(self, columns: List):
        """Replaces NaN values with 0 only for specific LinkedIn Ads columns."""
        existing_columns = [c for c in columns if c in self.response.columns]

        if existing_columns:
            self.response[existing_columns] = self.response[existing_columns].fillna(0)
