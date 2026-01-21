"""
Social data repository.
Handles all database operations for social ads data.
"""

import datetime
import re
from io import StringIO
from typing import List, Optional

import numpy as np
import pandas as pd
from jinjasql import JinjaSql
from loguru import logger
from vertica_python.errors import DatabaseError
from vertica_python.vertica.cursor import Cursor

from shared.connection.vertica import VerticaConnection
from social import SCHEMA
from social.repository.templatesql import (
    query_exclude_template,
    query_truncate,
    query_delete,
)


class SocialRepository:
    """
    Repository for social ads data operations.
    Handles reading and writing to Vertica database.
    """

    def __init__(self, connection: Optional[VerticaConnection] = None):
        """
        Initialize repository.

        Args:
            connection: Vertica connection (creates new if not provided)
        """
        self.connection = connection or VerticaConnection()

    def add_missing_columns(
        self, data: pd.DataFrame, col_ord: List[str]
    ) -> pd.DataFrame:
        """Add missing columns to DataFrame."""
        try:
            data = data[col_ord]
        except KeyError:
            col = list(set(col_ord) - set(data.columns))
            logger.info(f"Adding missing columns: {col}")
            for c in col:
                if c == "row_loaded_date":
                    data[c] = datetime.datetime.now()
                else:
                    data[c] = None
        finally:
            data = data[col_ord]
        return data

    def fix_data_type(
        self, df: pd.DataFrame, cur: Cursor, table_name: str
    ) -> pd.DataFrame:
        """Align DataFrame types with Vertica column types."""
        cur.execute(
            "SELECT column_name, data_type from v_catalog.columns WHERE table_name =:tab",
            {"tab": table_name},
        )

        column_data_type = pd.DataFrame(cur.fetchall())
        column_data_type.columns = ["COLUMN_NAME", "DATA_TYPE"]
        column_data_type["DATA_TYPE"] = column_data_type["DATA_TYPE"].apply(
            lambda x: re.sub(r"\([^()]*\)", "", x)
        )

        mapping = {
            "float": float,
            "int": int,
            "date": datetime.date,
            "varchar": str,
        }

        for idx, row in column_data_type.iterrows():
            typ = mapping.get(row["DATA_TYPE"])
            col = row["COLUMN_NAME"]

            if col not in df.columns:
                if row["DATA_TYPE"] in ["varchar", "date"]:
                    df[col] = ""
                else:
                    df[col] = None

            if row["DATA_TYPE"] == "float" and not all(pd.isna(df[col])):
                df[col] = df[col].astype(typ)
                df[col] = df[col].apply(lambda x: round(x, 2))

            if row["DATA_TYPE"] == "int" and not all(pd.isna(df[col])):
                df[col].fillna(0, inplace=True)
                df[col] = df[col].astype(typ)

            if row["DATA_TYPE"] == "date":
                df[col] = df[col].apply(
                    lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").date()
                    if isinstance(x, str)
                    else None
                )

        return df

    def column_order(self, cur: Cursor, table_name: str) -> List[str]:
        """Get column order from Vertica table."""
        cur.execute(
            "SELECT column_name from v_catalog.columns WHERE table_name =:tab",
            {"tab": table_name},
        )
        cols = cur.fetchall()
        return [item for sublist in cols for item in sublist]

    def truncate_table(self, cur: Cursor, table_name: str, schema: str) -> None:
        """Truncate a table."""
        j = JinjaSql()
        query, _ = j.prepare_query(
            query_truncate, {"table_name": table_name, "schema_name": schema}
        )
        cur.execute(query)
        cur.execute("COMMIT")
        logger.info(f"Truncated table: {table_name}")

    def delete(
        self, cur: Cursor, table_name: str, min_date: str, delete_col: str
    ) -> None:
        """Delete data from table based on date."""
        j = JinjaSql()
        query, _ = j.prepare_query(
            query_delete,
            {
                "table_name": table_name,
                "min_date": min_date,
                "delete_col": delete_col,
                "schema_name": SCHEMA,
            },
        )
        cur.execute(query)
        cur.execute("COMMIT")

    def get_excluded_data(
        self,
        table_name: str,
        schema: str,
        cur: Cursor,
        df: pd.DataFrame,
        params_query: dict,
    ) -> pd.DataFrame:
        """Get data already in table for deduplication."""
        params = {}
        if "data" in df.columns:
            params["min_data"] = min(df.data)
            params["max_data"] = max(df.data)

        if "date" in df.columns:
            params["min_date"] = min(df.date)
            params["max_date"] = max(df.date)

        params["schema_name"] = schema
        params["table_name"] = table_name
        cols = (
            params_query["column_name"]
            if len(params_query["column_name"]) > 1
            else params_query["column_name"][0]
        )
        params["column_name"] = ",".join(cols) if isinstance(cols, list) else cols

        if params_query.get("row_loaded_date", False):
            params["row_loaded_date"] = True

        j = JinjaSql()
        query, bind_params = j.prepare_query(query_exclude_template, params)

        try:
            cur.execute(query, bind_params)
            data_exclude = pd.DataFrame(
                cur.fetchall(), columns=[d.name for d in cur.description]
            )
        except DatabaseError as e:
            logger.exception(e)
            return pd.DataFrame()

        if not data_exclude.empty:
            try:
                data_exclude = data_exclude.replace("nan", np.nan, regex=True)
            except Exception:
                pass

        return data_exclude

    def write_to_db(
        self,
        table_name: str,
        schema: str,
        cur: Cursor,
        data: pd.DataFrame,
        params_query: dict,
    ) -> None:
        """
        Write data to database using COPY.

        Args:
            table_name: Target table name
            schema: Schema name
            cur: Database cursor
            data: DataFrame to write
            params_query: Query parameters for deduplication
        """
        col_ord = self.column_order(cur=cur, table_name=table_name)
        data = self.add_missing_columns(data=data, col_ord=col_ord)
        data = self.fix_data_type(cur=cur, table_name=table_name, df=data)

        excluded = self.get_excluded_data(
            table_name=table_name,
            cur=cur,
            schema=schema,
            df=data,
            params_query=params_query["query"],
        )

        if table_name in ["fb_ads_insight_actions", "fb_ads_insight_actions_source"]:
            excluded = excluded.astype(str)

        if not excluded.empty:
            data = data.merge(
                excluded.drop_duplicates(),
                on=list(excluded.columns),
                how="left",
                indicator="Exist",
            )
            data = data[data["Exist"] == "left_only"]
            data = data.drop(columns="Exist")

        if data.empty:
            logger.info(f"DataFrame empty, skipping write to {table_name}")
            return

        data.drop_duplicates(subset=list(excluded.columns), inplace=True)
        data.dropna(subset=list(excluded.columns), inplace=True)

        buff = StringIO()
        col_len = len(data.columns)
        col_name = ",".join(data.columns)
        buff_string = ("{}|" * col_len)[:-1] + "\n"
        # Removed NO ESCAPE to allow backslash escaping of pipe delimiters
        sql_query = f"COPY {schema}.{table_name} ({col_name}) FROM STDIN null 'None' ABORT ON ERROR"

        for idx, row in enumerate(data.values.tolist()):
            # Escape pipe and backslash characters in string values
            escaped_row = []
            for val in row:
                if isinstance(val, str):
                    # Escape backslash first, then pipe
                    escaped_val = val.replace("\\", "\\\\").replace("|", "\\|")
                    escaped_row.append(escaped_val)
                else:
                    escaped_row.append(val)
            buff.write(buff_string.format(*escaped_row))

        try:
            cur.copy(sql_query, buff.getvalue())
            cur.execute("COMMIT")
            logger.info(f"Inserted {data.shape[0]} rows into {table_name}")
        except Exception as e:
            logger.exception(f"Error inserting: {e}")

    def get_data(self, cur: Cursor, query: str) -> pd.DataFrame:
        """Execute query and return DataFrame."""
        cur.execute(query)
        data = cur.fetchall()
        cols = [d.name for d in cur.description]
        return pd.DataFrame(data, columns=cols)
