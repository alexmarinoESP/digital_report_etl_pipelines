"""
Database cursor wrapper.
Provides query execution with JinjaSQL templating support.
"""

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from jinjasql import JinjaSql
from loguru import logger


class Cur:
    """
    Cursor wrapper for database operations.
    Supports JinjaSQL templating for parameterized queries.
    """

    def __init__(self, connection: Any, conn_db: str):
        """
        Initialize cursor.

        Args:
            connection: Database connection object
            conn_db: Database type (vertica, oracle)
        """
        self.curs = connection.cursor()
        self.conn_db = conn_db

    def query_data(
        self,
        query: str,
        info: Optional[str] = None,
        data: Optional[Dict] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Execute query and return DataFrame.

        Args:
            query: SQL query (JinjaSQL template)
            info: Info message for logging
            data: Parameters for JinjaSQL template
            **kwargs: Additional keyword arguments

        Returns:
            DataFrame with query results
        """
        j = JinjaSql()

        # Run query
        if data is not None:
            query, bind_params = j.prepare_query(query, data)
            if self.conn_db == "oracle":
                print(query)
                self.curs.execute(query)
            elif self.conn_db == "vertica":
                self.curs.execute(query, bind_params)
        else:
            self.curs.execute(query)

        # Fetch and process results
        data_fetch = self.curs.fetchall()

        # Encode to UTF-8
        for result in data_fetch:
            for pos in np.arange(len(result)):
                try:
                    result[pos] = str(result[pos]).encode("utf-8")
                except Exception:
                    logger.info(f"Not decoded utf-8 for {result[pos]}")

        # Create DataFrame
        cols = [d[0] for d in self.curs.description]
        df = pd.DataFrame(data_fetch, columns=cols)

        logger.info(f"Extracted {df.shape[0]} rows. Information: {info}")

        return df

    def execute(self, query: str, params: Optional[Dict] = None) -> None:
        """
        Execute a query without returning results.

        Args:
            query: SQL query
            params: Query parameters
        """
        if params:
            self.curs.execute(query, params)
        else:
            self.curs.execute(query)

    def close(self) -> None:
        """Close the cursor."""
        self.curs.close()
