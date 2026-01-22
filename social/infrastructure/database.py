"""Database infrastructure for Vertica data sink.

This module implements the DataSink protocol for Vertica database,
handling all database operations with proper error handling and
data type management.
"""

import re
import datetime
from io import StringIO
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger
from vertica_python.errors import DatabaseError

from shared.connection.vertica import VerticaConnection
from social.core.protocols import DataSink
from social.core.config import DatabaseConfig
from social.core.exceptions import DatabaseError as SocialDatabaseError
from social.core.constants import DATABASE_SCHEMA, DATABASE_TEST_SUFFIX, PIPE_DELIMITER, ESCAPE_CHARS


class VerticaDataSink:
    """Vertica database implementation of DataSink protocol.

    This class handles all database operations for storing social media
    advertising data in Vertica, including:
    - Data type alignment with database schema
    - Deduplication using left anti-join pattern
    - Efficient COPY operations with proper escaping
    - Connection management
    """

    def __init__(self, config: DatabaseConfig, test_mode: bool = False):
        """Initialize Vertica data sink.

        Args:
            config: Database configuration
            test_mode: If True, append '_TEST' suffix to table names
        """
        self.config = config
        self.test_mode = test_mode
        self.schema = config.schema
        self._connection = VerticaConnection(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
        )
        self._conn = None
        self._cursor = None

    def _get_connection(self):
        """Get or create database connection.

        Returns:
            Vertica connection object
        """
        if self._conn is None:
            self._conn = self._connection.connect()
        return self._conn

    def _get_cursor(self):
        """Get or create database cursor.

        Returns:
            Vertica cursor object
        """
        if self._cursor is None:
            conn = self._get_connection()
            self._cursor = conn.cursor()
        return self._cursor

    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = "append",
        dedupe_columns: Optional[List[str]] = None,
    ) -> int:
        """Load DataFrame into Vertica table.

        Args:
            df: DataFrame to load
            table_name: Target table name (without _TEST suffix)
            mode: Load mode - 'append', 'replace', 'upsert'
            dedupe_columns: Columns to use for deduplication (None = auto-detect)

        Returns:
            Number of rows loaded

        Raises:
            DatabaseError: If load operation fails
        """
        if df.empty:
            logger.info(f"DataFrame empty, skipping load to {table_name}")
            return 0

        # Add _TEST suffix in test mode
        final_table_name = self._get_table_name(table_name)

        try:
            cursor = self._get_cursor()

            # Get column order and types from database
            col_order = self._get_column_order(cursor, final_table_name)
            df = self._add_missing_columns(df, col_order)
            df = self._align_data_types(cursor, final_table_name, df)

            # Handle different load modes
            if mode == "replace":
                self._truncate_table(cursor, final_table_name)
            elif mode == "append" or mode == "upsert":
                # Deduplicate against existing data
                df = self._deduplicate(cursor, final_table_name, df, dedupe_columns)

            if df.empty:
                logger.info(f"No new rows to load after deduplication for {final_table_name}")
                return 0

            # Write to database using COPY
            rows_loaded = self._copy_to_db(cursor, final_table_name, df)

            logger.info(f"✓ Loaded {rows_loaded} rows to {final_table_name}")
            return rows_loaded

        except Exception as e:
            logger.error(f"Failed to load data to {final_table_name}: {e}")
            raise SocialDatabaseError(
                f"Failed to load data to {final_table_name}",
                details={"error": str(e), "table": final_table_name}
            )

    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame.

        Args:
            sql: SQL query string

        Returns:
            DataFrame with query results

        Raises:
            DatabaseError: If query fails
        """
        try:
            cursor = self._get_cursor()
            cursor.execute(sql)
            data = cursor.fetchall()
            columns = [desc.name for desc in cursor.description]
            return pd.DataFrame(data, columns=columns)

        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise SocialDatabaseError(
                "Query execution failed",
                query=sql[:500],  # Truncate long queries
                details={"error": str(e)}
            )

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database.

        Args:
            table_name: Table name to check

        Returns:
            True if table exists, False otherwise
        """
        final_table_name = self._get_table_name(table_name)

        try:
            cursor = self._get_cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM v_catalog.tables WHERE table_schema = %s AND table_name = %s",
                (self.schema, final_table_name)
            )
            count = cursor.fetchone()[0]
            return count > 0

        except Exception as e:
            logger.warning(f"Error checking table existence: {e}")
            return False

    def close(self) -> None:
        """Close database connections."""
        if self._cursor:
            try:
                self._cursor.close()
            except Exception as e:
                logger.warning(f"Error closing cursor: {e}")
            finally:
                self._cursor = None

        if self._conn:
            try:
                self._conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._conn = None

    def _get_table_name(self, table_name: str) -> str:
        """Get final table name with optional _TEST suffix.

        Args:
            table_name: Base table name

        Returns:
            Table name with _TEST suffix in test mode
        """
        if self.test_mode and not table_name.endswith(DATABASE_TEST_SUFFIX):
            return f"{table_name}{DATABASE_TEST_SUFFIX}"
        return table_name

    def _get_column_order(self, cursor, table_name: str) -> List[str]:
        """Get ordered list of column names from table schema.

        Args:
            cursor: Database cursor
            table_name: Table name

        Returns:
            List of column names in table order
        """
        cursor.execute(
            "SELECT column_name FROM v_catalog.columns WHERE table_name = %s ORDER BY ordinal_position",
            (table_name,)
        )
        return [row[0] for row in cursor.fetchall()]

    def _add_missing_columns(self, df: pd.DataFrame, col_order: List[str]) -> pd.DataFrame:
        """Add missing columns to DataFrame with default values.

        Args:
            df: Source DataFrame
            col_order: Required columns in order

        Returns:
            DataFrame with all required columns
        """
        missing_cols = set(col_order) - set(df.columns)

        if missing_cols:
            logger.debug(f"Adding missing columns: {missing_cols}")
            for col in missing_cols:
                if col == "row_loaded_date":
                    df[col] = datetime.datetime.now()
                else:
                    df[col] = None

        # Reorder columns to match database
        return df[col_order]

    def _align_data_types(self, cursor, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Align DataFrame column types with database schema.

        Args:
            cursor: Database cursor
            table_name: Table name
            df: DataFrame to align

        Returns:
            DataFrame with aligned types
        """
        # Get column data types from Vertica
        cursor.execute(
            "SELECT column_name, data_type FROM v_catalog.columns WHERE table_name = %s",
            (table_name,)
        )

        column_types = pd.DataFrame(cursor.fetchall(), columns=["column_name", "data_type"])

        # Remove precision/scale from type (e.g., "numeric(18,2)" -> "numeric")
        column_types["data_type"] = column_types["data_type"].apply(
            lambda x: re.sub(r"\([^()]*\)", "", x)
        )

        # Type mapping
        type_mapping = {
            "float": float,
            "numeric": float,
            "int": int,
            "integer": int,
            "date": datetime.date,
            "timestamp": datetime.datetime,
            "varchar": str,
            "char": str,
        }

        for _, row in column_types.iterrows():
            col_name = row["column_name"]
            db_type = row["data_type"].lower()

            if col_name not in df.columns:
                continue

            python_type = type_mapping.get(db_type)
            if not python_type:
                continue

            # Handle float conversion
            if db_type in ["float", "numeric"] and not df[col_name].isna().all():
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
                df[col_name] = df[col_name].round(2)

            # Handle int conversion
            elif db_type in ["int", "integer"] and not df[col_name].isna().all():
                df[col_name] = df[col_name].fillna(0)
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype(int)

            # Handle date conversion
            elif db_type == "date":
                df[col_name] = pd.to_datetime(df[col_name], errors="coerce").dt.date

            # Handle timestamp conversion
            elif db_type == "timestamp":
                df[col_name] = pd.to_datetime(df[col_name], errors="coerce")

        return df

    def _deduplicate(
        self,
        cursor,
        table_name: str,
        df: pd.DataFrame,
        dedupe_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Remove rows that already exist in the database.

        Uses left anti-join pattern: keeps only rows from df that are NOT in the database.

        Args:
            cursor: Database cursor
            table_name: Table name
            df: DataFrame to deduplicate
            dedupe_columns: Columns to use for matching (None = auto-detect)

        Returns:
            DataFrame with only new rows
        """
        if dedupe_columns is None:
            # Auto-detect dedupe columns (exclude row_loaded_date)
            dedupe_columns = [col for col in df.columns if col != "row_loaded_date"]

        if not dedupe_columns:
            logger.warning("No deduplication columns found, skipping deduplication")
            return df

        # Build query to get existing data
        columns_str = ", ".join(dedupe_columns)
        where_clauses = []

        # Add date range filters if date columns exist
        if "date" in df.columns:
            min_date = df["date"].min()
            max_date = df["date"].max()
            where_clauses.append(f"date BETWEEN '{min_date}' AND '{max_date}'")
        elif "data" in df.columns:  # Some tables use 'data' instead of 'date'
            min_date = df["data"].min()
            max_date = df["data"].max()
            where_clauses.append(f"data BETWEEN '{min_date}' AND '{max_date}'")

        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        query = f"""
            SELECT {columns_str}
            FROM {self.schema}.{table_name}
            {where_clause}
        """

        try:
            cursor.execute(query)
            existing_data = pd.DataFrame(
                cursor.fetchall(),
                columns=dedupe_columns
            )

            if existing_data.empty:
                logger.debug(f"No existing data found for deduplication")
                return df

            # Convert 'nan' strings to actual NaN
            existing_data = existing_data.replace("nan", np.nan, regex=True)

            # Left anti-join: keep only rows from df NOT in existing_data
            merged = df.merge(
                existing_data,
                on=dedupe_columns,
                how="left",
                indicator="_merge_indicator"
            )

            new_rows = merged[merged["_merge_indicator"] == "left_only"]
            new_rows = new_rows.drop(columns=["_merge_indicator"])

            logger.debug(
                f"Deduplication: {len(df)} rows -> {len(new_rows)} new rows "
                f"({len(df) - len(new_rows)} duplicates removed)"
            )

            return new_rows

        except DatabaseError as e:
            logger.warning(f"Deduplication query failed, proceeding without dedup: {e}")
            return df

    def _copy_to_db(self, cursor, table_name: str, df: pd.DataFrame) -> int:
        """Write DataFrame to database using COPY command.

        Args:
            cursor: Database cursor
            table_name: Target table
            df: DataFrame to write

        Returns:
            Number of rows written

        Raises:
            DatabaseError: If COPY fails
        """
        # Drop duplicates within the DataFrame itself
        df = df.drop_duplicates()

        # Replace all NaT values with None before converting to list
        # This is necessary because .values.tolist() converts NaT to string "NaT"
        df = df.replace({pd.NaT: None})
        df = df.where(pd.notna(df), None)

        # Build COPY statement
        columns_str = ",".join(df.columns)
        sql_query = f"COPY {self.schema}.{table_name} ({columns_str}) FROM STDIN null 'None' ABORT ON ERROR"

        # Build data buffer with proper escaping
        buff = StringIO()
        col_count = len(df.columns)
        row_format = (PIPE_DELIMITER.join(["{}"] * col_count)) + "\n"

        for row_values in df.values.tolist():
            escaped_values = []
            for val in row_values:
                if isinstance(val, str):
                    # Escape special characters (backslash first, then pipe)
                    escaped_val = val
                    for char, replacement in ESCAPE_CHARS.items():
                        escaped_val = escaped_val.replace(char, replacement)
                    escaped_values.append(escaped_val)
                else:
                    escaped_values.append(val)

            buff.write(row_format.format(*escaped_values))

        # Execute COPY
        try:
            cursor.copy(sql_query, buff.getvalue())
            cursor.execute("COMMIT")
            return len(df)

        except Exception as e:
            logger.error(f"COPY command failed: {e}")
            raise SocialDatabaseError(
                "COPY command failed",
                query=sql_query[:500],
                details={"error": str(e), "rows": len(df)}
            )

    def _truncate_table(self, cursor, table_name: str) -> None:
        """Truncate a table.

        Args:
            cursor: Database cursor
            table_name: Table to truncate
        """
        query = f"TRUNCATE TABLE {self.schema}.{table_name}"
        cursor.execute(query)
        cursor.execute("COMMIT")
        logger.info(f"Truncated table: {table_name}")
