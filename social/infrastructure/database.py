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
        mode: str = "upsert",
        dedupe_columns: Optional[List[str]] = None,
    ) -> int:
        """Load DataFrame into Vertica table.

        Args:
            df: DataFrame to load
            table_name: Target table name (without _TEST suffix)
            mode: Load mode:
                - 'append': INSERT only new rows (skip duplicates)
                - 'replace': TRUNCATE table then INSERT all rows
                - 'upsert': INSERT new rows + UPDATE existing rows (default)
            dedupe_columns: Columns to use as Primary Key (None = auto-detect)

        Returns:
            Number of rows loaded/updated

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

            # STEP 1: Remove duplicates WITHIN the DataFrame itself
            initial_rows = len(df)
            df = df.drop_duplicates(keep='first')
            duplicates_removed = initial_rows - len(df)
            if duplicates_removed > 0:
                logger.warning(
                    f"Removed {duplicates_removed} internal duplicates from DataFrame "
                    f"before loading to {final_table_name}"
                )

            # Handle different load modes
            if mode == "replace":
                # REPLACE: Truncate + Insert all
                self._truncate_table(cursor, final_table_name)
                rows_loaded = self._copy_to_db(cursor, final_table_name, df)
                logger.info(f"✓ Replaced {rows_loaded} rows in {final_table_name}")
                return rows_loaded

            elif mode == "append":
                # APPEND: Insert only new rows (skip existing)
                df = self._deduplicate(cursor, final_table_name, df, dedupe_columns)
                if df.empty:
                    logger.info(f"No new rows to append to {final_table_name}")
                    return 0
                rows_loaded = self._copy_to_db(cursor, final_table_name, df)
                logger.info(f"✓ Appended {rows_loaded} new rows to {final_table_name}")
                return rows_loaded

            elif mode == "upsert":
                # UPSERT: Insert new + Update existing
                rows_affected = self._upsert(cursor, final_table_name, df, dedupe_columns)
                logger.info(f"✓ Upserted {rows_affected} rows to {final_table_name}")
                return rows_affected

            else:
                raise ValueError(f"Invalid mode: {mode}. Must be 'append', 'replace', or 'upsert'")

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
            dedupe_columns: Columns to use for matching (None = auto-detect using PK)

        Returns:
            DataFrame with only new rows
        """
        if dedupe_columns is None:
            # Auto-detect PK columns using same logic as _detect_pk_columns
            dedupe_columns = self._detect_pk_columns(df)

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

            # CRITICAL: Align data types between new and existing data for merge keys
            # This prevents type mismatch errors (e.g., Int64 vs str, int64 vs str)
            for col in dedupe_columns:
                if col in df.columns and col in existing_data.columns:
                    # Get dtypes
                    new_dtype = df[col].dtype
                    existing_dtype = existing_data[col].dtype

                    # If types differ, convert existing_data to match new df
                    if new_dtype != existing_dtype:
                        try:
                            existing_data[col] = existing_data[col].astype(new_dtype)
                            logger.debug(f"Converted existing data column '{col}' from {existing_dtype} to {new_dtype} for merge")
                        except Exception as e:
                            logger.warning(f"Could not convert column '{col}' type for merge: {e}")
                            # Try converting new df to existing type instead
                            try:
                                df[col] = df[col].astype(existing_dtype)
                                logger.debug(f"Converted new data column '{col}' from {new_dtype} to {existing_dtype} for merge")
                            except Exception as e2:
                                logger.error(f"Type conversion failed for merge key '{col}': {e2}")

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

    def _upsert(
        self,
        cursor,
        table_name: str,
        df: pd.DataFrame,
        pk_columns: Optional[List[str]] = None
    ) -> int:
        """Perform UPSERT operation: INSERT new rows + UPDATE existing rows.

        This replicates the old MERGE strategy:
        1. Create temporary _source table
        2. INSERT all data into _source table
        3. MERGE _source into target table:
           - UPDATE existing rows (matched on PK)
           - INSERT new rows (not matched)
        4. Drop _source table

        Args:
            cursor: Database cursor
            table_name: Target table name
            df: DataFrame with data to upsert
            pk_columns: Primary key columns (None = auto-detect)

        Returns:
            Total rows affected (inserted + updated)
        """
        # Auto-detect PK columns if not provided
        if pk_columns is None:
            pk_columns = self._detect_pk_columns(df)

        if not pk_columns:
            logger.warning(f"No PK columns detected for {table_name}, falling back to append mode")
            df_new = self._deduplicate(cursor, table_name, df, None)
            return self._copy_to_db(cursor, table_name, df_new)

        logger.debug(f"UPSERT using PK columns: {pk_columns}")

        # Step 1: Create temporary source table
        source_table = f"{table_name}_source"
        try:
            # Create source table with same schema as target
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{source_table}
                LIKE {self.schema}.{table_name}
            """)
            cursor.execute("COMMIT")
            logger.debug(f"Created source table: {source_table}")

            # Add last_updated_date column if not exists (for tracking updates)
            try:
                cursor.execute(f"""
                    ALTER TABLE {self.schema}.{source_table}
                    ADD COLUMN IF NOT EXISTS last_updated_date TIMESTAMP
                """)
                cursor.execute(f"""
                    ALTER TABLE {self.schema}.{table_name}
                    ADD COLUMN IF NOT EXISTS last_updated_date TIMESTAMP
                """)
                cursor.execute("COMMIT")
            except Exception:
                pass  # Column might already exist

            # Step 2: Truncate source table and insert new data
            self._truncate_table(cursor, source_table)
            rows_in_source = self._copy_to_db(cursor, source_table, df)
            logger.debug(f"Loaded {rows_in_source} rows into {source_table}")

            # Step 3: Build MERGE query
            # Identify update columns (all columns except PK and metadata)
            all_columns = list(df.columns)
            update_columns = [
                col for col in all_columns
                if col not in pk_columns
                and col not in ["row_loaded_date", "last_updated_date"]
            ]

            if not update_columns:
                logger.warning(f"No columns to update for {table_name}, only PK columns found")
                # If no update columns, just insert new rows
                df_new = self._deduplicate(cursor, table_name, df, pk_columns)
                return self._copy_to_db(cursor, table_name, df_new)

            # Build ON clause: TGT.id = SRC.id AND TGT.date = SRC.date
            on_conditions = " AND ".join([f"TGT.{col} = SRC.{col}" for col in pk_columns])

            # Build SET clause: field1=SRC.field1, field2=SRC.field2, ...
            set_assignments = ", ".join([f"{col} = SRC.{col}" for col in update_columns])
            set_assignments += ", last_updated_date = CURRENT_TIMESTAMP"

            # Build MERGE query
            merge_query = f"""
                MERGE INTO {self.schema}.{table_name} TGT
                USING {self.schema}.{source_table} SRC
                ON {on_conditions}
                WHEN MATCHED THEN
                    UPDATE SET {set_assignments}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(all_columns)})
                    VALUES ({', '.join([f'SRC.{col}' for col in all_columns])})
            """

            # Execute MERGE
            logger.debug(f"Executing MERGE query for {table_name}")
            cursor.execute(merge_query)
            cursor.execute("COMMIT")

            # Get count of rows in source (this is our "rows affected")
            # Note: Vertica doesn't return affected rows from MERGE, so we approximate
            rows_affected = rows_in_source

            logger.info(
                f"MERGE completed: {source_table} → {table_name} "
                f"(PK: {pk_columns}, {rows_affected} rows processed)"
            )

            return rows_affected

        except Exception as e:
            logger.error(f"UPSERT failed for {table_name}: {e}")
            raise SocialDatabaseError(
                f"UPSERT operation failed for {table_name}",
                details={"error": str(e), "pk_columns": pk_columns}
            )

        finally:
            # Clean up: we don't drop the source table to allow inspection if needed
            pass

    def _detect_pk_columns(self, df: pd.DataFrame) -> List[str]:
        """Auto-detect Primary Key columns from DataFrame.

        Detection logic:
        - If 'id' column exists → use 'id' alone
        - If 'date' column exists:
            - With 'creative_id' → use (creative_id, date)
            - With 'ad_id' → use (ad_id, date)
            - With 'adgroup_id' → use (adgroup_id, date)
            - With 'campaign_id' → use (campaign_id, date)
        - If 'device' column exists with ad_id → use (ad_id, device)
        - Otherwise → use all non-metadata columns

        Args:
            df: DataFrame to analyze

        Returns:
            List of PK column names
        """
        pk_candidates = []

        # Rule 1: Simple 'id' primary key (campaigns, accounts, creatives)
        if "id" in df.columns:
            pk_candidates.append("id")
            logger.debug("Detected PK: 'id' (single column)")
            return pk_candidates

        # Rule 2: Time-series data with date column
        if "date" in df.columns:
            # Priority order for composite keys
            for id_col in ["creative_id", "ad_id", "adgroup_id", "campaign_id"]:
                if id_col in df.columns:
                    pk_candidates.append(id_col)
                    pk_candidates.append("date")
                    logger.debug(f"Detected PK: ({id_col}, date) - time-series data")
                    return pk_candidates

        # Rule 3: Device-level aggregation (Google Ads cost_by_device)
        if "ad_id" in df.columns and "device" in df.columns:
            pk_candidates.extend(["ad_id", "device"])
            logger.debug("Detected PK: (ad_id, device) - device aggregation")
            return pk_candidates

        # Rule 4: Composite keys without date
        if "campaign_id" in df.columns and "adgroup_id" in df.columns and "ad_id" in df.columns:
            pk_candidates.extend(["campaign_id", "adgroup_id", "ad_id"])
            logger.debug("Detected PK: (campaign_id, adgroup_id, ad_id) - composite key")
            return pk_candidates

        # Fallback: use all columns except metadata
        pk_candidates = [
            col for col in df.columns
            if col not in ["row_loaded_date", "last_updated_date"]
        ]
        logger.warning(f"No standard PK detected, using all {len(pk_candidates)} columns")

        return pk_candidates

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
