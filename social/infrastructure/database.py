"""Database infrastructure for Vertica data sink.

This module implements the DataSink protocol for Vertica database,
handling all database operations with proper error handling and
data type management.
"""

import re
import datetime
from io import StringIO
from typing import Optional, List, Dict, Any, NamedTuple
import pandas as pd
import numpy as np
from loguru import logger
from vertica_python.errors import DatabaseError

from shared.connection.vertica import VerticaConnection
from social.core.protocols import DataSink
from social.core.config import DatabaseConfig
from social.core.exceptions import DatabaseError as SocialDatabaseError
from social.core.constants import DATABASE_SCHEMA, DATABASE_TEST_SUFFIX, PIPE_DELIMITER, ESCAPE_CHARS


class LoadStats(NamedTuple):
    """Statistics from a database load operation.

    Attributes:
        rows_from_api: Total rows received from API (before any processing)
        rows_inserted: New rows written to database
        rows_updated: Existing rows updated in database
        rows_skipped: Duplicate rows skipped (already in DB)
        rows_filtered: Rows filtered out (e.g., NULL in PK, internal duplicates)
    """
    rows_from_api: int
    rows_inserted: int
    rows_updated: int
    rows_skipped: int
    rows_filtered: int

    @property
    def rows_written(self) -> int:
        """Total rows written to database (inserted + updated)."""
        return self.rows_inserted + self.rows_updated

    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary for serialization."""
        return {
            "rows_from_api": self.rows_from_api,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_skipped": self.rows_skipped,
            "rows_filtered": self.rows_filtered,
            "rows_written": self.rows_written,
        }


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
        increment_columns: Optional[List[str]] = None,
    ) -> LoadStats:
        """Load DataFrame into Vertica table.

        Args:
            df: DataFrame to load
            table_name: Target table name (without _TEST suffix)
            mode: Load mode:
                - 'append': INSERT only new rows (skip duplicates)
                - 'replace': TRUNCATE table then INSERT all rows
                - 'upsert': INSERT new rows + UPDATE existing rows (default)
                - 'increment': INSERT new rows + INCREMENT metrics for existing rows
            dedupe_columns: Columns to use as Primary Key (None = auto-detect)
            increment_columns: Columns to increment (only for mode='increment')

        Returns:
            LoadStats with detailed statistics about rows written to database

        Raises:
            DatabaseError: If load operation fails
        """
        if df.empty:
            logger.info(f"DataFrame empty, skipping load to {table_name}")
            return LoadStats(
                rows_from_api=0,
                rows_inserted=0,
                rows_updated=0,
                rows_skipped=0,
                rows_filtered=0,
            )

        # Add _TEST suffix in test mode
        final_table_name = self._get_table_name(table_name)

        try:
            cursor = self._get_cursor()

            # DEBUG: Log DataFrame BEFORE any processing for placement table
            if 'placement' in table_name.lower():
                logger.warning(f"DEBUG BEFORE processing - DataFrame columns: {df.columns.tolist()}")
                if 'id' in df.columns:
                    id_sample = df['id'].head(5).tolist()
                    null_count = df['id'].isna().sum()
                    logger.warning(f"DEBUG BEFORE - 'id' sample: {id_sample}, nulls: {null_count}/{len(df)}")
                else:
                    logger.error("DEBUG BEFORE - 'id' column MISSING!")

            # Get column order and types from database
            col_order = self._get_column_order(cursor, final_table_name)
            logger.debug(f"Database column order: {col_order}")
            logger.debug(f"'load_date' in DB columns: {'load_date' in col_order}")

            df = self._add_missing_columns(df, col_order)
            logger.debug(f"DataFrame columns AFTER alignment: {list(df.columns)}")

            df = self._align_data_types(cursor, final_table_name, df)

            # DEBUG: Log DataFrame AFTER processing for placement table
            if 'placement' in table_name.lower():
                logger.warning(f"DEBUG AFTER processing - DataFrame columns: {df.columns.tolist()}")
                if 'id' in df.columns:
                    id_sample = df['id'].head(5).tolist()
                    null_count = df['id'].isna().sum()
                    logger.warning(f"DEBUG AFTER - 'id' sample: {id_sample}, nulls: {null_count}/{len(df)}")
                    logger.warning(f"DEBUG AFTER - DataFrame sample:\n{df[['id', 'placement']].head(3).to_string()}")
                else:
                    logger.error("DEBUG AFTER - 'id' column MISSING!")

            # Track statistics
            rows_from_api = len(df)

            # STEP 1: Auto-detect PK columns if not provided
            if dedupe_columns is None:
                dedupe_columns = self._detect_pk_columns(df)
                if dedupe_columns:
                    logger.error(f"🔴 AUTO-DETECTED PK columns for {final_table_name}: {dedupe_columns}")
                    logger.error(f"   DataFrame columns present: {list(df.columns)}")
            else:
                logger.error(f"🟢 EXPLICIT dedupe_columns provided for {final_table_name}: {dedupe_columns}")

            # STEP 2: Remove duplicates WITHIN the DataFrame itself
            initial_rows = len(df)
            logger.error(f"🔍 BEFORE drop_duplicates: {initial_rows} rows, dedupe_columns={dedupe_columns}")

            # Always deduplicate on PK columns if available (to avoid false duplicates from timestamp columns)
            if dedupe_columns:
                keep_strategy = 'last' if mode in ["upsert", "increment"] else 'first'
                df = df.drop_duplicates(subset=dedupe_columns, keep=keep_strategy)
                duplicates_removed = initial_rows - len(df)

                logger.error(f"🔍 AFTER drop_duplicates: {len(df)} rows (removed {duplicates_removed})")

                if duplicates_removed > 0:
                    logger.warning(
                        f"Removed {duplicates_removed} duplicate rows (by PK {dedupe_columns}) "
                        f"from DataFrame before loading to {final_table_name}, kept {keep_strategy} occurrence"
                    )
            else:
                # Fallback: if no PK detected, use all columns
                logger.warning(f"No PK columns detected for {final_table_name}, deduplicating on all columns")
                df = df.drop_duplicates(keep='first')
                duplicates_removed = initial_rows - len(df)
                if duplicates_removed > 0:
                    logger.warning(
                        f"Removed {duplicates_removed} internal duplicates from DataFrame "
                        f"before loading to {final_table_name}"
                    )

            rows_filtered = duplicates_removed

            # Handle different load modes
            if mode == "replace":
                # REPLACE: Truncate + Insert all
                self._truncate_table(cursor, final_table_name)
                rows_inserted = self._copy_to_db(cursor, final_table_name, df, pk_columns=dedupe_columns)
                logger.info(f"✓ Replaced {rows_inserted} rows in {final_table_name}")
                return LoadStats(
                    rows_from_api=rows_from_api,
                    rows_inserted=rows_inserted,
                    rows_updated=0,
                    rows_skipped=0,
                    rows_filtered=rows_filtered,
                )

            elif mode == "append":
                # APPEND: Insert only new rows (skip existing)
                rows_before_dedupe = len(df)
                df = self._deduplicate(cursor, final_table_name, df, dedupe_columns)
                rows_skipped = rows_before_dedupe - len(df)

                if df.empty:
                    logger.info(f"No new rows to append to {final_table_name}")
                    return LoadStats(
                        rows_from_api=rows_from_api,
                        rows_inserted=0,
                        rows_updated=0,
                        rows_skipped=rows_skipped,
                        rows_filtered=rows_filtered,
                    )

                rows_inserted = self._copy_to_db(cursor, final_table_name, df)
                logger.info(f"✓ Appended {rows_inserted} new rows to {final_table_name}")
                return LoadStats(
                    rows_from_api=rows_from_api,
                    rows_inserted=rows_inserted,
                    rows_updated=0,
                    rows_skipped=rows_skipped,
                    rows_filtered=rows_filtered,
                )

            elif mode == "upsert":
                # UPSERT: Insert new + Update existing
                stats = self._upsert(cursor, final_table_name, df, dedupe_columns)
                logger.info(
                    f"✓ Upserted {stats['rows_inserted']} new + {stats['rows_updated']} updated "
                    f"rows to {final_table_name}"
                )
                return LoadStats(
                    rows_from_api=rows_from_api,
                    rows_inserted=stats['rows_inserted'],
                    rows_updated=stats['rows_updated'],
                    rows_skipped=0,
                    rows_filtered=rows_filtered,
                )

            elif mode == "increment":
                # INCREMENT: Insert new + Increment metrics for existing
                if increment_columns is None:
                    raise ValueError("increment_columns required for mode='increment'")
                stats = self._increment(cursor, final_table_name, df, dedupe_columns, increment_columns)
                logger.info(
                    f"✓ Incremented {stats['rows_inserted']} new + {stats['rows_updated']} updated "
                    f"rows in {final_table_name}"
                )
                return LoadStats(
                    rows_from_api=rows_from_api,
                    rows_inserted=stats['rows_inserted'],
                    rows_updated=stats['rows_updated'],
                    rows_skipped=0,
                    rows_filtered=rows_filtered,
                )

            else:
                raise ValueError(f"Invalid mode: {mode}. Must be 'append', 'replace', 'upsert', or 'increment'")

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
            "SELECT column_name FROM v_catalog.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (self.schema, table_name)
        )
        columns = [row[0] for row in cursor.fetchall()]
        logger.debug(f"Column order for {self.schema}.{table_name}: {columns}")
        return columns

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
                if col == "load_date":
                    df[col] = datetime.date.today()
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
        # Get column data types from Vertica (including precision and scale)
        cursor.execute(
            "SELECT column_name, data_type, numeric_scale FROM v_catalog.columns WHERE table_schema = %s AND table_name = %s",
            (self.schema, table_name)
        )

        column_types = pd.DataFrame(cursor.fetchall(), columns=["column_name", "data_type", "numeric_scale"])

        # Remove precision/scale from type (e.g., "numeric(18,2)" -> "numeric")
        column_types["data_type_clean"] = column_types["data_type"].apply(
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
            db_type = row["data_type_clean"].lower()
            numeric_scale = row["numeric_scale"]

            if col_name not in df.columns:
                continue

            python_type = type_mapping.get(db_type)
            if not python_type:
                continue

            # Handle float conversion
            if db_type in ["float", "numeric"] and not df[col_name].isna().all():
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
                # DON'T ROUND - let Vertica handle precision based on column definition
                # The old hardcoded round(2) was destroying precision for columns like CTR
                # that need 4+ decimals

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
    ) -> Dict[str, int]:
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
            Dict with 'rows_inserted' and 'rows_updated' keys
        """
        # Auto-detect PK columns if not provided
        if pk_columns is None:
            pk_columns = self._detect_pk_columns(df)

        if not pk_columns:
            logger.warning(f"No PK columns detected for {table_name}, falling back to append mode")
            df_new = self._deduplicate(cursor, table_name, df, None)
            rows_inserted = self._copy_to_db(cursor, table_name, df_new)
            return {"rows_inserted": rows_inserted, "rows_updated": 0}

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

            # Note: last_updated_date column removed - using load_date instead

            # Step 2: Truncate source table and insert new data
            self._truncate_table(cursor, source_table)
            rows_in_source = self._copy_to_db(cursor, source_table, df)
            logger.debug(f"Loaded {rows_in_source} rows into {source_table}")

            # Step 3: Build MERGE query
            # Identify update columns (all columns except PK)
            # Note: load_date IS updated to track when each row was last refreshed
            all_columns = list(df.columns)
            update_columns = [
                col for col in all_columns
                if col not in pk_columns
                and col not in ["last_updated_date"]  # Only exclude deprecated column
            ]

            if not update_columns:
                logger.warning(f"No columns to update for {table_name}, only PK columns found")
                # If no update columns, just insert new rows
                df_new = self._deduplicate(cursor, table_name, df, pk_columns)
                rows_inserted = self._copy_to_db(cursor, table_name, df_new)
                return {"rows_inserted": rows_inserted, "rows_updated": 0}

            # Query existing keys to calculate inserted vs updated
            existing_keys = self._query_existing_keys(cursor, table_name, df, pk_columns)

            # Count new vs existing rows
            df_copy = df.copy()
            df_copy['_merge_key'] = df_copy[pk_columns].apply(lambda row: tuple(row), axis=1)
            df_copy['_is_new'] = ~df_copy['_merge_key'].isin(existing_keys)

            rows_to_insert = df_copy['_is_new'].sum()
            rows_to_update = (~df_copy['_is_new']).sum()

            # Build ON clause: TGT.id = SRC.id AND TGT.date = SRC.date
            on_conditions = " AND ".join([f"TGT.{col} = SRC.{col}" for col in pk_columns])

            # Build SET clause: field1=SRC.field1, field2=SRC.field2, load_date=SRC.load_date, ...
            set_assignments = ", ".join([f"{col} = SRC.{col}" for col in update_columns])

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

            logger.info(
                f"MERGE completed: {source_table} → {table_name} "
                f"(PK: {pk_columns}, {rows_to_insert} new + {rows_to_update} updated)"
            )

            return {"rows_inserted": rows_to_insert, "rows_updated": rows_to_update}

        except Exception as e:
            logger.error(f"UPSERT failed for {table_name}: {e}")
            raise SocialDatabaseError(
                f"UPSERT operation failed for {table_name}",
                details={"error": str(e), "pk_columns": pk_columns}
            )

        finally:
            # Clean up: we don't drop the source table to allow inspection if needed
            pass

    def _detect_pk_columns(self, df: pd.DataFrame, exclude_date: bool = False) -> List[str]:
        """Auto-detect Primary Key columns from DataFrame.

        Detection logic (case-insensitive):
        - If 'id' column exists → use 'id' alone
        - If 'date' column exists (and not excluded):
            - With 'creative_id' → use (creative_id, date)
            - With 'ad_id' → use (ad_id, date)
            - With 'adgroup_id' → use (adgroup_id, date)
            - With 'campaign_id' → use (campaign_id, date)
        - If exclude_date=True (for increment mode):
            - With 'creative_id' → use (creative_id) only
            - With 'ad_id' → use (ad_id) only
            - etc.
        - If 'device' column exists with ad_id → use (ad_id, device)
        - Otherwise → use all non-metadata columns

        Args:
            df: DataFrame to analyze
            exclude_date: If True, exclude 'date' from PK (for increment mode)

        Returns:
            List of PK column names (with original casing from DataFrame)
        """
        pk_candidates = []

        # Create case-insensitive lookup: lowercase -> original column name
        cols_lower = {col.lower(): col for col in df.columns}

        # Rule 1: Simple 'id' primary key (campaigns, accounts, creatives)
        if "id" in cols_lower:
            pk_candidates.append(cols_lower["id"])
            logger.debug("Detected PK: 'id' (single column)")
            return pk_candidates

        # IMPORTANT: Check Microsoft-specific rules BEFORE generic time-series rules
        # to avoid false matches on adgroup_id + time_period

        # Pre-compute date_col and campaign_col for reuse
        date_col = cols_lower.get("date") or cols_lower.get("time_period") or cols_lower.get("timeperiod")
        campaign_col = cols_lower.get("campaign_id") or cols_lower.get("campaignid")

        # Rule 1a: Microsoft Ads Publisher Usage (placement) - CHECK FIRST
        publisher_col = cols_lower.get("publisher_url") or cols_lower.get("publisherurl")
        if campaign_col and publisher_col:
            pk_candidates.extend([campaign_col, publisher_col])
            # Add time_period if present (for daily aggregation)
            if date_col:
                pk_candidates.append(date_col)
                logger.debug(f"Detected PK: ({campaign_col}, {publisher_col}, {date_col}) - Microsoft Ads placement (time-series)")
            else:
                logger.debug(f"Detected PK: ({campaign_col}, {publisher_col}) - Microsoft Ads placement (summary)")
            return pk_candidates

        # Rule 1b: Microsoft Ads Geographic - CHECK SECOND
        country_col = cols_lower.get("country")
        state_col = cols_lower.get("state")
        city_col = cols_lower.get("city")

        if campaign_col and country_col and state_col and city_col:
            pk_candidates.extend([campaign_col, country_col, state_col, city_col])
            # Add time_period if present (for daily aggregation)
            if date_col:
                pk_candidates.append(date_col)
                logger.debug(f"Detected PK: ({campaign_col}, {country_col}, {state_col}, {city_col}, {date_col}) - Microsoft Ads geographic (time-series)")
            else:
                logger.debug(f"Detected PK: ({campaign_col}, {country_col}, {state_col}, {city_col}) - Microsoft Ads geographic (summary)")
            return pk_candidates

        # Rule 2: Time-series data with date column (or aggregated if exclude_date)
        # This is a GENERIC fallback for other platforms
        if date_col or exclude_date:
            # Priority order for composite keys (check both snake_case and PascalCase)
            for id_col in ["creative_id", "creativeid", "ad_id", "adid", "adgroup_id", "adgroupid", "campaign_id", "campaignid", "organization_id", "organizationid"]:
                if id_col in cols_lower:
                    pk_candidates.append(cols_lower[id_col])
                    if date_col and not exclude_date:
                        pk_candidates.append(date_col)
                        logger.debug(f"Detected PK: ({cols_lower[id_col]}, {date_col}) - time-series data")
                    else:
                        logger.debug(f"Detected PK: ({cols_lower[id_col]}) - aggregated data (no date)")
                    return pk_candidates

        # Rule 3: Audience targeting tables (Facebook)
        if "audience_id" in cols_lower and "adset_id" in cols_lower:
            pk_candidates.extend([cols_lower["audience_id"], cols_lower["adset_id"]])
            logger.debug("Detected PK: (audience_id, adset_id) - audience targeting")
            return pk_candidates

        # Rule 3b: Insight actions table (Facebook)
        ad_col = cols_lower.get("ad_id") or cols_lower.get("adid")
        if ad_col and "action_type" in cols_lower:
            pk_candidates.extend([ad_col, cols_lower["action_type"]])
            logger.debug("Detected PK: (ad_id, action_type) - insight actions")
            return pk_candidates

        # Rule 4: Device-level aggregation (Google Ads cost_by_device)
        if ad_col and "device" in cols_lower:
            pk_candidates.extend([ad_col, cols_lower["device"]])
            logger.debug("Detected PK: (ad_id, device) - device aggregation")
            return pk_candidates

        # Rule 5: Composite keys without date
        adgroup_col = cols_lower.get("adgroup_id") or cols_lower.get("adgroupid")
        ad_col = cols_lower.get("ad_id") or cols_lower.get("adid")

        if campaign_col and adgroup_col and ad_col:
            pk_candidates.extend([campaign_col, adgroup_col, ad_col])
            logger.debug(f"Detected PK: ({campaign_col}, {adgroup_col}, {ad_col}) - composite key")
            return pk_candidates

        # Fallback: use all columns except metadata (case-insensitive check)
        metadata_cols_lower = {"load_date", "last_updated_date", "row_loaded_date", "ingestion_timestamp"}
        pk_candidates = [
            col for col in df.columns
            if col.lower() not in metadata_cols_lower
        ]
        logger.warning(f"No standard PK detected, using all {len(pk_candidates)} columns")

        return pk_candidates

    def _copy_to_db(self, cursor, table_name: str, df: pd.DataFrame, pk_columns: Optional[List[str]] = None) -> int:
        """Write DataFrame to database using COPY command.

        Args:
            cursor: Database cursor
            table_name: Target table
            df: DataFrame to write
            pk_columns: Primary key columns (if None, will auto-detect)

        Returns:
            Number of rows written

        Raises:
            DatabaseError: If COPY fails
        """
        # Use provided PK columns or auto-detect if not provided
        if pk_columns is None:
            pk_columns = self._detect_pk_columns(df)
            logger.error(f"🔴 _copy_to_db AUTO-DETECTED PK: {pk_columns}")
        else:
            logger.error(f"🟢 _copy_to_db USING PROVIDED PK: {pk_columns}")

        # Drop duplicates ONLY on PK columns (not all columns, to avoid false duplicates from timestamps)
        # NOTE: This should normally not remove anything since load() already deduplicated
        if pk_columns:
            rows_before = len(df)
            df = df.drop_duplicates(subset=pk_columns, keep='first')
            rows_dropped = rows_before - len(df)
            if rows_dropped > 0:
                logger.error(f"🔴 _copy_to_db DROPPED {rows_dropped} duplicates with PK {pk_columns}!")
            else:
                logger.error(f"🟢 _copy_to_db: No duplicates to drop (already clean)")

        # Replace all NaT values with None before converting to list
        # This is necessary because .values.tolist() converts NaT to string "NaT"
        df = df.replace({pd.NaT: None})
        df = df.where(pd.notna(df), None)

        # CRITICAL: Filter NULL values in Primary Key columns
        # This replicates OLD project's behavior (connectdb.py line 301)
        # where rows with NULL in PK columns are dropped before COPY
        if pk_columns:
            rows_before = len(df)
            df = df.dropna(subset=pk_columns)
            rows_filtered = rows_before - len(df)

            if rows_filtered > 0:
                logger.warning(
                    f"Filtered {rows_filtered} rows with NULL values in PK columns {pk_columns} "
                    f"before writing to {table_name}"
                )

        # DEBUG: Check load_date values
        if 'load_date' in df.columns:
            logger.debug(f"load_date dtype: {df['load_date'].dtype}")
            logger.debug(f"load_date first 3 values: {df['load_date'].head(3).tolist()}")
            logger.debug(f"load_date null count: {df['load_date'].isna().sum()}")

        # Build COPY statement
        columns_str = ",".join(df.columns)
        sql_query = f"COPY {self.schema}.{table_name} ({columns_str}) FROM STDIN null 'None' ABORT ON ERROR"

        # DEBUG: Log the full COPY SQL to verify columns
        logger.debug(f"COPY SQL: {sql_query}")
        logger.debug(f"DataFrame columns for COPY: {list(df.columns)}")

        # Build data buffer with proper escaping
        buff = StringIO()
        col_count = len(df.columns)
        row_format = (PIPE_DELIMITER.join(["{}"] * col_count)) + "\n"

        for row_values in df.values.tolist():
            escaped_values = []
            for val in row_values:
                # Convert NaN/None to 'None' string (matches COPY null value)
                if pd.isna(val) or val is None:
                    escaped_values.append('None')
                elif isinstance(val, str):
                    # Escape special characters (backslash first, then pipe)
                    escaped_val = val
                    for char, replacement in ESCAPE_CHARS.items():
                        escaped_val = escaped_val.replace(char, replacement)
                    escaped_values.append(escaped_val)
                else:
                    escaped_values.append(val)

            buff.write(row_format.format(*escaped_values))

        # DEBUG: Log first row of data being sent
        buff_preview = buff.getvalue()[:500] if buff.getvalue() else "EMPTY"
        logger.debug(f"First row of COPY data: {buff_preview}")

        # Execute COPY
        try:
            cursor.copy(sql_query, buff.getvalue())
            cursor.execute("COMMIT")

            # DEBUG: Verify data was written with load_date
            if 'load_date' in df.columns and 'creative_id' in df.columns:
                first_id = df['creative_id'].iloc[0]
                verify_query = f"SELECT creative_id, load_date FROM {self.schema}.{table_name} WHERE creative_id = {first_id} LIMIT 1"
                cursor.execute(verify_query)
                result = cursor.fetchone()
                logger.debug(f"VERIFY after COPY: {verify_query}")
                logger.debug(f"VERIFY result: {result}")

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

    def _increment(
        self,
        cursor,
        table_name: str,
        df: pd.DataFrame,
        pk_columns: Optional[List[str]] = None,
        increment_columns: List[str] = None,
    ) -> Dict[str, int]:
        """Perform INCREMENT operation: INSERT new rows + INCREMENT metrics for existing.

        This implements cumulative metric aggregation:
        - New entities (creative_id, ad_id, etc.) → INSERT with full data
        - Existing entities → UPDATE by ADDING new metrics to existing values

        Example:
            DB before:  creative_id=123 → impressions=1000, clicks=50
            New data:   creative_id=123 → impressions=200,  clicks=10
            DB after:   creative_id=123 → impressions=1200, clicks=60 ✅

        Args:
            cursor: Database cursor
            table_name: Target table name
            df: DataFrame with NEW metric values to ADD
            pk_columns: Primary key columns (None = auto-detect)
            increment_columns: Metric columns to increment (e.g., impressions, clicks)

        Returns:
            Dict with 'rows_inserted' and 'rows_updated' keys

        Raises:
            SocialDatabaseError: If increment operation fails
        """
        # Auto-detect PK columns if not provided (exclude 'date' for aggregated metrics)
        if pk_columns is None:
            pk_columns = self._detect_pk_columns(df, exclude_date=True)

        if not pk_columns:
            logger.warning(f"No PK columns detected for {table_name}, falling back to append mode")
            df_new = self._deduplicate(cursor, table_name, df, None)
            rows_inserted = self._copy_to_db(cursor, table_name, df_new)
            return {"rows_inserted": rows_inserted, "rows_updated": 0}

        logger.debug(f"INCREMENT using PK columns: {pk_columns}, increment columns: {increment_columns}")

        try:
            # Step 1: Query existing keys from database
            existing_keys = self._query_existing_keys(cursor, table_name, df, pk_columns)

            # Step 2: Separate new rows from existing rows
            df_copy = df.copy()
            df_copy['_merge_key'] = df_copy[pk_columns].apply(lambda row: tuple(row), axis=1)
            df_copy['_is_new'] = ~df_copy['_merge_key'].isin(existing_keys)

            new_rows = df_copy[df_copy['_is_new']].drop(columns=['_merge_key', '_is_new'])
            update_rows = df_copy[~df_copy['_is_new']].drop(columns=['_merge_key', '_is_new'])

            rows_inserted = 0
            rows_updated = 0

            # Step 3: INSERT new rows (with all columns)
            if not new_rows.empty:
                rows_inserted = self._copy_to_db(cursor, table_name, new_rows)
                logger.info(f"✓ Inserted {rows_inserted} new rows")

            # Step 4: INCREMENT metrics for existing rows
            if not update_rows.empty:
                rows_updated = self._batch_increment_metrics(
                    cursor, table_name, update_rows, pk_columns, increment_columns
                )
                logger.info(f"✓ Incremented {rows_updated} existing rows")

            return {"rows_inserted": rows_inserted, "rows_updated": rows_updated}

        except Exception as e:
            logger.error(f"INCREMENT failed for {table_name}: {e}")
            raise SocialDatabaseError(
                f"INCREMENT operation failed for {table_name}",
                details={"error": str(e), "pk_columns": pk_columns, "increment_columns": increment_columns}
            )

    def _query_existing_keys(
        self,
        cursor,
        table_name: str,
        df: pd.DataFrame,
        pk_columns: List[str],
    ) -> set:
        """Query existing primary keys from database for efficient lookup.

        Args:
            cursor: Database cursor
            table_name: Table name
            df: DataFrame with new data
            pk_columns: Primary key columns

        Returns:
            Set of tuples representing existing keys
        """
        # Build query to get existing keys
        columns_str = ", ".join(pk_columns)

        # Add WHERE filter for performance (if date column exists)
        where_clauses = []
        if "date" in df.columns:
            min_date = df["date"].min()
            max_date = df["date"].max()
            where_clauses.append(f"date BETWEEN '{min_date}' AND '{max_date}'")

        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        query = f"""
            SELECT {columns_str}
            FROM {self.schema}.{table_name}
            {where_clause}
        """

        try:
            cursor.execute(query)
            existing_data = cursor.fetchall()

            # Convert to set of tuples for fast lookup
            if len(pk_columns) == 1:
                # Single column PK
                return set(row[0] for row in existing_data)
            else:
                # Multi-column PK
                return set(tuple(row) for row in existing_data)

        except Exception as e:
            logger.warning(f"Failed to query existing keys: {e}, assuming no existing data")
            return set()

    def _batch_increment_metrics(
        self,
        cursor,
        table_name: str,
        df: pd.DataFrame,
        pk_columns: List[str],
        increment_columns: List[str],
    ) -> int:
        """Batch UPDATE using incremental addition (metric = metric + new_value).

        Args:
            cursor: Database cursor
            table_name: Table name
            df: DataFrame with rows to increment
            pk_columns: Primary key columns
            increment_columns: Metric columns to increment

        Returns:
            Number of rows updated
        """
        # Build UPDATE query with incremental SET clauses
        set_clauses = [f"{col} = {col} + %s" for col in increment_columns]

        # Also update load_date and row_loaded_date if present in DataFrame
        date_columns = []
        if "load_date" in df.columns:
            set_clauses.append("load_date = %s")
            date_columns.append("load_date")
        if "row_loaded_date" in df.columns:
            set_clauses.append("row_loaded_date = %s")
            date_columns.append("row_loaded_date")

        set_clause = ", ".join(set_clauses)

        where_clauses = [f"{col} = %s" for col in pk_columns]
        where_clause = " AND ".join(where_clauses)

        query = f"""
            UPDATE {self.schema}.{table_name}
            SET {set_clause}
            WHERE {where_clause}
        """

        # Prepare batch data: (increment_values..., date_values..., pk_values...)
        batch_data = []
        for _, row in df.iterrows():
            # First: values to increment
            values = [row[col] for col in increment_columns]
            # Then: date values
            for col in date_columns:
                values.append(row[col])
            # Finally: PK values for WHERE clause
            values.extend([row[col] for col in pk_columns])
            batch_data.append(tuple(values))

        # Execute batch UPDATE
        try:
            cursor.executemany(query, batch_data)
            cursor.execute("COMMIT")
            logger.debug(f"Batch incremented {len(batch_data)} rows")
            return len(batch_data)

        except Exception as e:
            logger.error(f"Batch increment failed: {e}")
            raise SocialDatabaseError(
                "Batch increment failed",
                query=query[:500],
                details={"error": str(e), "rows": len(batch_data)}
            )
