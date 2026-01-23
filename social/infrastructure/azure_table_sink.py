"""
Azure Table Storage Data Sink Implementation

Provides DataSink Protocol implementation for Azure Table Storage.
Used as alternative storage to Vertica for Microsoft Ads and other platforms.

SOLID Principles:
- Single Responsibility: Only handles Azure Table Storage operations
- Open/Closed: Extensible through Protocol interface
- Liskov Substitution: Implements DataSink Protocol fully
- Interface Segregation: Clean DataSink interface
- Dependency Inversion: Depends on Protocol abstraction
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
import pandas as pd
from azure.data.tables import TableServiceClient, TableClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from loguru import logger

from social.core.protocols import DataSink


class AzureTableDataSink(DataSink):
    """DataSink implementation for Azure Table Storage.

    Provides storage alternative to Vertica, suitable for:
    - Cloud-native deployments
    - Cost-effective append-only data
    - Simple key-value lookups
    - Development/testing environments

    Attributes:
        connection_string: Azure Storage account connection string
        table_name: Target table name
        partition_key_column: Column to use for PartitionKey (default: "date")
        row_key_columns: Columns to combine for RowKey (default: ["id"])
    """

    def __init__(
        self,
        connection_string: str,
        table_name: str,
        partition_key_column: str = "date",
        row_key_columns: Optional[List[str]] = None,
    ):
        """Initialize Azure Table Storage sink.

        Args:
            connection_string: Azure Storage connection string
            table_name: Name of the table to write to
            partition_key_column: Column to use for PartitionKey (for partitioning strategy)
            row_key_columns: List of columns to combine for unique RowKey

        Example:
            ```python
            sink = AzureTableDataSink(
                connection_string="DefaultEndpointsProtocol=https;...",
                table_name="microsoftads",
                partition_key_column="date",
                row_key_columns=["AccountId", "CampaignId", "AdId"]
            )
            ```
        """
        self.table_name = table_name
        self.partition_key_column = partition_key_column
        self.row_key_columns = row_key_columns or ["id"]

        # Initialize table service
        self.table_service = TableServiceClient.from_connection_string(
            connection_string
        )

        # Create table if not exists
        try:
            self.table_client = self.table_service.create_table_if_not_exists(
                table_name
            )
            logger.info(f"Connected to Azure Table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Azure Table {table_name}: {e}")
            raise

    def write(
        self,
        df: pd.DataFrame,
        table_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        if_exists: str = "append",
    ) -> int:
        """Write DataFrame to Azure Table Storage.

        Args:
            df: DataFrame to write
            table_name: Override table name (optional, uses instance table_name if not provided)
            schema_name: Ignored (Azure Tables don't have schemas)
            if_exists: Write mode:
                - 'append': Insert new entities (default)
                - 'replace': Delete all entities first, then insert
                - 'fail': Raise error if table exists

        Returns:
            Number of entities written

        Raises:
            ValueError: If DataFrame is empty or if_exists mode is invalid
            HttpResponseError: If Azure operation fails

        Example:
            ```python
            rows_written = sink.write(
                df=dataframe,
                if_exists='append'
            )
            print(f"Wrote {rows_written} rows")
            ```
        """
        if df.empty:
            logger.warning("Empty DataFrame provided, nothing to write")
            return 0

        # Validate mode
        if if_exists not in ["append", "replace", "fail"]:
            raise ValueError(
                f"Invalid if_exists mode: {if_exists}. Must be 'append', 'replace', or 'fail'"
            )

        # Use provided table name or instance default
        target_table = table_name or self.table_name

        # Get table client
        if target_table != self.table_name:
            table_client = self.table_service.create_table_if_not_exists(target_table)
        else:
            table_client = self.table_client

        # Handle 'fail' mode
        if if_exists == "fail":
            try:
                # Check if table exists and has data
                entities = list(table_client.list_entities(results_per_page=1))
                if entities:
                    raise ValueError(
                        f"Table {target_table} already exists and has data. "
                        "Use if_exists='append' or 'replace'"
                    )
            except ResourceNotFoundError:
                pass  # Table doesn't exist, proceed

        # Handle 'replace' mode
        if if_exists == "replace":
            deleted = self._truncate_table(table_client)
            logger.info(f"Truncated {deleted} entities from {target_table}")

        # Convert DataFrame to entities
        entities = self._dataframe_to_entities(df)

        # Batch write (100 entities per batch - Azure limit)
        written_count = self._batch_write_entities(table_client, entities)

        logger.success(
            f"Wrote {written_count} entities to Azure Table {target_table}"
        )
        return written_count

    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = "append",
    ) -> int:
        """Alternative interface for Protocol compatibility.

        Maps to write() method for DataSink Protocol implementation.

        Args:
            df: DataFrame to load
            table_name: Target table name
            mode: Load mode ('append', 'replace', 'upsert' - maps to if_exists)

        Returns:
            Number of rows loaded
        """
        # Map mode to if_exists
        if_exists_map = {
            "append": "append",
            "replace": "replace",
            "upsert": "append",  # Azure Tables upsert by default on key collision
        }
        if_exists = if_exists_map.get(mode, "append")

        return self.write(df=df, table_name=table_name, if_exists=if_exists)

    def query(self, sql: str) -> pd.DataFrame:
        """Query Azure Table Storage.

        Note: Azure Tables don't support SQL queries. This method provides
        limited querying capability using OData filters.

        Args:
            sql: Not supported - provide OData filter string instead

        Returns:
            DataFrame with query results

        Raises:
            NotImplementedError: Full SQL queries not supported by Azure Tables

        Example:
            ```python
            # Instead of SQL, use OData filter
            filter_str = "PartitionKey eq '20240115'"
            df = sink.query(filter_str)  # Will fail - not implemented
            ```
        """
        raise NotImplementedError(
            "Azure Table Storage does not support SQL queries. "
            "For complex queries, use Vertica. "
            "For simple filters, use query_with_filter() method."
        )

    def query_with_filter(
        self, filter_string: Optional[str] = None, select: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Query Azure Table using OData filter.

        Args:
            filter_string: OData filter expression (e.g., "PartitionKey eq '20240115'")
            select: List of columns to return (None = all columns)

        Returns:
            DataFrame with matching entities

        Example:
            ```python
            # Get all rows for specific date
            df = sink.query_with_filter("PartitionKey eq '20240115'")

            # Get specific columns
            df = sink.query_with_filter(
                filter_string="PartitionKey eq '20240115'",
                select=["AccountId", "Clicks", "Impressions"]
            )
            ```
        """
        try:
            entities = list(
                self.table_client.query_entities(
                    query_filter=filter_string, select=select
                )
            )

            if not entities:
                logger.warning("Query returned no results")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(entities)

            # Remove Azure metadata columns
            metadata_cols = ["odata.etag", "Timestamp", "etag"]
            df = df.drop(columns=[c for c in metadata_cols if c in df.columns])

            logger.info(f"Query returned {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in Azure Storage account.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists, False otherwise
        """
        try:
            tables = self.table_service.list_tables(results_per_page=1000)
            return any(t.name == table_name for t in tables)
        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            return False

    def _dataframe_to_entities(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert DataFrame rows to Azure Table entities.

        Adds PartitionKey and RowKey based on configuration.

        Args:
            df: DataFrame to convert

        Returns:
            List of entity dictionaries ready for Azure Table insert
        """
        entities = []

        for _, row in df.iterrows():
            # Generate PartitionKey
            partition_key = self._generate_partition_key(row)

            # Generate RowKey
            row_key = self._generate_row_key(row)

            # Create entity
            entity = {"PartitionKey": partition_key, "RowKey": row_key}

            # Add all columns
            for col, value in row.items():
                # Azure Tables type conversion
                if pd.isna(value):
                    entity[col] = None
                elif isinstance(value, (pd.Int64Dtype, pd.Float64Dtype)):
                    entity[col] = float(value)
                elif isinstance(value, datetime):
                    entity[col] = value.isoformat()
                elif isinstance(value, (list, dict)):
                    # Serialize complex types as JSON string
                    import json

                    entity[col] = json.dumps(value)
                else:
                    entity[col] = str(value)

            entities.append(entity)

        return entities

    def _generate_partition_key(self, row: pd.Series) -> str:
        """Generate PartitionKey from row data.

        Strategy: Use configured column (default: date in YYYYMMDD format).

        Args:
            row: DataFrame row

        Returns:
            PartitionKey string
        """
        if self.partition_key_column in row.index:
            value = row[self.partition_key_column]

            # Handle date formats
            if isinstance(value, str):
                # Try to parse and reformat
                try:
                    dt = pd.to_datetime(value)
                    return dt.strftime("%Y%m%d")
                except:
                    # Use as-is if not parseable
                    return value.replace("-", "").replace("/", "")[:8]
            elif isinstance(value, datetime):
                return value.strftime("%Y%m%d")
            else:
                return str(value)

        # Fallback: use current date
        return datetime.now().strftime("%Y%m%d")

    def _generate_row_key(self, row: pd.Series) -> str:
        """Generate unique RowKey from row data.

        Strategy: Combine configured columns with underscore separator.

        Args:
            row: DataFrame row

        Returns:
            RowKey string (must be unique within partition)
        """
        key_parts = []

        for col in self.row_key_columns:
            if col in row.index and not pd.isna(row[col]):
                key_parts.append(str(row[col]))

        if not key_parts:
            # Fallback: use timestamp for uniqueness
            return str(int(datetime.now().timestamp() * 1000000))

        return "_".join(key_parts)

    def _batch_write_entities(
        self, table_client: TableClient, entities: List[Dict[str, Any]]
    ) -> int:
        """Write entities in batches (Azure limit: 100 per batch).

        Args:
            table_client: Azure Table client
            entities: List of entities to write

        Returns:
            Number of entities successfully written
        """
        batch_size = 100
        written_count = 0

        for i in range(0, len(entities), batch_size):
            batch = entities[i : i + batch_size]

            try:
                # Use batch transaction (faster than individual upserts)
                operations = [("upsert", entity) for entity in batch]
                table_client.submit_transaction(operations)
                written_count += len(batch)

                logger.debug(
                    f"Batch {i//batch_size + 1}/{(len(entities)-1)//batch_size + 1}: "
                    f"Wrote {len(batch)} entities"
                )

            except HttpResponseError as e:
                logger.error(f"Batch write failed (batch {i//batch_size}): {e}")

                # Fallback: write individually
                for entity in batch:
                    try:
                        table_client.upsert_entity(entity)
                        written_count += 1
                    except Exception as e_individual:
                        logger.error(
                            f"Failed to write entity {entity.get('RowKey')}: {e_individual}"
                        )
                        continue

        return written_count

    def _truncate_table(self, table_client: TableClient) -> int:
        """Delete all entities from table (for 'replace' mode).

        Args:
            table_client: Azure Table client

        Returns:
            Number of entities deleted
        """
        deleted_count = 0

        try:
            # Query all entities (only get keys for efficiency)
            entities = list(table_client.list_entities(select=["PartitionKey", "RowKey"]))

            # Delete in batches
            batch_size = 100
            for i in range(0, len(entities), batch_size):
                batch = entities[i : i + batch_size]

                try:
                    operations = [
                        (
                            "delete",
                            {"PartitionKey": e["PartitionKey"], "RowKey": e["RowKey"]},
                        )
                        for e in batch
                    ]
                    table_client.submit_transaction(operations)
                    deleted_count += len(batch)

                except Exception as e:
                    logger.warning(f"Batch delete failed (batch {i//batch_size}): {e}")

                    # Fallback: delete individually
                    for entity in batch:
                        try:
                            table_client.delete_entity(
                                partition_key=entity["PartitionKey"],
                                row_key=entity["RowKey"],
                            )
                            deleted_count += 1
                        except:
                            continue

            logger.info(f"Truncated {deleted_count} entities")
            return deleted_count

        except Exception as e:
            logger.error(f"Failed to truncate table: {e}")
            return 0

    def get_entity_count(self, partition_key: Optional[str] = None) -> int:
        """Get count of entities in table.

        Args:
            partition_key: Optional partition key to filter by

        Returns:
            Number of entities
        """
        try:
            filter_str = f"PartitionKey eq '{partition_key}'" if partition_key else None
            entities = list(
                self.table_client.query_entities(
                    query_filter=filter_str, select=["PartitionKey"]
                )
            )
            return len(entities)
        except Exception as e:
            logger.error(f"Failed to get entity count: {e}")
            return 0

    def delete_partition(self, partition_key: str) -> int:
        """Delete all entities in a specific partition.

        Useful for data retention policies.

        Args:
            partition_key: Partition key to delete

        Returns:
            Number of entities deleted
        """
        try:
            # Get all entities in partition
            entities = list(
                self.table_client.query_entities(
                    query_filter=f"PartitionKey eq '{partition_key}'",
                    select=["PartitionKey", "RowKey"],
                )
            )

            if not entities:
                logger.info(f"No entities found in partition {partition_key}")
                return 0

            # Delete in batches
            deleted_count = 0
            batch_size = 100

            for i in range(0, len(entities), batch_size):
                batch = entities[i : i + batch_size]

                try:
                    operations = [
                        (
                            "delete",
                            {"PartitionKey": e["PartitionKey"], "RowKey": e["RowKey"]},
                        )
                        for e in batch
                    ]
                    self.table_client.submit_transaction(operations)
                    deleted_count += len(batch)

                except Exception as e:
                    logger.warning(
                        f"Batch delete failed for partition {partition_key}: {e}"
                    )

            logger.success(f"Deleted {deleted_count} entities from partition {partition_key}")
            return deleted_count

        except Exception as e:
            logger.error(f"Failed to delete partition {partition_key}: {e}")
            return 0
