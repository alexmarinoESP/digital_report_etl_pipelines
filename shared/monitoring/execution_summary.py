"""Execution Summary Writer for Pipeline Monitoring.

This module provides functionality to write execution summaries for
social media ETL pipelines, supporting dual output:
1. stdout (for console logging and Azure Container Apps logs)
2. Azure Blob Storage (for Logic App consumption and audit trail)

The summaries are used by Azure Logic Apps to generate consolidated
nightly reports with success/failure status for all platforms.

Architecture:
- Each pipeline execution writes to a unique blob path
- Path format: {platform}/{date}/exec-{timestamp}-{uuid}.json
- No file conflicts - each execution has unique UUID
- Logic App reads all blobs for a given date and aggregates

Example Usage:
    from shared.monitoring import ExecutionSummaryWriter

    summary = ExecutionSummaryWriter(
        platform="facebook",
        storage_connection_string=os.getenv("SUMMARY_STORAGE_CONNECTION_STRING")
    )

    # On success
    summary.write_success(
        start_time=start_time,
        end_time=datetime.now(),
        tables_processed=results,
        exit_code=0
    )

    # On failure
    summary.write_failure(
        start_time=start_time,
        end_time=datetime.now(),
        error=str(e),
        exit_code=3
    )
"""

import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from loguru import logger


class ExecutionSummaryWriter:
    """Writes pipeline execution summaries to stdout and Azure Blob Storage.

    This class handles the creation and persistence of execution summaries
    for social media ETL pipelines. Each execution gets a unique identifier
    and writes to a dedicated blob path to avoid concurrency issues.

    Attributes:
        platform: Platform name (facebook, google, microsoft, linkedin)
        execution_id: Unique identifier for this execution (UUID)
        storage_connection_string: Azure Blob Storage connection string (optional)
        container_name: Blob container name (default: social-pipeline-logs)
    """

    # Default container name (matches Azure setup)
    DEFAULT_CONTAINER_NAME = "social-pipeline-logs"

    def __init__(
        self,
        platform: str,
        execution_id: Optional[str] = None,
        storage_connection_string: Optional[str] = None,
        container_name: Optional[str] = None,
    ):
        """Initialize ExecutionSummaryWriter.

        Args:
            platform: Platform name (facebook, google, microsoft, linkedin)
            execution_id: Unique execution ID (auto-generated if not provided)
            storage_connection_string: Azure Blob Storage connection string
            container_name: Blob container name (uses default if not provided)
        """
        self.platform = platform.lower()
        self.execution_id = execution_id or str(uuid.uuid4())
        self.storage_connection_string = storage_connection_string
        self.container_name = container_name or self.DEFAULT_CONTAINER_NAME

        # Lazy-load Azure SDK only if storage is configured
        self._blob_service = None

        logger.debug(
            f"ExecutionSummaryWriter initialized: "
            f"platform={self.platform}, execution_id={self.execution_id[:8]}..."
        )

    def write_success(
        self,
        start_time: datetime,
        end_time: datetime,
        tables_processed: Dict[str, pd.DataFrame],
        exit_code: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write execution summary for successful pipeline run.

        Args:
            start_time: Pipeline start timestamp
            end_time: Pipeline end timestamp
            tables_processed: Dictionary of {table_name: DataFrame}
            exit_code: Exit code (default: 0 for success)
            metadata: Optional additional metadata to include
        """
        # Calculate total rows across all tables
        total_rows = sum(len(df) for df in tables_processed.values())

        # Format table details
        tables = self._format_tables(tables_processed, success=True)

        # Build summary
        summary = {
            "execution_id": self.execution_id,
            "platform": self.platform,
            "status": "success",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds(),
            "tables": tables,
            "tables_count": len(tables),
            "total_rows": total_rows,
            "exit_code": exit_code,
            "errors": [],
            "metadata": metadata or {},
        }

        # Write to outputs
        self._write_summary(summary)

        logger.success(
            f"Execution summary written: {len(tables)} tables, {total_rows} rows"
        )

    def write_partial_success(
        self,
        start_time: datetime,
        end_time: datetime,
        tables_succeeded: Dict[str, pd.DataFrame],
        tables_failed: List[str],
        errors: List[Dict[str, str]],
        exit_code: int = 3,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write execution summary for partially successful pipeline run.

        Args:
            start_time: Pipeline start timestamp
            end_time: Pipeline end timestamp
            tables_succeeded: Dictionary of {table_name: DataFrame} that succeeded
            tables_failed: List of table names that failed
            errors: List of error dictionaries with 'table' and 'message' keys
            exit_code: Exit code (default: 3 for pipeline error)
            metadata: Optional additional metadata to include
        """
        # Calculate total rows from successful tables
        total_rows = sum(len(df) for df in tables_succeeded.values())

        # Format successful tables
        tables_success = self._format_tables(tables_succeeded, success=True)

        # Format failed tables
        tables_fail = [
            {"name": table_name, "success": False, "rows": 0, "error": "Failed"}
            for table_name in tables_failed
        ]

        # Combine
        all_tables = tables_success + tables_fail

        # Build summary
        summary = {
            "execution_id": self.execution_id,
            "platform": self.platform,
            "status": "partial_success",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds(),
            "tables": all_tables,
            "tables_count": len(all_tables),
            "tables_succeeded": len(tables_success),
            "tables_failed": len(tables_fail),
            "total_rows": total_rows,
            "exit_code": exit_code,
            "errors": errors,
            "metadata": metadata or {},
        }

        # Write to outputs
        self._write_summary(summary)

        logger.warning(
            f"Execution summary written (partial): "
            f"{len(tables_success)}/{len(all_tables)} tables succeeded"
        )

    def write_failure(
        self,
        start_time: datetime,
        end_time: datetime,
        error: Union[str, Exception],
        exit_code: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write execution summary for failed pipeline run.

        Args:
            start_time: Pipeline start timestamp
            end_time: Pipeline end timestamp
            error: Error message or exception
            exit_code: Exit code (1-4 based on error type)
            metadata: Optional additional metadata to include
        """
        # Format error
        error_message = str(error)
        error_type = type(error).__name__ if isinstance(error, Exception) else "Error"

        # Build summary
        summary = {
            "execution_id": self.execution_id,
            "platform": self.platform,
            "status": "failed",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds(),
            "tables": [],
            "tables_count": 0,
            "total_rows": 0,
            "exit_code": exit_code,
            "errors": [
                {
                    "type": error_type,
                    "message": error_message,
                    "timestamp": datetime.now().isoformat(),
                }
            ],
            "metadata": metadata or {},
        }

        # Write to outputs
        self._write_summary(summary)

        logger.error(f"Execution summary written (failure): {error_message}")

    def _format_tables(
        self, tables: Dict[str, pd.DataFrame], success: bool = True
    ) -> List[Dict[str, Any]]:
        """Format table processing results for summary.

        Args:
            tables: Dictionary of {table_name: DataFrame}
            success: Whether tables were processed successfully

        Returns:
            List of table summary dictionaries
        """
        formatted = []

        for table_name, df in tables.items():
            table_info = {
                "name": table_name,
                "success": success,
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns)[:10],  # Limit to first 10 columns
            }

            # Add sample of first row if DataFrame not empty
            if not df.empty and len(df.columns) > 0:
                try:
                    # Convert first row to dict, handling non-serializable types
                    first_row = df.iloc[0].to_dict()
                    sample = {}
                    for k, v in first_row.items():
                        # Only include first 5 columns for brevity
                        if len(sample) >= 5:
                            break
                        # Convert to JSON-serializable type
                        if pd.isna(v):
                            sample[k] = None
                        elif isinstance(v, (int, float, str, bool)):
                            sample[k] = v
                        else:
                            sample[k] = str(v)

                    table_info["sample"] = sample
                except Exception as e:
                    logger.debug(f"Could not create sample for {table_name}: {e}")

            formatted.append(table_info)

        return formatted

    def _write_summary(self, summary: Dict[str, Any]) -> None:
        """Write summary to stdout and Azure Blob Storage.

        Args:
            summary: Summary dictionary to write
        """
        # 1. STDOUT - Always write to console (for Container Apps logs)
        self._write_to_stdout(summary)

        # 2. BLOB STORAGE - Only if connection string configured
        if self.storage_connection_string:
            try:
                self._write_to_blob(summary)
            except Exception as e:
                # Don't fail the pipeline if summary upload fails
                logger.warning(f"Failed to upload summary to Blob Storage: {e}")
                logger.debug(f"Summary upload error details", exc_info=True)
        else:
            logger.debug("No storage connection string - skipping blob upload")

    def _write_to_stdout(self, summary: Dict[str, Any]) -> None:
        """Write summary to stdout in formatted JSON.

        Args:
            summary: Summary dictionary to write
        """
        summary_json = json.dumps(summary, indent=2, default=str)

        # Print with visual separators for easy log parsing
        print("\n" + "=" * 80)
        print("EXECUTION SUMMARY")
        print("=" * 80)
        print(summary_json)
        print("=" * 80 + "\n")

    def _write_to_blob(self, summary: Dict[str, Any]) -> None:
        """Write summary to Azure Blob Storage.

        Args:
            summary: Summary dictionary to write

        Raises:
            Exception: If blob upload fails
        """
        # Lazy-load Azure SDK
        if self._blob_service is None:
            from azure.storage.blob import BlobServiceClient

            self._blob_service = BlobServiceClient.from_connection_string(
                self.storage_connection_string
            )

        # Generate unique blob path
        blob_path = self._generate_blob_path()

        # Get container client (create container if not exists)
        container_client = self._blob_service.get_container_client(self.container_name)

        try:
            # Try to create container (idempotent)
            container_client.create_container()
            logger.debug(f"Created container: {self.container_name}")
        except Exception:
            # Container already exists - this is fine
            pass

        # Upload blob
        blob_client = container_client.get_blob_client(blob_path)

        # Serialize summary to JSON
        summary_json = json.dumps(summary, indent=2, default=str)

        # Upload (overwrite is safe because path is unique with UUID)
        blob_client.upload_blob(
            summary_json,
            overwrite=True,
            content_settings={
                "content_type": "application/json",
            },
        )

        logger.success(f"Summary uploaded to blob: {blob_path}")

    def _generate_blob_path(self) -> str:
        """Generate unique blob path for this execution.

        Returns:
            Blob path string in format: {platform}/{date}/exec-{timestamp}-{uuid}.json
        """
        # Use current date for folder organization
        date_str = datetime.now().strftime("%Y-%m-%d")

        # Use timestamp for ordering within day
        timestamp_str = datetime.now().strftime("%Y%m%d-%H%M%S")

        # Use first 8 chars of UUID for uniqueness
        uuid_short = self.execution_id[:8]

        # Build path
        blob_path = f"{self.platform}/{date_str}/exec-{timestamp_str}-{uuid_short}.json"

        return blob_path
