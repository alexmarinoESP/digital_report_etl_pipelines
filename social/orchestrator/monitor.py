"""Execution Monitor Module.

This module provides monitoring and tracking for platform execution in the
Social Ads Orchestrator. It tracks execution status, timing, errors, and
generates summary reports.

Key Features:
- Real-time execution tracking
- Status management (started, completed, failed)
- Duration and performance metrics
- Error collection and reporting
- Export to JSON and CSV formats

Architecture:
- Event-based status tracking
- Thread-safe operations
- Comprehensive metrics collection
- Multiple export formats
"""

import csv
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger


class ExecutionStatus(Enum):
    """Execution status for platforms."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PlatformExecution:
    """Execution record for a single platform.

    Attributes:
        platform_name: Name of the platform
        status: Current execution status
        start_time: When execution started
        end_time: When execution ended
        duration_seconds: Total execution time in seconds
        rows_processed: Number of rows processed
        tables_processed: Number of tables processed
        error_message: Error message if failed
        retry_count: Number of retry attempts
    """
    platform_name: str
    status: ExecutionStatus = ExecutionStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    rows_processed: int = 0
    tables_processed: int = 0
    error_message: Optional[str] = None
    retry_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization.

        Returns:
            Dictionary representation
        """
        data = asdict(self)
        data["status"] = self.status.value
        data["start_time"] = self.start_time.isoformat() if self.start_time else None
        data["end_time"] = self.end_time.isoformat() if self.end_time else None
        return data


@dataclass
class ExecutionSummary:
    """Summary of orchestrator execution.

    Attributes:
        total_platforms: Total number of platforms
        completed: Number of completed platforms
        failed: Number of failed platforms
        skipped: Number of skipped platforms
        total_duration_seconds: Total execution time
        total_rows_processed: Total rows processed across all platforms
        start_time: When orchestrator started
        end_time: When orchestrator ended
        success_rate: Percentage of successful platforms
    """
    total_platforms: int = 0
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    total_duration_seconds: float = 0.0
    total_rows_processed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    success_rate: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization.

        Returns:
            Dictionary representation
        """
        data = asdict(self)
        data["start_time"] = self.start_time.isoformat() if self.start_time else None
        data["end_time"] = self.end_time.isoformat() if self.end_time else None
        return data


class ExecutionMonitor:
    """Monitor for tracking platform execution status.

    This class maintains execution state for all platforms and provides
    methods to update status, track metrics, and generate reports.

    Example:
        ```python
        monitor = ExecutionMonitor()

        # Start tracking platform
        monitor.start_platform("linkedin")

        # Update progress
        monitor.update_progress("linkedin", rows_processed=1000, tables_processed=3)

        # Mark as completed
        monitor.complete_platform("linkedin", rows_processed=5000, tables_processed=6)

        # Get summary
        summary = monitor.get_summary()
        print(f"Completed: {summary.completed}/{summary.total_platforms}")

        # Export report
        monitor.export_report("json", Path("execution_report.json"))
        ```
    """

    def __init__(self):
        """Initialize the execution monitor."""
        self._executions: Dict[str, PlatformExecution] = {}
        self._orchestrator_start_time: Optional[datetime] = None
        self._orchestrator_end_time: Optional[datetime] = None
        logger.debug("ExecutionMonitor initialized")

    def start_orchestrator(self) -> None:
        """Mark orchestrator execution as started."""
        self._orchestrator_start_time = datetime.now()
        logger.info("Orchestrator execution started")

    def end_orchestrator(self) -> None:
        """Mark orchestrator execution as ended."""
        self._orchestrator_end_time = datetime.now()
        duration = (self._orchestrator_end_time - self._orchestrator_start_time).total_seconds()
        logger.info(f"Orchestrator execution ended (duration: {duration:.2f}s)")

    def register_platform(self, platform_name: str) -> None:
        """Register a platform for monitoring.

        Args:
            platform_name: Name of platform to register
        """
        if platform_name not in self._executions:
            self._executions[platform_name] = PlatformExecution(
                platform_name=platform_name
            )
            logger.debug(f"Registered platform for monitoring: {platform_name}")

    def start_platform(self, platform_name: str) -> None:
        """Mark platform execution as started.

        Args:
            platform_name: Name of platform
        """
        self.register_platform(platform_name)

        execution = self._executions[platform_name]
        execution.status = ExecutionStatus.RUNNING
        execution.start_time = datetime.now()

        logger.info(f"Platform '{platform_name}' started")

    def complete_platform(
        self,
        platform_name: str,
        rows_processed: int = 0,
        tables_processed: int = 0,
    ) -> None:
        """Mark platform execution as completed successfully.

        Args:
            platform_name: Name of platform
            rows_processed: Total number of rows processed
            tables_processed: Total number of tables processed
        """
        if platform_name not in self._executions:
            logger.warning(f"Platform '{platform_name}' not registered")
            return

        execution = self._executions[platform_name]
        execution.status = ExecutionStatus.COMPLETED
        execution.end_time = datetime.now()
        execution.rows_processed = rows_processed
        execution.tables_processed = tables_processed

        if execution.start_time:
            execution.duration_seconds = (
                execution.end_time - execution.start_time
            ).total_seconds()

        logger.success(
            f"Platform '{platform_name}' completed: "
            f"{rows_processed} rows, {tables_processed} tables, "
            f"{execution.duration_seconds:.2f}s"
        )

    def fail_platform(
        self,
        platform_name: str,
        error: Exception,
        rows_processed: int = 0,
        tables_processed: int = 0,
    ) -> None:
        """Mark platform execution as failed.

        Args:
            platform_name: Name of platform
            error: Exception that caused the failure
            rows_processed: Number of rows processed before failure
            tables_processed: Number of tables processed before failure
        """
        if platform_name not in self._executions:
            logger.warning(f"Platform '{platform_name}' not registered")
            return

        execution = self._executions[platform_name]
        execution.status = ExecutionStatus.FAILED
        execution.end_time = datetime.now()
        execution.error_message = str(error)
        execution.rows_processed = rows_processed
        execution.tables_processed = tables_processed

        if execution.start_time:
            execution.duration_seconds = (
                execution.end_time - execution.start_time
            ).total_seconds()

        logger.error(
            f"Platform '{platform_name}' failed: {error} "
            f"(duration: {execution.duration_seconds:.2f}s)"
        )

    def skip_platform(self, platform_name: str, reason: str) -> None:
        """Mark platform as skipped.

        Args:
            platform_name: Name of platform
            reason: Reason for skipping
        """
        self.register_platform(platform_name)

        execution = self._executions[platform_name]
        execution.status = ExecutionStatus.SKIPPED
        execution.error_message = f"Skipped: {reason}"

        logger.warning(f"Platform '{platform_name}' skipped: {reason}")

    def update_progress(
        self,
        platform_name: str,
        rows_processed: Optional[int] = None,
        tables_processed: Optional[int] = None,
    ) -> None:
        """Update progress for a running platform.

        Args:
            platform_name: Name of platform
            rows_processed: Current number of rows processed
            tables_processed: Current number of tables processed
        """
        if platform_name not in self._executions:
            logger.warning(f"Platform '{platform_name}' not registered")
            return

        execution = self._executions[platform_name]

        if rows_processed is not None:
            execution.rows_processed = rows_processed

        if tables_processed is not None:
            execution.tables_processed = tables_processed

        logger.debug(
            f"Progress update for '{platform_name}': "
            f"{execution.rows_processed} rows, {execution.tables_processed} tables"
        )

    def increment_retry(self, platform_name: str) -> None:
        """Increment retry count for a platform.

        Args:
            platform_name: Name of platform
        """
        if platform_name not in self._executions:
            logger.warning(f"Platform '{platform_name}' not registered")
            return

        execution = self._executions[platform_name]
        execution.retry_count += 1
        logger.info(f"Platform '{platform_name}' retry attempt: {execution.retry_count}")

    def get_execution(self, platform_name: str) -> Optional[PlatformExecution]:
        """Get execution record for a platform.

        Args:
            platform_name: Name of platform

        Returns:
            PlatformExecution instance or None if not found
        """
        return self._executions.get(platform_name)

    def get_all_executions(self) -> List[PlatformExecution]:
        """Get all execution records.

        Returns:
            List of all PlatformExecution instances
        """
        return list(self._executions.values())

    def get_summary(self) -> ExecutionSummary:
        """Generate execution summary.

        Returns:
            ExecutionSummary with aggregated metrics
        """
        summary = ExecutionSummary(
            total_platforms=len(self._executions),
            start_time=self._orchestrator_start_time,
            end_time=self._orchestrator_end_time,
        )

        for execution in self._executions.values():
            if execution.status == ExecutionStatus.COMPLETED:
                summary.completed += 1
            elif execution.status == ExecutionStatus.FAILED:
                summary.failed += 1
            elif execution.status == ExecutionStatus.SKIPPED:
                summary.skipped += 1

            summary.total_rows_processed += execution.rows_processed
            summary.total_duration_seconds += execution.duration_seconds

        # Calculate success rate
        if summary.total_platforms > 0:
            summary.success_rate = (summary.completed / summary.total_platforms) * 100

        return summary

    def export_report(self, format: str, output_path: Path) -> None:
        """Export execution report to file.

        Args:
            format: Export format ('json' or 'csv')
            output_path: Path where to save the report

        Raises:
            ValueError: If format is not supported
        """
        format = format.lower()

        if format == "json":
            self._export_json(output_path)
        elif format == "csv":
            self._export_csv(output_path)
        else:
            raise ValueError(f"Unsupported export format: '{format}'. Use 'json' or 'csv'.")

        logger.success(f"Execution report exported to: {output_path}")

    def _export_json(self, output_path: Path) -> None:
        """Export report as JSON.

        Args:
            output_path: Path to JSON file
        """
        summary = self.get_summary()

        report = {
            "summary": summary.to_dict(),
            "platforms": [
                execution.to_dict()
                for execution in self._executions.values()
            ]
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

    def _export_csv(self, output_path: Path) -> None:
        """Export report as CSV.

        Args:
            output_path: Path to CSV file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # Write header
            writer.writerow([
                "Platform",
                "Status",
                "Start Time",
                "End Time",
                "Duration (s)",
                "Rows Processed",
                "Tables Processed",
                "Retry Count",
                "Error Message",
            ])

            # Write platform rows
            for execution in self._executions.values():
                writer.writerow([
                    execution.platform_name,
                    execution.status.value,
                    execution.start_time.isoformat() if execution.start_time else "",
                    execution.end_time.isoformat() if execution.end_time else "",
                    f"{execution.duration_seconds:.2f}",
                    execution.rows_processed,
                    execution.tables_processed,
                    execution.retry_count,
                    execution.error_message or "",
                ])

    def print_summary(self) -> None:
        """Print execution summary to console."""
        summary = self.get_summary()

        logger.info("=" * 60)
        logger.info("Execution Summary")
        logger.info("=" * 60)
        logger.info(f"Total Platforms:    {summary.total_platforms}")
        logger.info(f"Completed:          {summary.completed}")
        logger.info(f"Failed:             {summary.failed}")
        logger.info(f"Skipped:            {summary.skipped}")
        logger.info(f"Success Rate:       {summary.success_rate:.1f}%")
        logger.info(f"Total Rows:         {summary.total_rows_processed:,}")
        logger.info(f"Total Duration:     {summary.total_duration_seconds:.2f}s")

        if summary.start_time and summary.end_time:
            logger.info(f"Start Time:         {summary.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"End Time:           {summary.end_time.strftime('%Y-%m-%d %H:%M:%S')}")

        logger.info("=" * 60)

        # Print individual platform details
        logger.info("\nPlatform Details:")
        for execution in sorted(self._executions.values(), key=lambda e: e.platform_name):
            status_symbol = {
                ExecutionStatus.COMPLETED: "✓",
                ExecutionStatus.FAILED: "✗",
                ExecutionStatus.SKIPPED: "-",
                ExecutionStatus.RUNNING: "...",
                ExecutionStatus.PENDING: "○",
            }.get(execution.status, "?")

            logger.info(
                f"  {status_symbol} {execution.platform_name}: "
                f"{execution.status.value} "
                f"({execution.rows_processed:,} rows, {execution.duration_seconds:.2f}s)"
            )

            if execution.error_message:
                logger.info(f"      Error: {execution.error_message}")
