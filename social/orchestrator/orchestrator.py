"""Social Ads Orchestrator Module.

This is the main orchestrator that coordinates execution of all 4 platform pipelines:
Microsoft, LinkedIn, Facebook, and Google Ads.

Key Features:
- Coordinated execution of multiple platforms
- Dependency management between platforms
- Parallel execution support
- Retry logic with exponential backoff
- Comprehensive error handling
- Execution monitoring and reporting

Architecture:
- Protocol-based (no inheritance from platforms)
- Dependency injection for token providers and data sinks
- Configurable execution order and parallelism
- Continue-on-failure support
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from loguru import logger

from social.core.exceptions import ConfigurationError, PipelineError
from social.core.protocols import DataSink, TokenProvider
from social.orchestrator.config import OrchestratorConfig, PlatformConfig
from social.orchestrator.monitor import ExecutionMonitor, ExecutionStatus
from social.orchestrator.registry import PlatformRegistry
from social.orchestrator.scheduler import PlatformScheduler


@dataclass
class OrchestratorResult:
    """Result of orchestrator execution.

    Attributes:
        success: Whether all platforms completed successfully
        completed_platforms: List of successfully completed platform names
        failed_platforms: List of failed platform names
        skipped_platforms: List of skipped platform names
        total_duration_seconds: Total execution time
        total_rows_processed: Total rows processed across all platforms
        error_summary: Dictionary mapping platform names to error messages
    """
    success: bool
    completed_platforms: List[str]
    failed_platforms: List[str]
    skipped_platforms: List[str]
    total_duration_seconds: float
    total_rows_processed: int
    error_summary: Dict[str, str]


class SocialAdsOrchestrator:
    """Main orchestrator for coordinating all platform pipelines.

    This orchestrator manages the execution of Microsoft, LinkedIn, Facebook,
    and Google Ads pipelines with support for dependencies, parallel execution,
    and comprehensive monitoring.

    Example:
        ```python
        # Load configuration
        config = load_orchestrator_config(Path("orchestrator_config.yml"))

        # Create orchestrator
        orchestrator = SocialAdsOrchestrator(
            config=config,
            registry=create_default_registry(),
            token_provider=token_provider,
            data_sink=data_sink
        )

        # Run all platforms
        result = orchestrator.run_all_platforms()

        if result.success:
            print(f"All platforms completed: {result.total_rows_processed} rows")
        else:
            print(f"Some platforms failed: {result.failed_platforms}")
        ```
    """

    def __init__(
        self,
        config: OrchestratorConfig,
        registry: PlatformRegistry,
        token_provider: TokenProvider,
        data_sink: Optional[DataSink] = None,
    ):
        """Initialize the orchestrator.

        Args:
            config: Orchestrator configuration
            registry: Platform registry with registered pipelines
            token_provider: Token provider for authentication
            data_sink: Optional data sink for loading data

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if config is None:
            raise ConfigurationError("Configuration cannot be None")

        if registry is None:
            raise ConfigurationError("Registry cannot be None")

        if token_provider is None:
            raise ConfigurationError("Token provider cannot be None")

        self.config = config
        self.registry = registry
        self.token_provider = token_provider
        self.data_sink = data_sink

        # Initialize components
        self.monitor = ExecutionMonitor()
        self.scheduler = PlatformScheduler()

        # Track execution state
        self._completed_platforms: Set[str] = set()
        self._failed_platforms: Set[str] = set()

        logger.info(
            f"SocialAdsOrchestrator initialized with "
            f"{len(config.platforms)} platforms configured"
        )

    def run_all_platforms(self) -> OrchestratorResult:
        """Run all enabled platforms in configured order.

        This method orchestrates the execution of all enabled platforms,
        respecting dependencies, priorities, and parallel execution settings.

        Returns:
            OrchestratorResult with execution summary

        Raises:
            PipelineError: If orchestrator-level error occurs
        """
        logger.info("=" * 60)
        logger.info("Starting Social Ads Orchestrator")
        logger.info("=" * 60)

        self.monitor.start_orchestrator()
        start_time = time.time()

        try:
            # Get enabled platforms
            enabled_platforms = self.config.get_enabled_platforms()

            if not enabled_platforms:
                logger.warning("No enabled platforms configured")
                return self._create_result(start_time)

            logger.info(f"Enabled platforms: {[p.name for p in enabled_platforms]}")

            # Register all platforms with monitor
            for platform in enabled_platforms:
                self.monitor.register_platform(platform.name)

            # Schedule platforms
            execution_groups = self.scheduler.schedule_platforms(enabled_platforms)

            logger.info(f"Execution plan: {len(execution_groups)} group(s)")
            for i, group in enumerate(execution_groups):
                logger.info(f"  Group {i+1}: {', '.join(group)}")

            # Execute each group
            for group_idx, group in enumerate(execution_groups):
                logger.info(f"\n{'='*60}")
                logger.info(f"Executing Group {group_idx+1}/{len(execution_groups)}: {', '.join(group)}")
                logger.info(f"{'='*60}")

                if self.config.parallel_execution and len(group) > 1:
                    # Execute group in parallel
                    self._run_parallel(group, enabled_platforms)
                else:
                    # Execute group sequentially
                    for platform_name in group:
                        platform_config = self._get_platform_config(platform_name, enabled_platforms)
                        if platform_config:
                            self._run_single_platform(platform_config)

            # Create result
            result = self._create_result(start_time)

            # Print summary
            self.monitor.print_summary()

            return result

        except Exception as e:
            logger.exception(f"Orchestrator failed: {e}")
            raise PipelineError(f"Orchestrator execution failed: {e}") from e

        finally:
            self.monitor.end_orchestrator()

    def run_platform(self, platform_name: str) -> bool:
        """Run a single platform by name.

        Args:
            platform_name: Name of platform to run

        Returns:
            True if platform completed successfully

        Raises:
            ConfigurationError: If platform not configured
        """
        logger.info(f"Running single platform: {platform_name}")

        # Get platform config
        platform_config = self.config.get_platform(platform_name)
        if not platform_config:
            raise ConfigurationError(f"Platform '{platform_name}' not configured")

        if not platform_config.enabled:
            logger.warning(f"Platform '{platform_name}' is not enabled")
            return False

        # Register with monitor
        self.monitor.register_platform(platform_name)

        # Execute platform
        return self._run_single_platform(platform_config)

    def _run_single_platform(self, platform_config: PlatformConfig) -> bool:
        """Execute a single platform with retry logic.

        Args:
            platform_config: Platform configuration

        Returns:
            True if platform completed successfully
        """
        platform_name = platform_config.name
        max_attempts = platform_config.retry.max_attempts

        for attempt in range(1, max_attempts + 1):
            try:
                if attempt > 1:
                    # Calculate backoff time
                    backoff = platform_config.retry.get_backoff_time(attempt - 1)
                    logger.info(
                        f"Retrying platform '{platform_name}' "
                        f"(attempt {attempt}/{max_attempts}) after {backoff}s backoff"
                    )
                    time.sleep(backoff)
                    self.monitor.increment_retry(platform_name)

                # Start platform
                self.monitor.start_platform(platform_name)
                logger.info(f"Starting platform: {platform_name}")

                # Check dependencies
                if not self.scheduler.can_execute(platform_name, self._completed_platforms):
                    missing_deps = [
                        dep for dep in self.scheduler.get_dependencies(platform_name)
                        if dep not in self._completed_platforms
                    ]
                    error_msg = f"Dependencies not met: {', '.join(missing_deps)}"
                    logger.error(error_msg)
                    self.monitor.fail_platform(platform_name, Exception(error_msg))
                    self._failed_platforms.add(platform_name)
                    return False

                # Instantiate pipeline
                pipeline = self.registry.get_pipeline(
                    platform_name,
                    self.token_provider,
                    self.data_sink
                )

                # Run pipeline
                results = pipeline.run_all_tables(load_to_sink=(self.data_sink is not None))

                # Calculate metrics
                total_rows = sum(len(df) for df in results.values() if not df.empty)
                total_tables = len(results)

                # Mark as completed
                self.monitor.complete_platform(
                    platform_name,
                    rows_processed=total_rows,
                    tables_processed=total_tables
                )

                self._completed_platforms.add(platform_name)
                logger.success(
                    f"Platform '{platform_name}' completed: "
                    f"{total_rows:,} rows, {total_tables} tables"
                )

                return True

            except Exception as e:
                logger.error(f"Platform '{platform_name}' failed (attempt {attempt}/{max_attempts}): {e}")

                # If last attempt, mark as failed
                if attempt == max_attempts:
                    self.monitor.fail_platform(platform_name, e)
                    self._failed_platforms.add(platform_name)

                    # Check if we should continue
                    if not self.config.continue_on_failure:
                        logger.error("continue_on_failure=False, stopping orchestrator")
                        raise PipelineError(f"Platform '{platform_name}' failed, stopping execution") from e

                    return False

        return False

    def _run_parallel(
        self,
        platform_names: List[str],
        all_platforms: List[PlatformConfig],
    ) -> None:
        """Execute multiple platforms in parallel.

        Args:
            platform_names: List of platform names to execute
            all_platforms: List of all platform configurations
        """
        max_workers = min(len(platform_names), self.config.max_parallel)
        logger.info(f"Executing {len(platform_names)} platforms in parallel (max workers: {max_workers})")

        # Get platform configs
        platform_configs = [
            self._get_platform_config(name, all_platforms)
            for name in platform_names
        ]

        # Filter out None values
        platform_configs = [p for p in platform_configs if p is not None]

        # Execute in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_platform = {
                executor.submit(self._run_single_platform, config): config.name
                for config in platform_configs
            }

            # Wait for completion
            for future in as_completed(future_to_platform):
                platform_name = future_to_platform[future]
                try:
                    success = future.result()
                    if success:
                        logger.debug(f"Parallel execution completed for: {platform_name}")
                    else:
                        logger.warning(f"Parallel execution failed for: {platform_name}")
                except Exception as e:
                    logger.error(f"Parallel execution error for {platform_name}: {e}")

    def _get_platform_config(
        self,
        platform_name: str,
        platforms: List[PlatformConfig],
    ) -> Optional[PlatformConfig]:
        """Get platform configuration from list.

        Args:
            platform_name: Name of platform
            platforms: List of platform configurations

        Returns:
            PlatformConfig or None if not found
        """
        for platform in platforms:
            if platform.name == platform_name:
                return platform
        return None

    def _create_result(self, start_time: float) -> OrchestratorResult:
        """Create orchestrator result from execution state.

        Args:
            start_time: Start time timestamp

        Returns:
            OrchestratorResult with summary
        """
        summary = self.monitor.get_summary()
        duration = time.time() - start_time

        # Collect error messages
        error_summary = {}
        for execution in self.monitor.get_all_executions():
            if execution.status == ExecutionStatus.FAILED and execution.error_message:
                error_summary[execution.platform_name] = execution.error_message

        # Determine success
        success = (
            summary.failed == 0
            and summary.completed == summary.total_platforms
        )

        return OrchestratorResult(
            success=success,
            completed_platforms=list(self._completed_platforms),
            failed_platforms=list(self._failed_platforms),
            skipped_platforms=[
                e.platform_name for e in self.monitor.get_all_executions()
                if e.status == ExecutionStatus.SKIPPED
            ],
            total_duration_seconds=duration,
            total_rows_processed=summary.total_rows_processed,
            error_summary=error_summary,
        )

    def export_report(self, format: str, output_path: Path) -> None:
        """Export execution report.

        Args:
            format: Export format ('json' or 'csv')
            output_path: Path where to save the report
        """
        self.monitor.export_report(format, output_path)
        logger.success(f"Execution report exported to: {output_path}")
