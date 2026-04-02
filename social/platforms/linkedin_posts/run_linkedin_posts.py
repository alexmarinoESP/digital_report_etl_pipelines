#!/usr/bin/env python
"""LinkedIn Organic Posts ETL Runner.

This script provides the entry point for running the LinkedIn Organic Posts
ETL pipeline. It can be run from command line or imported as a module.

Usage:
    # Run all tables
    python -m social.platforms.linkedin_posts.run_linkedin_posts

    # Run specific tables
    python -m social.platforms.linkedin_posts.run_linkedin_posts --tables linkedin_organic_posts

    # Run in test mode (limited data)
    python -m social.platforms.linkedin_posts.run_linkedin_posts --test

Environment Variables:
    LINKEDIN_ACCESS_TOKEN: OAuth2 access token (if not using database)
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from social.core.config import ConfigurationManager
from social.infrastructure.database import VerticaDataSink
from social.infrastructure.vertica_token_provider import VerticaTokenProvider
from social.platforms.linkedin_posts.pipeline import LinkedInPostsPipeline, load_config


def run_pipeline(
    tables: Optional[List[str]] = None,
    test_mode: bool = False,
    max_posts_per_org: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Run the LinkedIn Organic Posts ETL pipeline.

    Args:
        tables: List of specific tables to process (default: all)
        test_mode: If True, use test database and limit data
        max_posts_per_org: Maximum posts per organization
        dry_run: If True, don't write to database

    Returns:
        Dictionary with results and metadata

    Raises:
        Exception: If pipeline execution fails
    """
    logger.info("=" * 60)
    logger.info("LinkedIn Organic Posts ETL Pipeline")
    logger.info("=" * 60)

    # Load configurations
    config_manager = ConfigurationManager()
    db_config = config_manager.load_config(test_mode=test_mode)
    pipeline_config = load_config()

    logger.info(f"Test mode: {test_mode}")
    logger.info(f"Dry run: {dry_run}")

    # Initialize data sink
    data_sink = None
    if not dry_run:
        data_sink = VerticaDataSink(
            config=db_config.database,
            test_mode=test_mode
        )

    # Initialize token provider (reads tokens from Vertica)
    token_provider = VerticaTokenProvider(
        platform="linkedin",
        host=db_config.database.host,
        user=db_config.database.user,
        password=db_config.database.password,
        database=db_config.database.database,
        schema="ESPDM",  # Schema where tokens are stored
    )

    # Initialize pipeline
    pipeline = LinkedInPostsPipeline(
        config=pipeline_config,
        token_provider=token_provider,
        data_sink=data_sink,
    )

    # Set max posts for testing
    if test_mode and max_posts_per_org is None:
        max_posts_per_org = 50  # Limit for test mode

    # Run pipeline
    start_time = datetime.now()
    results_stats, errors = pipeline.run(
        tables=tables,
        max_posts_per_org=max_posts_per_org,
    )
    end_time = datetime.now()

    # Log results
    logger.info("=" * 60)
    logger.info("Pipeline Results:")
    for table_name, stats in results_stats.items():
        if table_name in errors:
            logger.error(f"  {table_name}: FAILED - {errors[table_name]}")
        else:
            logger.info(
                f"  {table_name}: {stats['rows_written']} rows written "
                f"({stats['rows_inserted']} new + {stats['rows_updated']} updated) "
                f"from {stats['rows_from_api']} API rows"
            )
    logger.info("=" * 60)
    logger.info("Pipeline completed successfully!")

    # Clean up
    pipeline.close()

    return {
        "results_stats": results_stats,
        "errors": errors,
        "start_time": start_time,
        "end_time": end_time,
        "metadata": {
            "tables_count": len(results_stats),
            "tables_failed": len(errors),
            "test_mode": test_mode,
            "max_posts_per_org": max_posts_per_org,
        }
    }


def main() -> int:
    """Main entry point for command line execution."""
    parser = argparse.ArgumentParser(
        description="Run LinkedIn Organic Posts ETL Pipeline"
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific tables to process (default: all)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode with limited data",
    )
    parser.add_argument(
        "--max-posts",
        type=int,
        help="Maximum posts per organization",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to database",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    # Configure logging
    if args.debug:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
    else:
        logger.remove()
        logger.add(sys.stderr, level="INFO")

    # Initialize execution summary writer
    from shared.monitoring import ExecutionSummaryWriter

    summary_writer = ExecutionSummaryWriter(
        platform="linkedin_posts",
        storage_connection_string=os.getenv("SUMMARY_STORAGE_CONNECTION_STRING"),
    )

    pipeline_start = datetime.now()

    try:
        pipeline_result = run_pipeline(
            tables=args.tables,
            test_mode=args.test,
            max_posts_per_org=args.max_posts,
            dry_run=args.dry_run,
        )

        # Analyze results to detect failed tables (based on exception tracking)
        errors_dict = pipeline_result["errors"]

        # Separate succeeded and failed tables based on errors dict
        results_stats = pipeline_result.get("results_stats", {})
        tables_succeeded_stats = {name: stats for name, stats in results_stats.items() if name not in errors_dict}
        tables_failed = list(errors_dict.keys())

        # Write appropriate execution summary based on results
        if not tables_failed:
            # All tables succeeded
            summary_writer.write_success(
                start_time=pipeline_result["start_time"],
                end_time=pipeline_result["end_time"],
                tables_stats=results_stats,
                exit_code=0,
                metadata=pipeline_result["metadata"],
            )
            return 0
        elif not tables_succeeded_stats:
            # All tables failed with exceptions
            logger.error("All tables failed to process")
            summary_writer.write_failure(
                start_time=pipeline_result["start_time"],
                end_time=pipeline_result["end_time"],
                error=f"All {len(tables_failed)} tables failed to process",
                exit_code=3,
            )
            return 3
        else:
            # Partial success: some tables succeeded, some failed with exceptions
            logger.warning(f"Partial success: {len(tables_succeeded_stats)}/{len(results_stats)} tables succeeded")
            summary_writer.write_partial_success(
                start_time=pipeline_result["start_time"],
                end_time=pipeline_result["end_time"],
                tables_succeeded_stats=tables_succeeded_stats,
                tables_failed=tables_failed,
                errors=[{"table": name, "message": errors_dict[name]} for name in tables_failed],
                exit_code=3,
                metadata=pipeline_result["metadata"],
            )
            return 3

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        summary_writer.write_failure(
            start_time=pipeline_start,
            end_time=datetime.now(),
            error=e,
            exit_code=1,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
