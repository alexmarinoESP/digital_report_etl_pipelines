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
import sys
from pathlib import Path
from typing import List, Optional

from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from social.core.config import ConfigurationManager
from social.infrastructure.database import VerticaDataSink
from social.infrastructure.database_token_provider import DatabaseTokenProvider
from social.platforms.linkedin_posts.pipeline import LinkedInPostsPipeline, load_config


def run_pipeline(
    tables: Optional[List[str]] = None,
    test_mode: bool = False,
    max_posts_per_org: Optional[int] = None,
    dry_run: bool = False,
) -> None:
    """Run the LinkedIn Organic Posts ETL pipeline.

    Args:
        tables: List of specific tables to process (default: all)
        test_mode: If True, use test database and limit data
        max_posts_per_org: Maximum posts per organization
        dry_run: If True, don't write to database
    """
    logger.info("=" * 60)
    logger.info("LinkedIn Organic Posts ETL Pipeline")
    logger.info("=" * 60)

    try:
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

        # Initialize token provider
        token_provider = DatabaseTokenProvider(
            platform="linkedin",
            data_sink=VerticaDataSink(config=db_config.database, test_mode=test_mode)
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
        results = pipeline.run(
            tables=tables,
            max_posts_per_org=max_posts_per_org,
        )

        # Log results
        logger.info("=" * 60)
        logger.info("Pipeline Results:")
        for table_name, df in results.items():
            logger.info(f"  {table_name}: {len(df)} rows")
        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")

        # Clean up
        pipeline.close()

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


def main():
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

    run_pipeline(
        tables=args.tables,
        test_mode=args.test,
        max_posts_per_org=args.max_posts,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
