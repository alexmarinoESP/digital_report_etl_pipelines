#!/usr/bin/env python3
"""
LinkedIn Unified ETL Pipeline Runner.

Runs both LinkedIn Ads and LinkedIn Organic Posts pipelines sequentially.
Designed for Azure Container App deployment with shared OAuth token.

Features:
- Runs LinkedIn Ads pipeline first
- Runs LinkedIn Organic Posts pipeline second
- Shared token provider (same OAuth token)
- Unified error handling and logging
- Container-ready (no browser interactions)

Environment Variables:
    Required:
    - LINKEDIN_CLIENT_ID: OAuth application client ID
    - LINKEDIN_CLIENT_SECRET: OAuth application client secret
    - LINKEDIN_ACCESS_TOKEN: OAuth access token
    - VERTICA_HOST, VERTICA_PORT, VERTICA_DATABASE
    - VERTICA_USER, VERTICA_PASSWORD

    Optional:
    - LOG_LEVEL: Logging level (default: INFO)
    - TEST_MODE: "true" to write to *_TEST tables (default: false)
    - STORAGE_TYPE: "vertica" (default) or "azure_table"

Exit Codes:
    0: Success (both pipelines)
    1: Configuration error
    2: Authentication error
    3: LinkedIn Ads pipeline failed
    4: LinkedIn Posts pipeline failed
    5: Both pipelines failed
"""

import os
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging with console output."""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=log_level,
        colorize=True,
    )


def run_linkedin_ads() -> bool:
    """
    Run LinkedIn Ads ETL pipeline.

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info("=" * 80)
        logger.info("STARTING LINKEDIN ADS PIPELINE")
        logger.info("=" * 80)

        from social.platforms.linkedin.run_linkedin import main as ads_main

        exit_code = ads_main()

        if exit_code == 0:
            logger.success("LinkedIn Ads pipeline completed successfully")
            return True
        else:
            logger.error(f"LinkedIn Ads pipeline failed with exit code {exit_code}")
            return False

    except Exception as e:
        logger.exception(f"LinkedIn Ads pipeline failed with exception: {e}")
        return False


def run_linkedin_posts() -> bool:
    """
    Run LinkedIn Organic Posts ETL pipeline.

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info("")
        logger.info("=" * 80)
        logger.info("STARTING LINKEDIN ORGANIC POSTS PIPELINE")
        logger.info("=" * 80)

        from social.platforms.linkedin_posts.run_linkedin_posts import run_pipeline

        # Get test mode from environment
        test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        logger.info(f"Test mode: {test_mode}")

        # Run with environment-driven configuration
        run_pipeline(
            tables=None,
            test_mode=test_mode,
            max_posts_per_org=None,
            dry_run=False,
        )

        logger.success("LinkedIn Organic Posts pipeline completed successfully")
        return True

    except Exception as e:
        logger.exception(f"LinkedIn Organic Posts pipeline failed: {e}")
        return False


def main() -> int:
    """
    Main entry point - runs both LinkedIn pipelines sequentially.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Setup logging
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    setup_logging(log_level)

    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("LINKEDIN UNIFIED ETL PIPELINE - Ads + Organic Posts")
    logger.info("=" * 80)
    logger.info(f"Start time: {start_time}")
    logger.info("")

    # Track pipeline results
    ads_success = False
    posts_success = False

    # Run LinkedIn Ads pipeline
    ads_success = run_linkedin_ads()

    # Run LinkedIn Posts pipeline (even if ads failed, try to get posts data)
    posts_success = run_linkedin_posts()

    # Calculate duration
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Final summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"LinkedIn Ads:          {'✓ SUCCESS' if ads_success else '✗ FAILED'}")
    logger.info(f"LinkedIn Organic Posts: {'✓ SUCCESS' if posts_success else '✗ FAILED'}")
    logger.info(f"Total duration:        {duration:.2f}s")
    logger.info(f"End time:              {end_time}")
    logger.info("=" * 80)

    # Determine exit code
    if ads_success and posts_success:
        logger.success("All pipelines completed successfully")
        return 0
    elif not ads_success and not posts_success:
        logger.error("Both pipelines failed")
        return 5
    elif not ads_success:
        logger.warning("LinkedIn Ads pipeline failed, but Posts succeeded")
        return 3
    else:  # not posts_success
        logger.warning("LinkedIn Organic Posts pipeline failed, but Ads succeeded")
        return 4


if __name__ == "__main__":
    sys.exit(main())
