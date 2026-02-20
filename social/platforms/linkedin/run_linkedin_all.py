#!/usr/bin/env python3
"""
LinkedIn Unified Pipeline Runner (Ads + Organic Posts).

This orchestrator runs both LinkedIn Ads and LinkedIn Organic Posts pipelines.
Designed for Azure Container Apps deployment.

Features:
- Sequential execution of Ads and Posts pipelines
- Independent error handling for each pipeline
- Comprehensive logging
- Proper exit codes

Exit Codes:
    0: Both pipelines successful
    1: Ads pipeline failed
    2: Posts pipeline failed
    3: Both pipelines failed
"""

import sys
from pathlib import Path

from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# Import the individual pipeline runners
from social.platforms.linkedin.run_linkedin import main as run_ads_pipeline
from social.platforms.linkedin_posts.run_linkedin_posts import main as run_posts_pipeline


def main() -> int:
    """
    Main orchestrator for LinkedIn unified pipeline.

    Returns:
        Exit code (0 = success, non-zero = error)
    """
    logger.info("=" * 80)
    logger.info("LinkedIn Unified Pipeline Starting (Ads + Organic Posts)")
    logger.info("=" * 80)

    ads_success = False
    posts_success = False

    # Run LinkedIn Ads pipeline
    logger.info("")
    logger.info("=" * 80)
    logger.info("[1/2] Running LinkedIn Ads Pipeline")
    logger.info("=" * 80)
    try:
        ads_exit_code = run_ads_pipeline()
        if ads_exit_code == 0:
            logger.success("LinkedIn Ads pipeline completed successfully")
            ads_success = True
        else:
            logger.error(f"LinkedIn Ads pipeline failed with exit code: {ads_exit_code}")
    except Exception as e:
        logger.exception(f"LinkedIn Ads pipeline crashed: {e}")

    # Run LinkedIn Organic Posts pipeline
    logger.info("")
    logger.info("=" * 80)
    logger.info("[2/2] Running LinkedIn Organic Posts Pipeline")
    logger.info("=" * 80)
    try:
        posts_exit_code = run_posts_pipeline()
        if posts_exit_code == 0:
            logger.success("LinkedIn Organic Posts pipeline completed successfully")
            posts_success = True
        else:
            logger.error(f"LinkedIn Organic Posts pipeline failed with exit code: {posts_exit_code}")
    except Exception as e:
        logger.exception(f"LinkedIn Organic Posts pipeline crashed: {e}")

    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("LinkedIn Unified Pipeline Summary")
    logger.info("=" * 80)
    logger.info(f"Ads Pipeline:    {'✓ SUCCESS' if ads_success else '✗ FAILED'}")
    logger.info(f"Posts Pipeline:  {'✓ SUCCESS' if posts_success else '✗ FAILED'}")
    logger.info("=" * 80)

    # Determine exit code
    if ads_success and posts_success:
        logger.success("All LinkedIn pipelines completed successfully")
        return 0
    elif ads_success and not posts_success:
        logger.warning("Ads pipeline succeeded but Posts pipeline failed")
        return 2
    elif not ads_success and posts_success:
        logger.warning("Posts pipeline succeeded but Ads pipeline failed")
        return 1
    else:
        logger.error("Both LinkedIn pipelines failed")
        return 3


if __name__ == "__main__":
    sys.exit(main())
