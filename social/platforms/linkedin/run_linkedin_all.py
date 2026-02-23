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
    import os
    from datetime import datetime
    from shared.monitoring import ExecutionSummaryWriter

    logger.info("=" * 80)
    logger.info("LinkedIn Unified Pipeline Starting (Ads + Organic Posts)")
    logger.info("=" * 80)

    # Initialize execution summary writer
    summary_writer = ExecutionSummaryWriter(
        platform="linkedin",
        storage_connection_string=os.getenv("SUMMARY_STORAGE_CONNECTION_STRING"),
    )

    pipeline_start = datetime.now()
    ads_success = False
    posts_success = False
    ads_error = None
    posts_error = None

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
            ads_error = f"LinkedIn Ads pipeline failed with exit code: {ads_exit_code}"
            logger.error(ads_error)
    except Exception as e:
        ads_error = f"LinkedIn Ads pipeline crashed: {str(e)}"
        logger.exception(ads_error)

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
            posts_error = f"LinkedIn Organic Posts pipeline failed with exit code: {posts_exit_code}"
            logger.error(posts_error)
    except Exception as e:
        posts_error = f"LinkedIn Organic Posts pipeline crashed: {str(e)}"
        logger.exception(posts_error)

    pipeline_end = datetime.now()

    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("LinkedIn Unified Pipeline Summary")
    logger.info("=" * 80)
    logger.info(f"Ads Pipeline:    {'✓ SUCCESS' if ads_success else '✗ FAILED'}")
    logger.info(f"Posts Pipeline:  {'✓ SUCCESS' if posts_success else '✗ FAILED'}")
    logger.info("=" * 80)

    # Write execution summary
    metadata = {
        "ads_success": ads_success,
        "posts_success": posts_success,
        "sub_pipelines": 2,
    }

    # Determine exit code and write summary
    if ads_success and posts_success:
        logger.success("All LinkedIn pipelines completed successfully")
        # For unified pipeline, write success with metadata about sub-pipelines
        summary_writer.write_success(
            start_time=pipeline_start,
            end_time=pipeline_end,
            tables_processed={},  # Sub-pipelines write their own summaries
            exit_code=0,
            metadata=metadata,
        )
        return 0
    elif ads_success and not posts_success:
        logger.warning("Ads pipeline succeeded but Posts pipeline failed")
        errors = [{"pipeline": "linkedin_posts", "message": posts_error or "Unknown error"}]
        summary_writer.write_partial_success(
            start_time=pipeline_start,
            end_time=pipeline_end,
            tables_succeeded={},
            tables_failed=["linkedin_posts"],
            errors=errors,
            exit_code=2,
            metadata=metadata,
        )
        return 2
    elif not ads_success and posts_success:
        logger.warning("Posts pipeline succeeded but Ads pipeline failed")
        errors = [{"pipeline": "linkedin_ads", "message": ads_error or "Unknown error"}]
        summary_writer.write_partial_success(
            start_time=pipeline_start,
            end_time=pipeline_end,
            tables_succeeded={},
            tables_failed=["linkedin_ads"],
            errors=errors,
            exit_code=1,
            metadata=metadata,
        )
        return 1
    else:
        logger.error("Both LinkedIn pipelines failed")
        errors = [
            {"pipeline": "linkedin_ads", "message": ads_error or "Unknown error"},
            {"pipeline": "linkedin_posts", "message": posts_error or "Unknown error"},
        ]
        summary_writer.write_failure(
            start_time=pipeline_start,
            end_time=pipeline_end,
            error=f"Both sub-pipelines failed: {'; '.join([e['message'] for e in errors])}",
            exit_code=3,
        )
        return 3


if __name__ == "__main__":
    sys.exit(main())
