"""
SMS Campaign ETL Pipeline - Main Entry Point.

This script orchestrates the extraction, enrichment, and loading of
SMS campaign data from MAPP platform into the Vertica database.

Features:
- Extracts SMS campaigns from multiple companies
- Enriches with MAPP statistics (sent, delivered, bounced)
- Extracts Bitly links from SMS text
- Enriches links with click statistics from Bitly API
- Persists all data to Vertica database
- Refreshes click counts for existing campaigns (nightly job)

Usage:
    # Run full pipeline for all companies (new campaigns only)
    python -m sms.run_sms_pipeline

    # Run for specific company
    python -m sms.run_sms_pipeline --company IT

    # Refresh click counts only (no new campaigns, recommended for nightly)
    python -m sms.run_sms_pipeline --refresh-clicks

    # Full job: new campaigns + click refresh (recommended for nightly)
    python -m sms.run_sms_pipeline --with-refresh

    # Refresh clicks for last 30 days only
    python -m sms.run_sms_pipeline --refresh-clicks --refresh-days 30

    # Run without Bitly enrichment
    python -m sms.run_sms_pipeline --no-bitly

    # Look back 5 years
    python -m sms.run_sms_pipeline --years-behind 5

    # Dry run (no database writes)
    python -m sms.run_sms_pipeline --dry-run

Environment Variables:
    VERTICA_HOST: Database host
    VERTICA_PORT: Database port (default: 5433)
    VERTICA_DATABASE: Database name
    VERTICA_USER: Database user
    VERTICA_PASSWORD: Database password
    BITLY_TOKEN: Bitly API token (optional, defaults to embedded token)
"""

import sys
import argparse
from pathlib import Path
from typing import List, Optional

from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sms.pipeline import PipelineFactory, PipelineResult
from sms.domain.models import Company
from sms.domain.interfaces import SMSError
from shared.connection.vertica import VerticaConnection


def configure_logging(verbose: bool = False) -> None:
    """
    Configure loguru logger.

    Args:
        verbose: If True, set DEBUG level, otherwise INFO
    """
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level="DEBUG" if verbose else "INFO",
    )


def parse_company(company_str: str) -> Company:
    """
    Parse company string to Company enum.

    Args:
        company_str: Company name (IT, ES, VVIT)

    Returns:
        Company enum value

    Raises:
        ValueError: If company is invalid
    """
    try:
        return Company[company_str.upper()]
    except KeyError:
        raise ValueError(
            f"Invalid company: {company_str}. Valid options: IT, ES, VVIT"
        )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="SMS Campaign ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline for all companies (new campaigns only)
  python -m sms.run_sms_pipeline

  # Run for specific company
  python -m sms.run_sms_pipeline --company IT

  # NIGHTLY JOB (recommended): new campaigns + refresh clicks
  python -m sms.run_sms_pipeline --with-refresh

  # Refresh click counts only (no new campaigns)
  python -m sms.run_sms_pipeline --refresh-clicks

  # Refresh clicks for last 30 days only
  python -m sms.run_sms_pipeline --refresh-clicks --refresh-days 30

  # Run without Bitly enrichment (faster)
  python -m sms.run_sms_pipeline --no-bitly

  # Look back 5 years instead of default 2
  python -m sms.run_sms_pipeline --years-behind 5

  # Dry run mode (no database writes)
  python -m sms.run_sms_pipeline --dry-run
        """,
    )

    parser.add_argument(
        "--company",
        type=str,
        choices=["IT", "ES", "VVIT"],
        help="Run pipeline for specific company only (default: all)",
    )

    parser.add_argument(
        "--years-behind",
        type=int,
        default=2,
        help="Number of years to look back (default: 2)",
    )

    parser.add_argument(
        "--no-bitly",
        action="store_true",
        help="Skip Bitly enrichment (faster but no click data)",
    )

    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip campaigns that already exist in database (default: True)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run extraction only, don't write to database",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging",
    )

    # Click refresh options
    parser.add_argument(
        "--refresh-clicks",
        action="store_true",
        help="Only refresh click counts for existing campaigns (no new campaigns)",
    )

    parser.add_argument(
        "--with-refresh",
        action="store_true",
        help="Run full pipeline + refresh clicks for existing campaigns",
    )

    parser.add_argument(
        "--refresh-days",
        type=int,
        default=90,
        help="Days to look back for click refresh (default: 90)",
    )

    return parser.parse_args()


def main() -> int:
    """
    Main entry point for SMS pipeline.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    args = parse_args()

    # Configure logging
    configure_logging(verbose=args.verbose)

    logger.info("=" * 70)
    logger.info("SMS Campaign ETL Pipeline")
    logger.info("=" * 70)

    # Parse companies
    companies: Optional[List[Company]] = None
    if args.company:
        try:
            companies = [parse_company(args.company)]
            logger.info(f"Processing company: {args.company}")
        except ValueError as e:
            logger.error(str(e))
            return 1
    else:
        logger.info("Processing all companies: IT, ES, VVIT")

    # Display configuration
    logger.info(f"Years behind: {args.years_behind}")
    logger.info(f"Bitly enrichment: {'Disabled' if args.no_bitly else 'Enabled'}")
    logger.info(f"Skip existing: {args.skip_existing}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(f"Refresh clicks only: {args.refresh_clicks}")
    logger.info(f"With refresh: {args.with_refresh}")
    logger.info(f"Refresh days: {args.refresh_days}")

    try:
        # Create database connection
        logger.info("Connecting to Vertica database...")
        vertica_conn = VerticaConnection()
        connection = vertica_conn.connect()
        logger.info("Database connection established")

        # Create pipeline
        logger.info("Initializing SMS pipeline...")
        pipeline = PipelineFactory.create_default(connection=connection)

        # Run pipeline based on mode
        if args.dry_run:
            logger.info("Running in DRY RUN mode (extraction only)...")
            campaigns, stats = pipeline.run_extraction_only(
                companies=companies,
                years_behind=args.years_behind,
                enrich_bitly=not args.no_bitly,
            )

            logger.info("=" * 70)
            logger.info("Dry Run Results")
            logger.info("=" * 70)
            logger.info(f"Campaigns extracted: {len(campaigns)}")
            logger.info(f"Statistics: {stats}")

            # Show sample data
            if campaigns:
                logger.info("\nSample Campaign:")
                sample = campaigns[0]
                logger.info(f"  Message ID: {sample.message_id}")
                logger.info(f"  Activity ID: {sample.activity_id}")
                logger.info(f"  Campaign Name: {sample.campaign_name}")
                logger.info(f"  Sent Count: {sample.sent_count}")
                logger.info(f"  Bitly Links: {len(sample.bitly_links)}")
                if sample.bitly_links:
                    for link in sample.bitly_links:
                        logger.info(f"    - {link.bitly_short_url}: {link.total_clicks} clicks")

        elif args.refresh_clicks:
            # Only refresh click counts for existing campaigns
            logger.info(f"Running click refresh only (last {args.refresh_days} days)...")
            refresh_stats = pipeline.refresh_clicks(days_back=args.refresh_days)

            logger.info("=" * 70)
            logger.info("Click Refresh Complete")
            logger.info("=" * 70)
            logger.info(f"Campaigns processed: {refresh_stats['campaigns_processed']}")
            logger.info(f"Links updated: {refresh_stats['links_updated']}")
            logger.info(f"Errors: {refresh_stats['errors']}")

            if refresh_stats['errors'] == 0:
                logger.success("Click refresh completed successfully!")
                return 0
            else:
                logger.warning("Click refresh completed with some errors")
                return 1

        else:
            # Run full pipeline (new campaigns)
            logger.info("Running full pipeline...")
            result: PipelineResult = pipeline.run(
                companies=companies,
                years_behind=args.years_behind,
                enrich_bitly=not args.no_bitly,
                skip_existing=args.skip_existing,
            )

            logger.info("=" * 70)
            logger.info("Pipeline Complete")
            logger.info("=" * 70)
            logger.info(str(result))

            # Also refresh clicks if --with-refresh is specified
            if args.with_refresh:
                logger.info("")
                logger.info("=" * 70)
                logger.info(f"Running click refresh (last {args.refresh_days} days)...")
                logger.info("=" * 70)
                refresh_stats = pipeline.refresh_clicks(days_back=args.refresh_days)

                logger.info(f"Click refresh: {refresh_stats['campaigns_processed']} campaigns, "
                           f"{refresh_stats['links_updated']} links updated")

            if result.success:
                logger.success("Pipeline completed successfully!")
                return 0
            else:
                logger.warning("Pipeline completed with failures")
                return 1

        # Close connection
        connection.close()
        logger.info("Database connection closed")

        return 0

    except SMSError as e:
        logger.error(f"SMS Pipeline error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
