"""
Unified platform runner script.

This script runs ALL tables for a given platform, mimicking the behavior
of the old social_posts project where each platform processes all configured tables.

Usage:
    python -m social.run_platform --platform linkedin --days 7
    python -m social.run_platform --platform facebook --days 5
    python -m social.run_platform --platform google --days 30
    python -m social.run_platform --platform all --days 7
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import yaml
from loguru import logger

# Setup logging (no emoji for Windows compatibility)
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    level="INFO"
)


def run_linkedin(days: int = 7, test_mode: bool = True) -> Dict[str, Any]:
    """
    Run LinkedIn pipeline for ALL tables.

    Args:
        days: Number of days to fetch
        test_mode: If True, write to test schema

    Returns:
        Dict with execution results
    """
    logger.info("="*80)
    logger.info("[LinkedIn] Starting LinkedIn Ads pipeline for ALL tables")
    logger.info("="*80)

    start_time = datetime.now()
    results = {
        "platform": "linkedin",
        "tables_processed": [],
        "tables_failed": [],
        "total_duration": 0,
        "success": False
    }

    try:
        from social.platforms.linkedin.pipeline import LinkedInPipeline
        from social.infrastructure.vertica_token_provider import VerticaTokenProvider
        from social.infrastructure.database import VerticaDataSink
        from social.core.config import DatabaseConfig

        # Date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Load config
        config_path = Path("social/platforms/linkedin/config_linkedin_ads.yml")
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Setup dependencies - use VerticaTokenProvider for LinkedIn (like old project)
        token_provider = VerticaTokenProvider.from_env(platform="linkedin")
        db_config = DatabaseConfig.from_env()
        data_sink = VerticaDataSink(config=db_config, test_mode=test_mode)

        # Create pipeline
        pipeline = LinkedInPipeline(
            config=config,
            token_provider=token_provider,
            data_sink=data_sink
        )

        # Get all table names from config (exclude 'platform' key)
        table_names = [k for k in config.keys() if k != "platform"]
        logger.info(f"Found {len(table_names)} tables to process: {', '.join(table_names)}")

        # Run pipeline for each table
        for table_name in table_names:
            logger.info(f"\n[LinkedIn] Processing table: {table_name}")
            try:
                pipeline.run(
                    table_name=table_name,
                    start_date=start_date,
                    end_date=end_date,
                    load_to_sink=True
                )
                results["tables_processed"].append(table_name)
                logger.info(f"[OK] Table {table_name} completed")
            except Exception as e:
                logger.error(f"[FAIL] Table {table_name} failed: {e}")
                results["tables_failed"].append({"table": table_name, "error": str(e)})

        duration = (datetime.now() - start_time).total_seconds()
        results["total_duration"] = duration
        results["success"] = len(results["tables_failed"]) == 0

        logger.info("="*80)
        logger.info(f"[LinkedIn] Summary: {len(results['tables_processed'])}/{len(table_names)} tables succeeded")
        logger.info(f"[LinkedIn] Duration: {duration:.2f}s")
        logger.info("="*80)

        return results

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[FAIL] LinkedIn pipeline failed: {e}")
        results["total_duration"] = duration
        results["error"] = str(e)
        return results


def run_facebook(days: int = 7, test_mode: bool = True) -> Dict[str, Any]:
    """
    Run Facebook pipeline for ALL tables and ALL accounts.

    Args:
        days: Number of days to fetch
        test_mode: If True, write to test schema

    Returns:
        Dict with execution results
    """
    logger.info("="*80)
    logger.info("[Facebook] Starting Facebook Ads pipeline for ALL tables")
    logger.info("="*80)

    start_time = datetime.now()
    results = {
        "platform": "facebook",
        "tables_processed": [],
        "tables_failed": [],
        "total_duration": 0,
        "success": False
    }

    try:
        from social.platforms.facebook.pipeline import FacebookPipeline
        from social.infrastructure.file_token_provider import FileBasedTokenProvider
        from social.infrastructure.database import VerticaDataSink
        from social.core.config import DatabaseConfig

        # Date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Load config
        config_path = Path("social/platforms/facebook/config_facebook_ads.yml")
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Load credentials
        cred_path = Path("social/config/credentials.yml")
        with open(cred_path, "r", encoding="utf-8") as f:
            credentials = yaml.safe_load(f)

        # Setup dependencies
        token_provider = FileBasedTokenProvider(platform="facebook")
        db_config = DatabaseConfig.from_env()
        data_sink = VerticaDataSink(config=db_config, test_mode=test_mode)

        # Get Facebook-specific parameters
        fb_creds = credentials.get("facebook", {})
        ad_account_ids = fb_creds.get("id_account", [])
        app_id = fb_creds.get("app_id", "")
        app_secret = fb_creds.get("app_secret", "")

        logger.info(f"Will process {len(ad_account_ids)} Facebook ad accounts")

        # Create pipeline
        pipeline = FacebookPipeline(
            config=config,
            token_provider=token_provider,
            ad_account_ids=ad_account_ids,
            app_id=app_id,
            app_secret=app_secret,
            data_sink=data_sink
        )

        # Get all table names from config (exclude 'platform' key)
        table_names = [k for k in config.keys() if k != "platform"]
        logger.info(f"Found {len(table_names)} tables to process: {', '.join(table_names)}")

        # Run pipeline for each table
        for table_name in table_names:
            logger.info(f"\n[Facebook] Processing table: {table_name}")
            try:
                pipeline.run(
                    table_name=table_name,
                    start_date=start_date,
                    end_date=end_date,
                    load_to_sink=True
                )
                results["tables_processed"].append(table_name)
                logger.info(f"[OK] Table {table_name} completed")
            except Exception as e:
                logger.error(f"[FAIL] Table {table_name} failed: {e}")
                results["tables_failed"].append({"table": table_name, "error": str(e)})

        duration = (datetime.now() - start_time).total_seconds()
        results["total_duration"] = duration
        results["success"] = len(results["tables_failed"]) == 0

        logger.info("="*80)
        logger.info(f"[Facebook] Summary: {len(results['tables_processed'])}/{len(table_names)} tables succeeded")
        logger.info(f"[Facebook] Duration: {duration:.2f}s")
        logger.info("="*80)

        return results

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[FAIL] Facebook pipeline failed: {e}")
        results["total_duration"] = duration
        results["error"] = str(e)
        return results


def run_google(days: int = 7, test_mode: bool = True, manager_id: str = None) -> Dict[str, Any]:
    """
    Run Google Ads pipeline for ALL tables.

    Args:
        days: Number of days to fetch
        test_mode: If True, write to test schema
        manager_id: Manager customer ID (defaults to first from credentials)

    Returns:
        Dict with execution results
    """
    logger.info("="*80)
    logger.info("[Google] Starting Google Ads pipeline for ALL tables")
    logger.info("="*80)

    start_time = datetime.now()
    results = {
        "platform": "google",
        "tables_processed": [],
        "tables_failed": [],
        "total_duration": 0,
        "success": False
    }

    try:
        from social.platforms.google.pipeline import GooglePipeline
        from social.infrastructure.file_token_provider import FileBasedTokenProvider
        from social.infrastructure.database import VerticaDataSink
        from social.core.config import DatabaseConfig

        # Date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Load config
        config_path = Path("social/platforms/google/config_google_ads.yml")
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Load credentials
        cred_path = Path("social/config/credentials.yml")
        with open(cred_path, "r", encoding="utf-8") as f:
            credentials = yaml.safe_load(f)

        # Setup dependencies
        token_provider = FileBasedTokenProvider(platform="google")
        db_config = DatabaseConfig.from_env()
        data_sink = VerticaDataSink(config=db_config, test_mode=test_mode)

        # Get manager ID - use 9474097201 (working one from old project)
        google_creds = credentials.get("google", {})
        manager_ids = google_creds.get("manager_id", [])
        if manager_id is None:
            # Use second manager ID (9474097201) which is the working one
            manager_id = str(manager_ids[1]) if len(manager_ids) > 1 else "9474097201"

        # Find google-ads YAML file (old project location)
        google_config_file = f"social_posts/social_posts/googleads/google-ads-{manager_id}.yml"
        if not Path(google_config_file).exists():
            raise FileNotFoundError(f"Google Ads config file not found: {google_config_file}")

        logger.info(f"Using Google Ads config: {google_config_file}")
        logger.info(f"Manager account: {manager_id} (using working manager from old project)")

        # Create pipeline
        pipeline = GooglePipeline(
            config=config,
            token_provider=token_provider,
            google_config_file=google_config_file,
            manager_customer_id=manager_id,
            data_sink=data_sink
        )

        # Get all table names from config (exclude 'platform' key)
        table_names = [k for k in config.keys() if k != "platform"]
        logger.info(f"Found {len(table_names)} tables to process: {', '.join(table_names)}")

        # Run pipeline for each table
        for table_name in table_names:
            logger.info(f"\n[Google] Processing table: {table_name}")
            try:
                pipeline.run(
                    table_name=table_name,
                    start_date=start_date,
                    end_date=end_date,
                    load_to_sink=True
                )
                results["tables_processed"].append(table_name)
                logger.info(f"[OK] Table {table_name} completed")
            except Exception as e:
                logger.error(f"[FAIL] Table {table_name} failed: {e}")
                results["tables_failed"].append({"table": table_name, "error": str(e)})

        duration = (datetime.now() - start_time).total_seconds()
        results["total_duration"] = duration
        results["success"] = len(results["tables_failed"]) == 0

        logger.info("="*80)
        logger.info(f"[Google] Summary: {len(results['tables_processed'])}/{len(table_names)} tables succeeded")
        logger.info(f"[Google] Duration: {duration:.2f}s")
        logger.info("="*80)

        return results

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[FAIL] Google pipeline failed: {e}")
        results["total_duration"] = duration
        results["error"] = str(e)
        return results


def run_microsoft(days: int = 7, test_mode: bool = True) -> Dict[str, Any]:
    """
    Run Microsoft Ads pipeline for ALL tables.

    Args:
        days: Number of days to fetch
        test_mode: If True, write to test schema

    Returns:
        Dict with execution results
    """
    logger.info("="*80)
    logger.info("[Microsoft] Starting Microsoft Ads pipeline for ALL tables")
    logger.info("="*80)

    start_time = datetime.now()
    results = {
        "platform": "microsoft",
        "tables_processed": [],
        "tables_failed": [],
        "total_duration": 0,
        "success": False,
        "error": "Microsoft Ads implementation requires bingads SDK (not installed)"
    }

    logger.warning("[Microsoft] BingAds SDK not installed - skipping")
    results["total_duration"] = (datetime.now() - start_time).total_seconds()

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run social ads platforms - processes ALL tables for each platform"
    )
    parser.add_argument(
        "--platform",
        choices=["linkedin", "facebook", "google", "microsoft", "all"],
        default="all",
        help="Platform to run (default: all)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to fetch (default: 7)"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        default=True,
        help="Run in test mode (writes to test schema)"
    )
    parser.add_argument(
        "--production",
        action="store_true",
        help="Run in production mode (writes to production schema)"
    )

    args = parser.parse_args()

    # If --production is specified, disable test mode
    test_mode = not args.production

    logger.info("="*80)
    logger.info("[RUN] SOCIAL ADS PLATFORM - UNIFIED RUNNER")
    logger.info("="*80)
    logger.info(f"Platform: {args.platform}")
    logger.info(f"Days: {args.days}")
    logger.info(f"Mode: {'TEST' if test_mode else 'PRODUCTION'}")
    schema = os.getenv("VERTICA_SCHEMA", "GoogleAnalytics")
    # Note: Schema is ALWAYS the same (GoogleAnalytics)
    # In test mode, only table names get _TEST suffix (e.g., linkedin_ads_campaign_TEST)
    logger.info(f"Target schema: {schema}")
    if test_mode:
        logger.info(f"Table suffix: _TEST (e.g., linkedin_ads_campaign_TEST)")
    logger.info("="*80 + "\n")

    all_results = []
    overall_start = datetime.now()

    # Run platforms
    if args.platform == "all":
        all_results.append(run_linkedin(args.days, test_mode))
        all_results.append(run_facebook(args.days, test_mode))
        all_results.append(run_google(args.days, test_mode))
        all_results.append(run_microsoft(args.days, test_mode))
    elif args.platform == "linkedin":
        all_results.append(run_linkedin(args.days, test_mode))
    elif args.platform == "facebook":
        all_results.append(run_facebook(args.days, test_mode))
    elif args.platform == "google":
        all_results.append(run_google(args.days, test_mode))
    elif args.platform == "microsoft":
        all_results.append(run_microsoft(args.days, test_mode))

    overall_duration = (datetime.now() - overall_start).total_seconds()

    # Print summary
    logger.info("\n" + "="*80)
    logger.info("[SUMMARY] OVERALL RESULTS")
    logger.info("="*80)

    total_platforms = len(all_results)
    successful_platforms = sum(1 for r in all_results if r.get("success", False))
    failed_platforms = total_platforms - successful_platforms

    for result in all_results:
        platform = result["platform"].upper()
        tables_ok = len(result.get("tables_processed", []))
        tables_fail = len(result.get("tables_failed", []))
        duration = result.get("total_duration", 0)

        if result.get("success"):
            logger.info(f"[OK] {platform:<12} | Tables: {tables_ok:>2} OK, {tables_fail:>2} FAIL | Duration: {duration:>6.2f}s")
        else:
            logger.info(f"[FAIL] {platform:<12} | Error: {result.get('error', 'Unknown error')[:50]}")

    logger.info("="*80)
    logger.info(f"Platforms: {successful_platforms}/{total_platforms} succeeded")
    logger.info(f"Total duration: {overall_duration:.2f}s")
    logger.info("="*80)

    # Exit code
    sys.exit(failed_platforms)


if __name__ == "__main__":
    main()
