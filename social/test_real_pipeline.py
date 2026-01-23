"""
Quick test script per testare le pipeline REALI esistenti.

Questo script:
1. Usa le pipeline e classi che esistono GIÀ nel progetto
2. Recupera dati degli ultimi N giorni
3. Scrive su schema di test (aggiunge _test al nome schema)
4. Mostra metriche di esecuzione

Usage:
    python -m social.test_real_pipeline

    # Oppure solo una piattaforma:
    python -m social.test_real_pipeline --platform linkedin

    # Oppure ultimi N giorni:
    python -m social.test_real_pipeline --days 5
"""

import os
import sys
from datetime import datetime, timedelta
import argparse
import yaml

from loguru import logger

# Setup logging (no emoji for Windows compatibility)
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    level="INFO"
)


def test_linkedin(days: int = 7) -> dict:
    """Test LinkedIn pipeline."""
    logger.info("[LinkedIn] Testing LinkedIn Ads...")
    start_time = datetime.now()

    try:
        from social.platforms.linkedin.pipeline import LinkedInPipeline
        from social.infrastructure.file_token_provider import FileBasedTokenProvider
        from social.infrastructure.database import VerticaDataSink
        from social.core.config import DatabaseConfig

        # Date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Load config
        config_path = "social/platforms/linkedin/config_linkedin_ads.yml"
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Setup dependencies
        token_provider = FileBasedTokenProvider(platform="linkedin")

        # Create database config from environment (test_mode aggiunge _TEST suffix alle tabelle)
        db_config = DatabaseConfig.from_env()
        data_sink = VerticaDataSink(config=db_config, test_mode=True)

        # Create and run pipeline
        pipeline = LinkedInPipeline(
            config=config,
            token_provider=token_provider,
            data_sink=data_sink
        )

        # Run pipeline
        pipeline.run(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"[OK] LinkedIn completed in {duration:.2f}s")

        return {
            "platform": "linkedin",
            "success": True,
            "rows": 0,  # Le pipeline reali non ritornano row count direttamente
            "duration": duration,
            "error": None
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[FAIL] LinkedIn failed: {e}")
        return {
            "platform": "linkedin",
            "success": False,
            "rows": 0,
            "duration": duration,
            "error": str(e)
        }


def test_facebook(days: int = 7) -> dict:
    """Test Facebook pipeline."""
    logger.info("[Facebook] Testing Facebook Ads...")
    start_time = datetime.now()

    try:
        from social.platforms.facebook.pipeline import FacebookPipeline
        from social.infrastructure.file_token_provider import FileBasedTokenProvider
        from social.infrastructure.database import VerticaDataSink
        from social.core.config import DatabaseConfig

        # Date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Load config
        config_path = "social/platforms/facebook/config_facebook_ads.yml"
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Load credentials
        cred_path = "social/config/credentials.yml"
        with open(cred_path, "r", encoding="utf-8") as f:
            credentials = yaml.safe_load(f)

        # Setup dependencies
        token_provider = FileBasedTokenProvider(platform="facebook")

        # Create database config from environment
        db_config = DatabaseConfig.from_env()
        data_sink = VerticaDataSink(config=db_config, test_mode=True)

        # Get Facebook-specific parameters
        fb_creds = credentials.get("facebook", {})
        ad_account_ids = fb_creds.get("id_account", [])
        app_id = fb_creds.get("app_id", "")
        app_secret = fb_creds.get("app_secret", "")

        # Create and run pipeline
        pipeline = FacebookPipeline(
            config=config,
            token_provider=token_provider,
            ad_account_ids=ad_account_ids,
            app_id=app_id,
            app_secret=app_secret,
            data_sink=data_sink
        )

        # Run pipeline
        pipeline.run(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"[OK] Facebook completed in {duration:.2f}s")

        return {
            "platform": "facebook",
            "success": True,
            "rows": 0,
            "duration": duration,
            "error": None
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[FAIL] Facebook failed: {e}")
        return {
            "platform": "facebook",
            "success": False,
            "rows": 0,
            "duration": duration,
            "error": str(e)
        }


def test_google(days: int = 7) -> dict:
    """Test Google pipeline."""
    logger.info("[Google] Testing Google Ads...")
    start_time = datetime.now()

    try:
        from social.platforms.google.pipeline import GooglePipeline
        from social.infrastructure.file_token_provider import FileBasedTokenProvider
        from social.infrastructure.database import VerticaDataSink
        from social.core.config import DatabaseConfig
        import os

        # Date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Load config
        config_path = "social/platforms/google/config_google_ads.yml"
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Load credentials
        cred_path = "social/config/credentials.yml"
        with open(cred_path, "r", encoding="utf-8") as f:
            credentials = yaml.safe_load(f)

        # Setup dependencies
        token_provider = FileBasedTokenProvider(platform="google")

        # Create database config from environment
        db_config = DatabaseConfig.from_env()
        data_sink = VerticaDataSink(config=db_config, test_mode=True)

        # Google needs google-ads.yaml config file
        # Use first manager account from credentials
        google_creds = credentials.get("google", {})
        manager_ids = google_creds.get("manager_id", [])
        manager_id = str(manager_ids[0]) if manager_ids else "9474097201"

        # Find google-ads YAML file
        google_config_file = f"social_posts/social_posts/googleads/google-ads-{manager_id}.yml"

        # Create and run pipeline
        pipeline = GooglePipeline(
            config=config,
            token_provider=token_provider,
            google_config_file=google_config_file,
            manager_customer_id=manager_id,
            data_sink=data_sink
        )

        # Run pipeline
        pipeline.run(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"[OK] Google completed in {duration:.2f}s")

        return {
            "platform": "google",
            "success": True,
            "rows": 0,
            "duration": duration,
            "error": None
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[FAIL] Google failed: {e}")
        return {
            "platform": "google",
            "success": False,
            "rows": 0,
            "duration": duration,
            "error": str(e)
        }


def print_summary(results: list[dict], total_duration: float):
    """Print test summary."""
    logger.info("\n" + "="*80)
    logger.info("[DATA] TEST SUMMARY")
    logger.info("="*80)

    successful = 0
    failed = 0

    for result in results:
        status = "[OK]" if result["success"] else "[FAIL]"
        platform = result["platform"].upper()
        duration = result["duration"]

        logger.info(f"{status} {platform:<12} | Duration: {duration:>6.2f}s")

        if result["success"]:
            successful += 1
        else:
            failed += 1
            logger.error(f"   Error: {result['error']}")

    logger.info("="*80)
    logger.info(f"Total Platforms: {len(results)}")
    logger.info(f"[OK] Successful: {successful}")
    logger.info(f"[FAIL] Failed: {failed}")
    logger.info(f"[TIME] Total Duration: {total_duration:.2f}s")
    logger.info("="*80)

    test_schema = os.getenv("VERTICA_SCHEMA", "social_ads") + "_test"
    logger.info(f"\n[SAVE] Data written to TEST schema: {test_schema}")
    logger.info("   Check tables with: SELECT * FROM <schema>.<table_name>")
    logger.info("="*80)


def test_microsoft(days: int = 7) -> dict:
    """Test Microsoft pipeline."""
    logger.info("[Microsoft] Testing Microsoft Ads...")
    start_time = datetime.now()

    try:
        from social.platforms.microsoft.pipeline import MicrosoftPipeline
        from social.infrastructure.file_token_provider import FileBasedTokenProvider
        from social.infrastructure.database import VerticaDataSink
        from social.core.config import DatabaseConfig

        # Date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Load config
        config_path = "social/platforms/microsoft/config_microsoft_ads.yml"
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Setup dependencies
        token_provider = FileBasedTokenProvider(platform="microsoft")

        # Create database config from environment
        db_config = DatabaseConfig.from_env()
        data_sink = VerticaDataSink(config=db_config, test_mode=True)

        # Create and run pipeline
        pipeline = MicrosoftPipeline(
            config=config,
            token_provider=token_provider,
            data_sink=data_sink
        )

        # Run pipeline
        pipeline.run(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"[OK] Microsoft completed in {duration:.2f}s")

        return {
            "platform": "microsoft",
            "success": True,
            "rows": 0,
            "duration": duration,
            "error": None
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[FAIL] Microsoft failed: {e}")
        return {
            "platform": "microsoft",
            "success": False,
            "rows": 0,
            "duration": duration,
            "error": str(e)
        }


def main():
    """Main test runner."""
    parser = argparse.ArgumentParser(description="Quick test for Social Ads pipelines (REAL)")
    parser.add_argument(
        "--platform",
        choices=["linkedin", "facebook", "google", "microsoft", "all"],
        default="all",
        help="Platform to test (default: all)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to fetch (default: 7)"
    )

    args = parser.parse_args()

    logger.info("="*80)
    logger.info("[RUN] SOCIAL ADS PLATFORM - REAL PIPELINE TEST")
    logger.info("="*80)
    logger.info(f"Date range: Last {args.days} days")
    test_schema = os.getenv("VERTICA_SCHEMA", "social_ads") + "_test"
    logger.info(f"Target: TEST schema ({test_schema})")
    logger.info("="*80 + "\n")

    start_time = datetime.now()
    results = []

    # Run tests based on platform selection
    if args.platform == "all":
        results.append(test_linkedin(args.days))
        results.append(test_facebook(args.days))
        results.append(test_google(args.days))
        results.append(test_microsoft(args.days))
    elif args.platform == "linkedin":
        results.append(test_linkedin(args.days))
    elif args.platform == "facebook":
        results.append(test_facebook(args.days))
    elif args.platform == "google":
        results.append(test_google(args.days))
    elif args.platform == "microsoft":
        results.append(test_microsoft(args.days))

    total_duration = (datetime.now() - start_time).total_seconds()

    # Print summary
    print_summary(results, total_duration)

    # Exit code
    failed_count = sum(1 for r in results if not r["success"])
    sys.exit(failed_count)


if __name__ == "__main__":
    main()
