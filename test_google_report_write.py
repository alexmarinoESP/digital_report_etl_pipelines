#!/usr/bin/env python3
"""
Test script for Google Ads Report - WITH DATABASE WRITE.
Tests the full pipeline including write to database.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from loguru import logger
from social.core.config import DatabaseConfig
from social.infrastructure.database import VerticaDataSink
from social.infrastructure.file_token_provider import FileBasedTokenProvider
from social.platforms.google.pipeline import GooglePipeline, load_config
from social.platforms.google.constants import API_VERSION

# Setup logging
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    level="DEBUG"  # Debug mode to see all details
)

def main():
    """Test google_ads_report table with database write."""

    logger.info("=" * 80)
    logger.info("Google Ads Report Table Test - WITH DATABASE WRITE")
    logger.info("=" * 80)

    # Configuration
    config_path = Path("social/platforms/google/googleads_config.yml")
    google_config_file = Path("social/platforms/google/google-ads-9474097201.yml")
    manager_customer_id = os.getenv("GOOGLE_MANAGER_CUSTOMER_ID", "9474097201")

    # Database config
    db_config = DatabaseConfig(
        host=os.getenv("VERTICA_HOST"),
        user=os.getenv("VERTICA_USER"),
        password=os.getenv("VERTICA_PASSWORD"),
        database=os.getenv("VERTICA_DATABASE"),
        port=int(os.getenv("VERTICA_PORT", "5433")),
        schema=os.getenv("VERTICA_SCHEMA", "GoogleAnalytics"),
    )

    logger.info(f"Database: {db_config.host}:{db_config.port}/{db_config.database}")
    logger.info(f"Schema: {db_config.schema}")
    logger.info(f"Manager Customer ID: {manager_customer_id}")
    logger.info(f"API Version: {API_VERSION}")

    # Load configuration
    logger.info("Loading configuration...")
    config = load_config(config_path)

    # Filter to only google_ads_report table
    config = {"google_ads_report": config["google_ads_report"]}
    logger.info("Testing table: google_ads_report")

    # Initialize token provider
    token_provider = FileBasedTokenProvider(
        platform="google",
        credentials_file=None
    )

    # Initialize Vertica sink in TEST MODE
    logger.info("Initializing Vertica data sink (TEST_MODE=true)...")
    data_sink = VerticaDataSink(config=db_config, test_mode=True)

    # Initialize pipeline
    logger.info("Initializing Google Ads pipeline...")
    pipeline = GooglePipeline(
        config=config,
        token_provider=token_provider,
        google_config_file=str(google_config_file),
        manager_customer_id=manager_customer_id,
        api_version=API_VERSION,
        data_sink=data_sink,
    )

    logger.success("Pipeline initialized")

    # Run extraction AND load
    logger.info("=" * 80)
    logger.info("Running pipeline with database write...")
    logger.info("=" * 80)

    try:
        results = pipeline.run_all_tables(load_to_sink=True)

        # Analyze results
        if "google_ads_report" in results:
            df = results["google_ads_report"]

            logger.info("=" * 80)
            logger.info("PIPELINE RESULTS:")
            logger.info("=" * 80)
            logger.info(f"Rows processed: {len(df)}")
            logger.info(f"Columns in DataFrame: {list(df.columns)}")

            # Check for the 4 problematic columns
            problem_cols = ["cost_micros", "average_cpm", "average_cpc", "average_cost"]
            logger.info("")
            logger.info("Checking problematic columns BEFORE database write:")
            for col in problem_cols:
                if col in df.columns:
                    non_null = df[col].notna().sum()
                    logger.info(f"  {col}: {non_null} non-null values")
                else:
                    logger.error(f"  {col}: MISSING!")

            logger.success("Check database to see if columns were written correctly")
            logger.info("Query: SELECT TOP 5 * FROM GoogleAnalytics.google_ads_report_TEST ORDER BY date DESC;")

        else:
            logger.error("google_ads_report not in results!")
            return 1

        pipeline.close()
        logger.success("Test completed!")
        return 0

    except Exception as e:
        logger.exception(f"Test failed: {e}")
        return 1


if __name__ == "__main__":
    # Set environment variables
    os.environ["TEST_MODE"] = "true"
    os.environ["STORAGE_TYPE"] = "vertica"

    sys.exit(main())
