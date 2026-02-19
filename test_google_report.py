#!/usr/bin/env python3
"""
Test script for Google Ads Report table only.
Tests column mapping and data extraction for google_ads_report.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from loguru import logger
from social.infrastructure.file_token_provider import FileBasedTokenProvider
from social.platforms.google.pipeline import GooglePipeline, load_config
from social.platforms.google.constants import API_VERSION

# Setup logging
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    level="INFO"
)

def main():
    """Test google_ads_report table extraction."""

    logger.info("=" * 80)
    logger.info("Google Ads Report Table Test")
    logger.info("=" * 80)

    # Configuration
    config_path = Path("social/platforms/google/googleads_config.yml")
    google_config_file = Path("social/platforms/google/google-ads-9474097201.yml")
    manager_customer_id = os.getenv("GOOGLE_MANAGER_CUSTOMER_ID", "9474097201")

    logger.info(f"Config file: {config_path}")
    logger.info(f"Google Ads config: {google_config_file}")
    logger.info(f"Manager Customer ID: {manager_customer_id}")
    logger.info(f"API Version: {API_VERSION}")

    # Load configuration
    logger.info("Loading configuration...")
    config = load_config(config_path)

    # Filter to only google_ads_report table
    if "google_ads_report" not in config:
        logger.error("google_ads_report not found in config!")
        return 1

    # Keep only the report table
    config = {"google_ads_report": config["google_ads_report"]}
    logger.info(f"Testing table: google_ads_report")

    # Initialize token provider (placeholder)
    token_provider = FileBasedTokenProvider(
        platform="google",
        credentials_file=None
    )

    # Initialize pipeline WITHOUT data sink (dry run)
    logger.info("Initializing Google Ads pipeline (DRY RUN - no database writes)...")
    pipeline = GooglePipeline(
        config=config,
        token_provider=token_provider,
        google_config_file=str(google_config_file),
        manager_customer_id=manager_customer_id,
        api_version=API_VERSION,
        data_sink=None,  # No writes
    )

    logger.success("Pipeline initialized")

    # Run extraction (without loading to sink)
    logger.info("=" * 80)
    logger.info("Extracting google_ads_report data...")
    logger.info("=" * 80)

    try:
        results = pipeline.run_all_tables(load_to_sink=False)

        # Analyze results
        if "google_ads_report" in results:
            df = results["google_ads_report"]

            logger.info("=" * 80)
            logger.info("EXTRACTION RESULTS:")
            logger.info("=" * 80)
            logger.info(f"Rows extracted: {len(df)}")
            logger.info(f"Columns: {list(df.columns)}")
            logger.info("")

            # Check for the 4 problematic columns
            problem_cols = ["cost_micros", "average_cpm", "average_cpc", "average_cost"]
            logger.info("Checking problematic columns:")
            for col in problem_cols:
                if col in df.columns:
                    non_null = df[col].notna().sum()
                    null_count = df[col].isna().sum()
                    if non_null > 0:
                        sample_values = df[col].dropna().head(3).tolist()
                        logger.success(f"  ✓ {col}: {non_null} non-null values, sample: {sample_values}")
                    else:
                        logger.warning(f"  ⚠ {col}: ALL NULL ({len(df)} rows)")
                else:
                    logger.error(f"  ✗ {col}: COLUMN MISSING!")

            logger.info("")
            logger.info("Sample data (first 3 rows):")
            print(df.head(3).to_string())

            logger.info("")
            logger.info("Column data types:")
            print(df.dtypes)

        else:
            logger.error("google_ads_report not in results!")
            return 1

        pipeline.close()
        logger.success("Test completed successfully!")
        return 0

    except Exception as e:
        logger.exception(f"Test failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
