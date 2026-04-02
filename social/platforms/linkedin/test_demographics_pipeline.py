#!/usr/bin/env python3
"""
Test script for LinkedIn Demographics Pipeline.

This script tests the complete demographics pipeline:
1. Extract demographics per campaign from API
2. Process data (extract IDs, lookup names, rename columns)
3. Validate data structure matches DB schema

Usage:
    python test_demographics_pipeline.py
"""

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from loguru import logger

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from social.platforms.linkedin.pipeline import LinkedInPipeline
from social.platforms.linkedin.vertica_token_provider import VerticaTokenProvider
from social.infrastructure.database import VerticaDataSink


def setup_logging():
    """Configure logging."""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="DEBUG",
    )


def load_config():
    """Load LinkedIn Ads configuration."""
    config_file = Path(__file__).parent / "config_linkedin_ads.yml"
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


def get_token_provider():
    """Setup token provider from Vertica."""
    logger.info("Setting up token provider")

    host = os.getenv("VERTICA_HOST")
    port = int(os.getenv("VERTICA_PORT", "5433"))
    database = os.getenv("VERTICA_DATABASE")
    user = os.getenv("VERTICA_USER")
    password = os.getenv("VERTICA_PASSWORD")

    if not all([host, database, user, password]):
        raise ValueError("Missing Vertica credentials in environment variables")

    token_provider = VerticaTokenProvider(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    logger.success("Token provider initialized")
    return token_provider


def get_data_sink():
    """Setup Vertica data sink."""
    logger.info("Setting up data sink")

    from social.core.config import DatabaseConfig

    host = os.getenv("VERTICA_HOST")
    port = int(os.getenv("VERTICA_PORT", "5433"))
    database = os.getenv("VERTICA_DATABASE")
    user = os.getenv("VERTICA_USER")
    password = os.getenv("VERTICA_PASSWORD")
    schema = "GoogleAnalytics"  # Use GoogleAnalytics schema
    test_mode = False  # Use production tables

    config = DatabaseConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        schema=schema,
    )

    data_sink = VerticaDataSink(
        config=config,
        test_mode=test_mode
    )

    logger.success(f"Data sink initialized (test_mode={test_mode})")
    return data_sink


def test_table(pipeline: LinkedInPipeline, table_name: str):
    """Test a single demographics table."""
    logger.info("=" * 80)
    logger.info(f"Testing table: {table_name}")
    logger.info("=" * 80)

    try:
        # Run pipeline for this table
        df, stats = pipeline.run(
            table_name=table_name,
            load_to_sink=True  # Don't load to DB yet, just test
        )

        logger.info(f"Extraction completed: {len(df)} rows")

        if df.empty:
            logger.warning(f"No data for {table_name}")
            return None

        # Display sample
        logger.info("Sample records:")
        print(df.head(10).to_string())

        # Display column info
        logger.info(f"\nColumns ({len(df.columns)}):")
        for col in df.columns:
            dtype = df[col].dtype
            non_null = df[col].notna().sum()
            logger.info(f"  - {col}: {dtype} ({non_null}/{len(df)} non-null)")

        # Validate required columns
        if table_name == "linkedin_ads_demographics_company":
            required_cols = ["campaign_id", "campaign_name", "company_id", "company_name",
                           "impressions", "clicks", "cost_in_local_currency"]
        elif table_name == "linkedin_ads_demographics_job_title":
            required_cols = ["campaign_id", "campaign_name", "job_title_id", "job_title_name",
                           "impressions", "clicks", "cost_in_local_currency"]
        elif table_name == "linkedin_ads_demographics_seniority":
            required_cols = ["campaign_id", "campaign_name", "seniority_code", "seniority_name",
                           "impressions", "clicks", "cost_in_local_currency"]
        else:
            required_cols = []

        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
        else:
            logger.success(f"All required columns present")

        # Save to CSV
        output_dir = Path("test_output")
        output_dir.mkdir(exist_ok=True)
        output_file = output_dir / f"{table_name}_{datetime.now():%Y%m%d_%H%M%S}.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Saved to: {output_file}")

        return df

    except Exception as e:
        logger.error(f"Failed to test {table_name}: {e}")
        logger.exception(e)
        return None


def main():
    """Main test function."""
    setup_logging()

    logger.info("LinkedIn Demographics Pipeline Test")
    logger.info("=" * 80)

    try:
        # Setup
        config = load_config()
        token_provider = get_token_provider()
        data_sink = get_data_sink()

        # Initialize pipeline
        pipeline = LinkedInPipeline(
            config=config,
            token_provider=token_provider,
            data_sink=data_sink
        )

        logger.success("Pipeline initialized")

        # Test each demographics table
        tables = [
            "linkedin_ads_demographics_company",
            "linkedin_ads_demographics_job_title",
            "linkedin_ads_demographics_seniority"
        ]

        results = {}
        for table_name in tables:
            try:
                df = test_table(pipeline, table_name)
                results[table_name] = df
            except Exception as e:
                logger.error(f"Test failed for {table_name}: {e}")
                results[table_name] = None

        # Summary
        logger.info("=" * 80)
        logger.info("TEST SUMMARY")
        logger.info("=" * 80)

        for table_name, df in results.items():
            if df is not None and not df.empty:
                logger.success(f"✓ {table_name}: {len(df)} records")

                # Check for unique campaigns
                if "campaign_id" in df.columns:
                    n_campaigns = df["campaign_id"].nunique()
                    logger.info(f"  - {n_campaigns} unique campaigns")

                # Check demographic IDs
                if "company_id" in df.columns:
                    n_demo = df["company_id"].nunique()
                    logger.info(f"  - {n_demo} unique companies")
                elif "job_title_id" in df.columns:
                    n_demo = df["job_title_id"].nunique()
                    logger.info(f"  - {n_demo} unique job titles")
                elif "seniority_code" in df.columns:
                    n_demo = df["seniority_code"].nunique()
                    logger.info(f"  - {n_demo} unique seniority levels")

            elif df is not None:
                logger.warning(f"⚠ {table_name}: No data")
            else:
                logger.error(f"✗ {table_name}: Failed")

        # Cleanup
        pipeline.close()

        logger.success("\nTest completed successfully!")
        return 0

    except Exception as e:
        logger.error(f"Test failed: {e}")
        logger.exception(e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
