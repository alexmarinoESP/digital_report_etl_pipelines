#!/usr/bin/env python3
"""
Test script for LinkedIn Demographics API.

This script tests the new demographics functionality:
1. Fetch demographic insights (company, job title, seniority)
2. Lookup organization and title names
3. Validate data structure and completeness

Usage:
    python test_demographics_api.py
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from loguru import logger

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from social.platforms.linkedin.adapter import LinkedInAdapter
from social.platforms.linkedin.vertica_token_provider import VerticaTokenProvider
from social.platforms.linkedin.constants import COMPANY_ACCOUNT_MAP


def setup_logging():
    """Configure logging."""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="DEBUG",
    )


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


def test_company_demographics(adapter: LinkedInAdapter, account_ids: list):
    """Test company demographics API."""
    logger.info("=" * 80)
    logger.info("Testing MEMBER_COMPANY demographics")
    logger.info("=" * 80)

    try:
        # Fetch last 30 days of data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        insights = adapter.get_demographics_insights(
            pivot="MEMBER_COMPANY",
            account_ids=account_ids,
            start_date=start_date,
            end_date=end_date,
            time_granularity="ALL"  # Recommended for demographics
        )

        logger.info(f"Retrieved {len(insights)} company demographic records")

        if insights:
            # Display first record
            logger.info("Sample record:")
            logger.info(insights[0])

            # Extract unique organization IDs
            org_ids = set()
            for record in insights:
                pivot_values = record.get("pivotValues", [])
                for urn in pivot_values:
                    if "urn:li:organization:" in urn:
                        org_id = urn.split(":")[-1]
                        org_ids.add(org_id)

            logger.info(f"Found {len(org_ids)} unique organizations")

            # Lookup organization names (batch - max 100 at a time)
            if org_ids:
                org_ids_list = list(org_ids)[:10]  # Test with first 10
                logger.info(f"Looking up names for {len(org_ids_list)} organizations...")

                org_details = adapter.lookup_organizations(org_ids_list)
                logger.info(f"Retrieved {len(org_details)} organization details")

                # Display organization names
                logger.info("Sample organization names:")
                for org_id, details in list(org_details.items())[:5]:
                    org_name = details.get("localizedName", "N/A")
                    logger.info(f"  - ID {org_id}: {org_name}")

            # Create DataFrame
            df = pd.DataFrame(insights)
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"Columns: {list(df.columns)}")

            return df
        else:
            logger.warning("No company demographic data available")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Company demographics test failed: {e}")
        raise


def test_job_title_demographics(adapter: LinkedInAdapter, account_ids: list):
    """Test job title demographics API."""
    logger.info("=" * 80)
    logger.info("Testing MEMBER_JOB_TITLE demographics")
    logger.info("=" * 80)

    try:
        # Fetch last 30 days of data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        insights = adapter.get_demographics_insights(
            pivot="MEMBER_JOB_TITLE",
            account_ids=account_ids,
            start_date=start_date,
            end_date=end_date,
            time_granularity="ALL"
        )

        logger.info(f"Retrieved {len(insights)} job title demographic records")

        if insights:
            # Display first record
            logger.info("Sample record:")
            logger.info(insights[0])

            # Extract unique title IDs
            title_ids = set()
            for record in insights:
                pivot_values = record.get("pivotValues", [])
                for urn in pivot_values:
                    if "urn:li:title:" in urn:
                        title_id = urn.split(":")[-1]
                        title_ids.add(title_id)

            logger.info(f"Found {len(title_ids)} unique job titles")

            # Lookup title names (batch - max 100 at a time)
            if title_ids:
                title_ids_list = list(title_ids)[:10]  # Test with first 10
                logger.info(f"Looking up names for {len(title_ids_list)} titles...")

                title_details = adapter.lookup_titles(title_ids_list)
                logger.info(f"Retrieved {len(title_details)} title details")

                # Display title names
                logger.info("Sample title names:")
                for title_id, details in list(title_details.items())[:5]:
                    title_name_obj = details.get("name", {})
                    localized = title_name_obj.get("localized", {})
                    # Try to get English name
                    title_name = localized.get("en_US") or list(localized.values())[0] if localized else "N/A"
                    logger.info(f"  - ID {title_id}: {title_name}")

            # Create DataFrame
            df = pd.DataFrame(insights)
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"Columns: {list(df.columns)}")

            return df
        else:
            logger.warning("No job title demographic data available")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Job title demographics test failed: {e}")
        raise


def test_seniority_demographics(adapter: LinkedInAdapter, account_ids: list):
    """Test seniority demographics API."""
    logger.info("=" * 80)
    logger.info("Testing MEMBER_SENIORITY demographics")
    logger.info("=" * 80)

    try:
        # Fetch last 30 days of data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        insights = adapter.get_demographics_insights(
            pivot="MEMBER_SENIORITY",
            account_ids=account_ids,
            start_date=start_date,
            end_date=end_date,
            time_granularity="ALL"
        )

        logger.info(f"Retrieved {len(insights)} seniority demographic records")

        if insights:
            # Display first record
            logger.info("Sample record:")
            logger.info(insights[0])

            # Seniority mapping (reference table)
            seniority_map = {
                "1": "Unpaid",
                "2": "Training",
                "3": "Entry level",
                "4": "Senior",
                "5": "Manager",
                "6": "Director",
                "7": "VP",
                "8": "CXO",
                "9": "Partner",
                "10": "Owner"
            }

            # Display seniority distribution
            logger.info("Seniority distribution:")
            for record in insights[:10]:  # First 10 records
                pivot_values = record.get("pivotValues", [])
                impressions = record.get("impressions", 0)
                if pivot_values:
                    seniority_code = str(pivot_values[0])
                    seniority_name = seniority_map.get(seniority_code, f"Unknown ({seniority_code})")
                    logger.info(f"  - {seniority_name}: {impressions} impressions")

            # Create DataFrame
            df = pd.DataFrame(insights)
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"Columns: {list(df.columns)}")

            return df
        else:
            logger.warning("No seniority demographic data available")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Seniority demographics test failed: {e}")
        raise


def main():
    """Main test function."""
    setup_logging()

    logger.info("LinkedIn Demographics API Test")
    logger.info("=" * 80)

    try:
        # Setup
        token_provider = get_token_provider()
        adapter = LinkedInAdapter(token_provider=token_provider)

        # Get account IDs
        account_ids = list(COMPANY_ACCOUNT_MAP.keys())
        logger.info(f"Testing with {len(account_ids)} accounts: {account_ids}")

        # Test each demographic pivot
        results = {}

        # Test 1: Company demographics
        try:
            results["company"] = test_company_demographics(adapter, account_ids)
        except Exception as e:
            logger.error(f"Company test failed: {e}")
            results["company"] = None

        # Test 2: Job title demographics
        try:
            results["job_title"] = test_job_title_demographics(adapter, account_ids)
        except Exception as e:
            logger.error(f"Job title test failed: {e}")
            results["job_title"] = None

        # Test 3: Seniority demographics
        try:
            results["seniority"] = test_seniority_demographics(adapter, account_ids)
        except Exception as e:
            logger.error(f"Seniority test failed: {e}")
            results["seniority"] = None

        # Summary
        logger.info("=" * 80)
        logger.info("TEST SUMMARY")
        logger.info("=" * 80)

        for name, df in results.items():
            if df is not None and not df.empty:
                logger.success(f"✓ {name.upper()}: {len(df)} records")
            elif df is not None:
                logger.warning(f"⚠ {name.upper()}: No data")
            else:
                logger.error(f"✗ {name.upper()}: Failed")

        # Save results to CSV for inspection
        output_dir = Path("test_output")
        output_dir.mkdir(exist_ok=True)

        for name, df in results.items():
            if df is not None and not df.empty:
                output_file = output_dir / f"linkedin_demographics_{name}_{datetime.now():%Y%m%d_%H%M%S}.csv"
                df.to_csv(output_file, index=False)
                logger.info(f"Saved {name} results to: {output_file}")

        # Cleanup
        adapter.close()

        logger.success("Test completed successfully!")
        return 0

    except Exception as e:
        logger.error(f"Test failed: {e}")
        logger.exception(e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
