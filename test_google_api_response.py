"""Test script to check actual Google Ads API response structure."""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from google.protobuf.json_format import MessageToDict
import pandas as pd
from loguru import logger

# Import Google Ads client
from social.platforms.google.http_client import GoogleAdsClient
from social.infrastructure.file_token_provider import FileBasedTokenProvider

def test_audience_response():
    """Test audience query response structure."""
    logger.info("Testing audience API response...")

    # Initialize token provider (dummy)
    token_provider = FileBasedTokenProvider(platform="google", credentials_file=None)

    # Initialize client
    client = GoogleAdsClient(
        token_provider=token_provider,
        config_file_path="social/platforms/google/google-ads-9474097201.yml",
        api_version="v19",
        manager_customer_id="9474097201"
    )

    # Get one test account
    accounts = client.get_all_accounts()
    test_account = None
    for acc in accounts:
        if acc.get('status') == 'ENABLED' and not acc.get('manager'):
            test_account = acc['id']
            break

    if not test_account:
        logger.error("No enabled customer accounts found")
        return

    logger.info(f"Using test account: {test_account}")

    # Test audience query
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    query = f"""
    SELECT
        ad_group_criterion.criterion_id,
        ad_group.id,
        ad_group_criterion.display_name,
        customer.id
    FROM ad_group_criterion
    WHERE segments.date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
    AND campaign.status = 'ENABLED'
    AND ad_group_criterion.type IN ('USER_LIST', 'USER_INTEREST', 'AUDIENCE')
    """

    logger.info(f"Query: {query}")

    try:
        df = client.execute_query(
            customer_id=str(test_account),
            query=query,
            use_streaming=False
        )

        if df.empty:
            logger.warning("No audience data returned")
        else:
            logger.success(f"Retrieved {len(df)} rows")
            logger.info(f"Columns: {df.columns.tolist()}")
            logger.info(f"First row:\n{df.iloc[0].to_dict()}")

            # Print all column names and sample values
            for col in df.columns:
                sample = df[col].iloc[0] if len(df) > 0 else None
                logger.info(f"Column '{col}': {type(sample).__name__} = {sample}")

    except Exception as e:
        logger.error(f"Error: {e}")

def test_placement_response():
    """Test placement query response structure."""
    logger.info("Testing placement API response...")

    # Initialize token provider (dummy)
    token_provider = FileBasedTokenProvider(platform="google", credentials_file=None)

    # Initialize client
    client = GoogleAdsClient(
        token_provider=token_provider,
        config_file_path="social/platforms/google/google-ads-9474097201.yml",
        api_version="v19",
        manager_customer_id="9474097201"
    )

    # Get one test account
    accounts = client.get_all_accounts()
    test_account = None
    for acc in accounts:
        if acc.get('status') == 'ENABLED' and not acc.get('manager'):
            test_account = acc['id']
            break

    if not test_account:
        logger.error("No enabled customer accounts found")
        return

    logger.info(f"Using test account: {test_account}")

    # Test placement query
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    query = f"""
    SELECT
        ad_group.id,
        group_placement_view.placement,
        group_placement_view.placement_type,
        group_placement_view.display_name,
        group_placement_view.target_url,
        metrics.impressions,
        metrics.active_view_ctr,
        customer.id
    FROM group_placement_view
    WHERE segments.date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
    AND campaign.status = 'ENABLED'
    """

    logger.info(f"Query: {query}")

    try:
        df = client.execute_query(
            customer_id=str(test_account),
            query=query,
            use_streaming=False
        )

        if df.empty:
            logger.warning("No placement data returned")
        else:
            logger.success(f"Retrieved {len(df)} rows")
            logger.info(f"Columns: {df.columns.tolist()}")
            logger.info(f"First row:\n{df.iloc[0].to_dict()}")

            # Print all column names and sample values
            for col in df.columns:
                sample = df[col].iloc[0] if len(df) > 0 else None
                logger.info(f"Column '{col}': {type(sample).__name__} = {sample}")

    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Testing Google Ads API Response Structures")
    logger.info("=" * 80)

    test_audience_response()
    print("\n" + "=" * 80 + "\n")
    test_placement_response()
