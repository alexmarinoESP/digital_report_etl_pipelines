#!/usr/bin/env python
"""Truncate LinkedIn TEST tables in Vertica database.

This script clears all LinkedIn TEST tables to allow fresh data loading.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from social.core.config import ConfigurationManager
from social.infrastructure.database import VerticaDataSink
from loguru import logger

def truncate_linkedin_test_tables():
    """Truncate all LinkedIn TEST tables."""

    # Load config to get database connection details
    config_manager = ConfigurationManager()
    config = config_manager.load_config(test_mode=True)

    if not config.database:
        logger.error("Database configuration not found")
        return 1

    # Create database connection
    data_sink = VerticaDataSink(config=config.database, test_mode=True)

    # LinkedIn TEST tables
    tables = [
        'linkedin_ads_campaign_TEST',
        'linkedin_ads_creative_TEST',
        'linkedin_ads_insights_TEST',
        'linkedin_ads_campaign_audience_TEST',
        'linkedin_ads_account_TEST',
        'linkedin_ads_audience_TEST',
    ]

    logger.info("Starting to truncate LinkedIn TEST tables...")

    try:
        for table in tables:
            full_table_name = f"{config.database.schema}.{table}"
            logger.info(f"Truncating {full_table_name}...")

            with data_sink.conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {full_table_name}")

            logger.success(f"✓ Truncated {table}")

        data_sink.conn.commit()
        logger.success(f"✓ Successfully truncated all {len(tables)} tables")

        return 0

    except Exception as e:
        logger.error(f"Failed to truncate tables: {e}")
        return 1

    finally:
        data_sink.close()


if __name__ == "__main__":
    sys.exit(truncate_linkedin_test_tables())
