#!/usr/bin/env python
"""Sync LinkedIn TEST tables with production data in Vertica database.

This script:
1. Truncates all LinkedIn TEST tables
2. Copies data from production tables to TEST tables

This allows testing individual tables while having dependencies already populated.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from social.core.config import ConfigurationManager
from social.infrastructure.database import VerticaDataSink
from loguru import logger


# LinkedIn tables (without _TEST suffix)
LINKEDIN_TABLES = [
    'linkedin_ads_account',
    'linkedin_ads_campaign',
    'linkedin_ads_audience',
    'linkedin_ads_campaign_audience',
    'linkedin_ads_insights',
    'linkedin_ads_creative',
]


def sync_linkedin_test_tables():
    """Truncate and copy production data to LinkedIn TEST tables."""

    # Load config to get database connection details
    config_manager = ConfigurationManager()
    config = config_manager.load_config(test_mode=True)

    if not config.database:
        logger.error("Database configuration not found")
        return 1

    # Create database connection
    data_sink = VerticaDataSink(config=config.database, test_mode=True)
    schema = config.database.schema

    logger.info("=" * 60)
    logger.info("Syncing LinkedIn TEST tables with production data...")
    logger.info("=" * 60)

    try:
        for table in LINKEDIN_TABLES:
            prod_table = f"{schema}.{table}"
            test_table = f"{schema}.{table}_TEST"

            logger.info(f"\n--- Processing {table} ---")

            with data_sink.conn.cursor() as cursor:
                # Step 1: Truncate TEST table
                logger.info(f"  Truncating {test_table}...")
                cursor.execute(f"TRUNCATE TABLE {test_table}")

                # Step 2: Copy data from production to TEST
                logger.info(f"  Copying data from {prod_table} to {test_table}...")
                cursor.execute(f"INSERT INTO {test_table} SELECT * FROM {prod_table}")

                # Step 3: Count rows copied
                cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
                row_count = cursor.fetchone()[0]

            data_sink.conn.commit()
            logger.success(f"  ✓ {table}_TEST: {row_count} rows copied")

        logger.info("\n" + "=" * 60)
        logger.success(f"✓ Successfully synced all {len(LINKEDIN_TABLES)} tables")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"Failed to sync tables: {e}")
        data_sink.conn.rollback()
        return 1

    finally:
        data_sink.close()


if __name__ == "__main__":
    sys.exit(sync_linkedin_test_tables())
