#!/usr/bin/env python3
"""
Test script for Microsoft Ads pipeline using test tables.

This script:
1. Creates test tables with '_test' suffix
2. Runs the pipeline writing to test tables
3. Shows results

Usage:
    python test_microsoft_pipeline.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from loguru import logger
from social.infrastructure.database import VerticaDataSink
from social.core.config import ConfigurationManager, DatabaseConfig
from social.platforms.microsoft.authenticator import MicrosoftAdsAuthenticator
from social.platforms.microsoft.pipeline import MicrosoftAdsPipeline

# Configure logging
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    level="INFO",
    colorize=True,
)


def get_vertica_connection():
    """Get Vertica database connection from environment variables."""
    required_vars = ["VERTICA_HOST", "VERTICA_PORT", "VERTICA_DATABASE", "VERTICA_USER", "VERTICA_PASSWORD"]
    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    # Create DatabaseConfig for VerticaDataSink
    db_config = DatabaseConfig(
        host=os.getenv("VERTICA_HOST"),
        port=int(os.getenv("VERTICA_PORT", "5433")),
        database=os.getenv("VERTICA_DATABASE"),
        user=os.getenv("VERTICA_USER"),
        password=os.getenv("VERTICA_PASSWORD"),
        schema="GoogleAnalytics"
    )

    return VerticaDataSink(config=db_config, test_mode=False)


def create_test_table_from_config(cursor, schema, table_name, table_config):
    """Create a test table from configuration fields."""
    fields = table_config.fields if table_config.fields else []

    # Build column definitions based on field names
    columns = []
    for field in fields:
        # Map field names to SQL types
        if 'Id' in field or field in ['CustomerId', 'AccountId', 'CampaignId', 'AdId']:
            columns.append(f"{field} VARCHAR(100)")
        elif field in ['CustomerName', 'AccountName', 'CampaignName']:
            columns.append(f"{field} VARCHAR(500)")
        elif field in ['Clicks', 'Impressions']:
            columns.append(f"{field} INTEGER")
        elif field == 'Ctr':
            # CTR needs more precision (4 decimals: 0.0123 = 1.23%)
            columns.append(f"{field} NUMERIC(10,4)")
        elif field in ['Conversions', 'Spend', 'AverageCpc', 'AverageCpm']:
            # Other metrics with 2 decimals
            columns.append(f"{field} NUMERIC(18,2)")
        else:
            columns.append(f"{field} VARCHAR(500)")

    # Add metadata columns
    columns.append("row_loaded_date DATE")
    columns.append("IngestionTimestamp TIMESTAMP")

    create_sql = f"""
    CREATE TABLE {schema}.{table_name}_test (
        {', '.join(columns)}
    );
    """
    cursor.execute(create_sql)
    cursor.execute("COMMIT")
    logger.success(f"✓ Created test table from config: {table_name}_test")


def create_test_tables(db_sink, platform_config, schema="GoogleAnalytics"):
    """Create test tables by copying structure from production tables or config."""

    # Get table names from config
    config_tables = list(platform_config.tables.keys())
    logger.info(f"Tables in config: {config_tables}")

    # Check which Microsoft tables actually exist
    logger.info(f"Checking existing Microsoft tables in schema '{schema}'...")
    existing_tables_df = db_sink.query(
        f"SELECT table_name FROM v_catalog.tables "
        f"WHERE table_schema='{schema}' AND table_name LIKE '%microsoft%' "
        f"ORDER BY table_name;"
    )
    existing_tables = existing_tables_df['table_name'].tolist() if not existing_tables_df.empty else []

    if existing_tables:
        logger.info(f"Found {len(existing_tables)} existing Microsoft table(s):")
        for table in existing_tables:
            logger.info(f"  • {table}")

    logger.info(f"Creating test tables in schema '{schema}'...")

    # Get raw cursor for DDL operations
    cursor = db_sink._get_cursor()

    for table_name in config_tables:
        test_table_name = f"{table_name}_test"
        production_exists = table_name in existing_tables

        try:
            # Drop test table if exists
            logger.info(f"Dropping existing test table {test_table_name} if exists...")
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{test_table_name} CASCADE;")
            cursor.execute("COMMIT")

            if production_exists:
                # Get the exact CREATE TABLE statement from production
                logger.info(f"Getting table definition from {table_name}...")
                cursor.execute(f"""
                    SELECT column_name, data_type, character_maximum_length,
                           numeric_precision, numeric_scale
                    FROM v_catalog.columns
                    WHERE table_schema='{schema}' AND table_name='{table_name}'
                    ORDER BY ordinal_position;
                """)
                columns_def = cursor.fetchall()

                # Build CREATE TABLE statement with exact types
                column_defs = []
                for col in columns_def:
                    col_name, data_type, char_len, num_prec, num_scale = col

                    # Reconstruct data type with precision/scale
                    if 'numeric' in data_type.lower() and num_prec and num_scale is not None:
                        col_def = f"{col_name} NUMERIC({num_prec},{num_scale})"
                    elif 'varchar' in data_type.lower() and char_len:
                        col_def = f"{col_name} VARCHAR({char_len})"
                    elif 'char' in data_type.lower() and char_len:
                        col_def = f"{col_name} CHAR({char_len})"
                    else:
                        col_def = f"{col_name} {data_type}"

                    column_defs.append(col_def)

                create_sql = f"""
                CREATE TABLE {schema}.{test_table_name} (
                    {', '.join(column_defs)}
                );
                """

                logger.info(f"Creating test table {test_table_name} with exact column definitions...")
                logger.debug(f"Column definitions: {column_defs}")
                cursor.execute(create_sql)
                cursor.execute("COMMIT")
                logger.success(f"✓ Created test table: {test_table_name}")
            else:
                # Create test table from config fields
                logger.info(f"Production table {table_name} doesn't exist, creating from config...")
                table_config = platform_config.get_table_config(table_name)
                create_test_table_from_config(cursor, schema, table_name, table_config)

        except Exception as e:
            logger.error(f"Failed to create test table {test_table_name}: {e}")
            raise


def setup_authenticator():
    """Setup Microsoft Ads authenticator from environment variables."""
    required_vars = {
        "client_id": "MICROSOFT_ADS_CLIENT_ID",
        "client_secret": "MICROSOFT_ADS_CLIENT_SECRET",
        "developer_token": "MICROSOFT_ADS_DEVELOPER_TOKEN",
        "customer_id": "MICROSOFT_ADS_CUSTOMER_ID",
        "account_id": "MICROSOFT_ADS_ACCOUNT_ID",
    }

    credentials = {}
    missing_vars = []

    for key, env_var in required_vars.items():
        value = os.getenv(env_var)
        if value is None:
            missing_vars.append(env_var)
        else:
            credentials[key] = value

    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    # Get optional token file path
    token_file = os.getenv("TOKEN_FILE", "tokens.json")

    # Initialize authenticator
    authenticator = MicrosoftAdsAuthenticator(
        client_id=credentials["client_id"],
        client_secret=credentials["client_secret"],
        developer_token=credentials["developer_token"],
        customer_id=credentials["customer_id"],
        account_id=credentials["account_id"],
        token_file=token_file,
    )

    return authenticator


def run_test_pipeline():
    """Run the Microsoft Ads pipeline in test mode."""
    config_path = Path(__file__).parent / "social" / "platforms" / "microsoft" / "config_microsoft_ads.yml"

    try:
        # Connect to Vertica
        logger.info("Connecting to Vertica...")
        db_sink = get_vertica_connection()
        logger.success("✓ Connected to Vertica")

        # Load configuration
        logger.info("Loading configuration...")
        config_manager = ConfigurationManager(config_dir=config_path.parent)
        app_config = config_manager.load_config(platform="microsoft")
        platform_config = app_config.get_platform_config("microsoft")
        logger.success("✓ Configuration loaded")

        # Create test tables
        create_test_tables(db_sink, platform_config)

        # Setup authenticator
        logger.info("Setting up authenticator...")
        authenticator = setup_authenticator()
        logger.success("✓ Authenticator initialized")

        # Authenticate
        logger.info("Authenticating...")
        tenant_id = os.getenv("AZURE_TENANT_ID")
        is_container = os.getenv("CONTAINER_MODE", "false").lower() == "true"

        if is_container:
            success = authenticator.authenticate_for_container_app(tenant_id=tenant_id)
        else:
            success = authenticator.authenticate(tenant_id=tenant_id)

        if not success:
            raise RuntimeError("Authentication failed")

        logger.success("✓ Authentication successful")

        # Create custom data sink wrapper to override table names with _test suffix
        class TestDataSinkWrapper:
            def __init__(self, db_sink):
                self.db_sink = db_sink

            def write_dataframe(self, df, table_name, schema_name="GoogleAnalytics", if_exists="append"):
                # Add _test suffix
                if not table_name.endswith("_test"):
                    test_table_name = f"{table_name}_test"
                else:
                    test_table_name = table_name

                logger.info(f"Writing to test table: {test_table_name}")

                # Map if_exists to mode for VerticaDataSink.load()
                mode_mapping = {
                    "replace": "replace",
                    "append": "append",
                    "fail": "append"
                }
                mode = mode_mapping.get(if_exists, "replace")

                return self.db_sink.load(
                    df=df,
                    table_name=test_table_name,
                    mode=mode
                )

        data_sink = TestDataSinkWrapper(db_sink)

        # Initialize pipeline
        logger.info("Initializing pipeline...")
        pipeline = MicrosoftAdsPipeline(
            config=platform_config,
            authenticator=authenticator,
            data_sink=data_sink,
        )
        logger.success("✓ Pipeline initialized")

        # Run pipeline for all tables
        logger.info("\n" + "=" * 60)
        logger.info("Running pipeline with TEST TABLES")
        logger.info("=" * 60 + "\n")

        start_time = datetime.now()
        results = pipeline.run_all_tables(load_to_sink=True)
        duration = (datetime.now() - start_time).total_seconds()

        # Print summary
        logger.info("\n" + "=" * 60)
        logger.success("TEST PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"Duration: {duration:.2f}s")
        logger.info(f"Tables processed: {len(results)}")

        for table_name, df in results.items():
            logger.info(f"  • {table_name}: {len(df)} rows, {len(df.columns)} columns")

        # Show sample data
        if results:
            first_table = list(results.keys())[0]
            first_df = results[first_table]

            logger.info(f"\nSample data from {first_table}:")
            logger.info(f"Columns: {list(first_df.columns)}")

            if len(first_df) > 0:
                logger.info("\nFirst 3 rows:")
                # Set pandas display options to show more decimal places for CTR
                import pandas as pd
                pd.set_option('display.float_format', lambda x: f'{x:.6f}')
                print(first_df.head(3).to_string())
                pd.reset_option('display.float_format')

        logger.info("\n" + "=" * 60)
        logger.info("Test tables created and populated successfully!")
        logger.info("You can now query them in Vertica:")
        for table_name in results.keys():
            logger.info(f"  SELECT * FROM GoogleAnalytics.{table_name}_test LIMIT 10;")
        logger.info("=" * 60)

        return True

    except Exception as e:
        logger.error(f"\n{'=' * 60}")
        logger.error(f"TEST FAILED: {e}")
        logger.error(f"{'=' * 60}")
        logger.exception(e)
        return False

    finally:
        # Close database connection
        if 'db_sink' in locals():
            db_sink.close()


if __name__ == "__main__":
    logger.info("Microsoft Ads Pipeline Test Script")
    logger.info("=" * 60)

    # Check environment
    logger.info("Checking environment variables...")

    required_vars = [
        "VERTICA_HOST", "VERTICA_USER", "VERTICA_PASSWORD",
        "MICROSOFT_ADS_CLIENT_ID", "MICROSOFT_ADS_CLIENT_SECRET",
        "MICROSOFT_ADS_DEVELOPER_TOKEN", "MICROSOFT_ADS_CUSTOMER_ID",
        "MICROSOFT_ADS_ACCOUNT_ID"
    ]

    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        logger.error(f"Missing required environment variables:")
        for var in missing:
            logger.error(f"  - {var}")
        logger.error("\nPlease set these variables and try again.")
        sys.exit(1)

    logger.success("✓ All required environment variables are set")

    # Run test
    success = run_test_pipeline()

    sys.exit(0 if success else 1)
