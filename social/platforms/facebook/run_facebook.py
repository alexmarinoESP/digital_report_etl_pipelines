#!/usr/bin/env python3
"""
Facebook Ads ETL Pipeline Runner.

Standalone entry point for Azure Container App deployment.
This script orchestrates the complete Facebook Ads data extraction pipeline:
1. Load configuration from YAML
2. Setup authentication (Token Provider)
3. Initialize data sink (Vertica or Azure Table Storage)
4. Run ETL pipeline for all configured tables
5. Exit with proper status codes

Features:
- Environment-driven configuration
- Token-based authentication (file or environment)
- Flexible data sink selection (Vertica, Azure Table, etc.)
- Container-ready (no browser interactions)
- Comprehensive logging and error handling
- Graceful failure recovery

Environment Variables:
    Required:
    - FACEBOOK_APP_ID: Facebook app ID
    - FACEBOOK_APP_SECRET: Facebook app secret
    - FACEBOOK_ACCESS_TOKEN: Facebook access token
    - FB_AD_ACCOUNT_IDS: Comma-separated list of ad account IDs

    Optional:
    - STORAGE_TYPE: "vertica" (default) or "azure_table"
    - CREDENTIALS_FILE: Path to credentials YAML file
    - LOG_LEVEL: Logging level (default: INFO)

    For Vertica:
    - VERTICA_HOST, VERTICA_PORT, VERTICA_DATABASE
    - VERTICA_USER, VERTICA_PASSWORD, VERTICA_SCHEMA

    For Azure Table Storage:
    - AZURE_STORAGE_CONNECTION_STRING
    - AZURE_TABLE_NAME

Exit Codes:
    0: Success
    1: Configuration error
    2: Authentication error
    3: Pipeline execution error
    4: Data sink error
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from social.core.exceptions import AuthenticationError, ConfigurationError, PipelineError
from social.core.protocols import DataSink, TokenProvider
from social.infrastructure.file_token_provider import FileBasedTokenProvider
from social.platforms.facebook.pipeline import FacebookPipeline


def setup_logging(log_level: str = "INFO") -> None:
    """
    Configure logging with both console and file output.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    # Remove default handler
    logger.remove()

    # Disable colors on Azure (NO_COLOR env var or when running in container)
    # Azure Container Apps doesn't render ANSI color codes correctly
    use_colors = os.getenv("NO_COLOR") is None and os.getenv("LOGURU_COLORIZE", "true").lower() != "false"

    # Add console handler
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=log_level,
        colorize=use_colors,
    )

    # Add file handler (optional, for debugging)
    if os.getenv("LOG_TO_FILE", "false").lower() == "true":
        log_file = Path("logs") / f"facebook_ads_{datetime.now():%Y%m%d}.log"
        log_file.parent.mkdir(exist_ok=True)

        logger.add(
            log_file,
            rotation="10 MB",
            retention="7 days",
            level="DEBUG",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{function}:{line} - {message}",
        )

        logger.info(f"File logging enabled: {log_file}")


def load_configuration() -> Dict[str, Any]:
    """
    Load Facebook Ads platform configuration from YAML.

    Returns:
        Configuration dictionary

    Raises:
        ConfigurationError: If configuration loading fails
    """
    try:
        logger.info("Loading configuration")

        # Determine config file path
        config_file = Path(__file__).parent / "config_fb_ads.yml"

        if not config_file.exists():
            raise ConfigurationError(f"Configuration file not found: {config_file}")

        # Load YAML configuration
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        logger.success("Configuration loaded successfully")

        # Extract table names
        table_names = [k for k in config.keys() if k.startswith("fb_ads_")]
        logger.debug(f"Tables configured: {table_names}")

        return config

    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise ConfigurationError("Configuration loading failed") from e


def setup_token_provider() -> TokenProvider:
    """
    Setup Facebook token provider with credentials.

    Returns:
        Configured TokenProvider instance

    Raises:
        AuthenticationError: If authentication setup fails
    """
    try:
        logger.info("Setting up token provider")

        # Get credentials file path (optional)
        credentials_file = os.getenv("CREDENTIALS_FILE")

        # Initialize file-based token provider
        token_provider = FileBasedTokenProvider(
            platform="facebook",
            credentials_file=credentials_file
        )

        # Validate token
        access_token = token_provider.get_access_token()
        if not access_token:
            raise AuthenticationError("No access token available")

        logger.success("Token provider initialized")
        return token_provider

    except Exception as e:
        logger.error(f"Failed to setup token provider: {e}")
        raise AuthenticationError("Token provider setup failed") from e


def get_ad_account_ids(token_provider: Any = None) -> List[str]:
    """
    Get Facebook ad account IDs from environment or credentials file.

    Args:
        token_provider: Optional TokenProvider with loaded credentials

    Returns:
        List of ad account IDs

    Raises:
        ConfigurationError: If no account IDs are configured
    """
    try:
        # Try environment variable first
        account_ids_str = os.getenv("FB_AD_ACCOUNT_IDS", "")

        if account_ids_str:
            # Split by comma and strip whitespace
            account_ids = [aid.strip() for aid in account_ids_str.split(",") if aid.strip()]
            if account_ids:
                logger.info(f"Configured {len(account_ids)} ad account(s) from environment")
                return account_ids

        # Fallback to credentials from token provider (file)
        if token_provider and hasattr(token_provider, '_credentials'):
            credentials = token_provider._credentials
            if 'id_account' in credentials and credentials['id_account']:
                account_ids = credentials['id_account']
                if isinstance(account_ids, str):
                    account_ids = [account_ids]
                logger.info(f"Configured {len(account_ids)} ad account(s) from credentials file")
                return account_ids

        raise ConfigurationError(
            "No ad account IDs found. Set FB_AD_ACCOUNT_IDS environment variable "
            "or configure 'id_account' in credentials file"
        )

    except ConfigurationError:
        raise
    except Exception as e:
        logger.error(f"Failed to parse ad account IDs: {e}")
        raise ConfigurationError("Ad account ID parsing failed") from e


def setup_data_sink() -> Optional[DataSink]:
    """
    Setup data sink based on STORAGE_TYPE environment variable.

    Returns:
        Data sink instance (VerticaDataSink or Azure Table client) or None

    Raises:
        ConfigurationError: If storage configuration is invalid
    """
    storage_type = os.getenv("STORAGE_TYPE", "vertica").lower()
    logger.info(f"Setting up data sink: {storage_type}")

    try:
        if storage_type == "vertica":
            return setup_vertica_sink()
        elif storage_type == "azure_table":
            return setup_azure_table_sink()
        elif storage_type == "none":
            logger.warning("No data sink configured - data will not be persisted")
            return None
        else:
            raise ConfigurationError(f"Unknown storage type: {storage_type}")

    except Exception as e:
        logger.error(f"Failed to setup data sink: {e}")
        raise


def setup_vertica_sink() -> DataSink:
    """
    Setup Vertica database sink.

    Returns:
        VerticaDataSink instance

    Raises:
        ConfigurationError: If Vertica configuration is invalid
    """
    try:
        # Import here to avoid dependency if not using Vertica
        from social.infrastructure.database import VerticaDataSink
        from social.core.config import DatabaseConfig

        # Get Vertica credentials from environment
        required_vars = {
            "host": "VERTICA_HOST",
            "user": "VERTICA_USER",
            "password": "VERTICA_PASSWORD",
            "database": "VERTICA_DATABASE",
        }

        config = {}
        missing_vars = []

        for key, env_var in required_vars.items():
            value = os.getenv(env_var)
            if value is None:
                missing_vars.append(env_var)
            else:
                config[key] = value

        if missing_vars:
            raise ConfigurationError(
                f"Missing Vertica configuration: {', '.join(missing_vars)}"
            )

        # Get optional port and schema
        config["port"] = int(os.getenv("VERTICA_PORT", "5433"))
        config["schema"] = os.getenv("VERTICA_SCHEMA", "GoogleAnalytics")

        # Create database config (matches Google approach)
        db_config = DatabaseConfig(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            schema=config["schema"]
        )

        # Check if running in test mode
        test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        logger.info(f"Test mode: {test_mode}")

        # Initialize Vertica client
        vertica_sink = VerticaDataSink(config=db_config, test_mode=test_mode)

        logger.success("Vertica data sink initialized")
        return vertica_sink

    except ConfigurationError:
        raise
    except Exception as e:
        logger.error(f"Failed to setup Vertica sink: {e}")
        raise ConfigurationError("Vertica setup failed") from e


def setup_azure_table_sink() -> Any:
    """
    Setup Azure Table Storage sink.

    Returns:
        Azure Table client

    Raises:
        ConfigurationError: If Azure configuration is invalid
    """
    try:
        # Import here to avoid dependency if not using Azure
        from azure.data.tables import TableServiceClient

        # Get Azure credentials
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        table_name = os.getenv("AZURE_TABLE_NAME", "facebookads")

        if not connection_string:
            raise ConfigurationError("AZURE_STORAGE_CONNECTION_STRING not set")

        # Initialize table client
        table_service = TableServiceClient.from_connection_string(connection_string)
        table_client = table_service.get_table_client(table_name)

        # Create table if it doesn't exist
        try:
            table_client.create_table()
            logger.info(f"Created Azure table: {table_name}")
        except Exception:
            logger.debug(f"Azure table already exists: {table_name}")

        logger.success("Azure Table Storage sink initialized")
        return table_client

    except ConfigurationError:
        raise
    except Exception as e:
        logger.error(f"Failed to setup Azure Table sink: {e}")
        raise ConfigurationError("Azure Table setup failed") from e


def run_pipeline(
    config: Dict[str, Any],
    token_provider: TokenProvider,
    ad_account_ids: List[str],
    data_sink: Optional[DataSink],
) -> bool:
    """
    Run the complete Facebook Ads ETL pipeline.

    Args:
        config: Platform configuration
        token_provider: Configured token provider
        ad_account_ids: List of ad account IDs to process
        data_sink: Data sink for loading data

    Returns:
        True if all tables processed successfully

    Raises:
        PipelineError: If pipeline execution fails
    """
    try:
        logger.info("Initializing pipeline")

        # Get Facebook App credentials from environment
        app_id = os.getenv("FACEBOOK_APP_ID")
        app_secret = os.getenv("FACEBOOK_APP_SECRET")

        if not app_id or not app_secret:
            raise ConfigurationError("FACEBOOK_APP_ID and FACEBOOK_APP_SECRET must be set")

        # Create pipeline
        pipeline = FacebookPipeline(
            config=config,
            token_provider=token_provider,
            ad_account_ids=ad_account_ids,
            app_id=app_id,
            app_secret=app_secret,
            data_sink=data_sink,
        )

        # Run all configured tables
        logger.info("Starting pipeline execution for all tables")
        start_time = datetime.now()

        results = pipeline.run_all_tables()

        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()

        # Log summary
        logger.success(
            f"Pipeline completed successfully: "
            f"{len(results)} table(s) processed in {duration:.2f}s"
        )

        for table_name, df in results.items():
            logger.info(f"  {table_name}: {len(df)} rows")

        # Close pipeline resources
        pipeline.close()

        return True

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise PipelineError("Pipeline execution failed") from e


def main() -> int:
    """
    Main entry point.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Setup logging
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    setup_logging(log_level)

    logger.info("=" * 60)
    logger.info("Facebook Ads ETL Pipeline")
    logger.info("=" * 60)

    try:
        # Step 1: Load configuration
        logger.info("\n[1/5] Loading configuration...")
        config = load_configuration()

        # Step 2: Setup token provider
        logger.info("\n[2/5] Setting up token provider...")
        token_provider = setup_token_provider()

        # Step 3: Get ad account IDs (pass token_provider to allow fallback to credentials file)
        logger.info("\n[3/5] Loading ad account IDs...")
        ad_account_ids = get_ad_account_ids(token_provider=token_provider)

        # Step 4: Setup data sink
        logger.info("\n[4/5] Setting up data sink...")
        data_sink = setup_data_sink()

        # Step 5: Run pipeline
        logger.info("\n[5/5] Running pipeline...")
        run_pipeline(config, token_provider, ad_account_ids, data_sink)

        # Success
        logger.info("\n" + "=" * 60)
        logger.success("Facebook Ads ETL Pipeline completed successfully")
        logger.info("=" * 60)
        return 0

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1

    except AuthenticationError as e:
        logger.error(f"Authentication error: {e}")
        return 2

    except PipelineError as e:
        logger.error(f"Pipeline error: {e}")
        return 3

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 4


if __name__ == "__main__":
    sys.exit(main())
