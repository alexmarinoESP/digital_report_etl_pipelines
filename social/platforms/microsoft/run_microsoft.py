#!/usr/bin/env python3
"""
Microsoft Ads ETL Pipeline Runner.

Standalone entry point for Azure Container App deployment.
This script orchestrates the complete Microsoft Ads data extraction pipeline:
1. Load configuration from YAML
2. Setup authentication (OAuth2 or Service Principal)
3. Initialize data sink (Vertica or Azure Table Storage)
4. Run ETL pipeline for all configured tables
5. Exit with proper status codes

Features:
- Environment-driven configuration
- Multiple authentication strategies (refresh token, service principal, browser OAuth)
- Flexible data sink selection (Vertica, Azure Table, etc.)
- Container-ready (no browser interactions when using service principal)
- Comprehensive logging and error handling
- Graceful failure recovery

Environment Variables:
    Required:
    - MICROSOFT_ADS_CLIENT_ID: OAuth application client ID
    - MICROSOFT_ADS_CLIENT_SECRET: OAuth application client secret
    - MICROSOFT_ADS_DEVELOPER_TOKEN: Microsoft Ads developer token
    - MICROSOFT_ADS_CUSTOMER_ID: Microsoft Ads customer ID
    - MICROSOFT_ADS_ACCOUNT_ID: Microsoft Ads account ID

    Optional:
    - STORAGE_TYPE: "vertica" (default) or "azure_table"
    - AZURE_TENANT_ID: Azure tenant ID (for service principal auth)
    - TOKEN_FILE: Path to token file (default: tokens.json)
    - LOG_LEVEL: Logging level (default: INFO)

    For Vertica:
    - VERTICA_HOST, VERTICA_PORT, VERTICA_DATABASE
    - VERTICA_USER, VERTICA_PASSWORD

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
from typing import Any, Optional

from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from social.core.config import ConfigurationManager
from social.core.exceptions import ConfigurationError, PipelineError
from social.platforms.microsoft.authenticator import MicrosoftAdsAuthenticator
from social.platforms.microsoft.pipeline import MicrosoftAdsPipeline


def setup_logging(log_level: str = "INFO") -> None:
    """
    Configure logging with both console and file output.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    # Remove default handler
    logger.remove()

    # Add console handler with colors
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=log_level,
        colorize=True,
    )

    # Add file handler (optional, for debugging)
    if os.getenv("LOG_TO_FILE", "false").lower() == "true":
        log_file = Path("logs") / f"microsoft_ads_{datetime.now():%Y%m%d}.log"
        log_file.parent.mkdir(exist_ok=True)

        logger.add(
            log_file,
            rotation="10 MB",
            retention="7 days",
            level="DEBUG",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{function}:{line} - {message}",
        )

        logger.info(f"File logging enabled: {log_file}")


def load_configuration() -> Any:
    """
    Load Microsoft Ads platform configuration from YAML.

    Returns:
        PlatformConfig instance

    Raises:
        ConfigurationError: If configuration loading fails
    """
    try:
        logger.info("Loading configuration")

        # Determine config directory
        config_dir = Path(__file__).parent

        # Initialize configuration manager
        config_manager = ConfigurationManager(config_dir=config_dir)

        # Load platform config (microsoft specific)
        app_config = config_manager.load_config(platform="microsoft")

        # Get Microsoft Ads platform config
        platform_config = app_config.get_platform_config("microsoft")

        logger.success("Configuration loaded successfully")
        logger.debug(f"Tables configured: {list(platform_config.tables.keys())}")

        return platform_config

    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise ConfigurationError("Configuration loading failed") from e


def setup_authenticator() -> MicrosoftAdsAuthenticator:
    """
    Setup Microsoft Ads authenticator with credentials from environment.

    Returns:
        Configured MicrosoftAdsAuthenticator instance

    Raises:
        ConfigurationError: If required credentials are missing
    """
    try:
        logger.info("Setting up authenticator")

        # Get required credentials from environment
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
            raise ConfigurationError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

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

        logger.success("Authenticator initialized")
        return authenticator

    except ConfigurationError:
        raise
    except Exception as e:
        logger.error(f"Failed to setup authenticator: {e}")
        raise ConfigurationError("Authenticator setup failed") from e


def authenticate(authenticator: MicrosoftAdsAuthenticator) -> bool:
    """
    Perform authentication using available methods.

    Priority:
    1. Stored refresh token (from file)
    2. Service Principal (Azure AD) - for Container Apps
    3. Browser OAuth flow - for local development

    Args:
        authenticator: MicrosoftAdsAuthenticator instance

    Returns:
        True if authentication successful

    Raises:
        RuntimeError: If all authentication methods fail
    """
    try:
        logger.info("Starting authentication process")

        # Get tenant ID if available (for service principal)
        tenant_id = os.getenv("AZURE_TENANT_ID")

        # Check if we're in container mode (no display available)
        is_container = os.getenv("CONTAINER_MODE", "false").lower() == "true"

        if is_container:
            logger.info("Container mode detected - using container-specific authentication")
            success = authenticator.authenticate_for_container_app(tenant_id=tenant_id)
        else:
            logger.info("Standard mode - attempting all authentication methods")
            success = authenticator.authenticate(tenant_id=tenant_id)

        if success:
            logger.success("Authentication successful")
            return True
        else:
            raise RuntimeError("All authentication methods failed")

    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        raise


def setup_data_sink() -> Optional[Any]:
    """
    Setup data sink based on STORAGE_TYPE environment variable.

    Returns:
        Data sink instance (VerticaDBManager or Azure Table client) or None

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


def setup_vertica_sink() -> Any:
    """
    Setup Vertica database sink.

    Returns:
        VerticaDBManager instance

    Raises:
        ConfigurationError: If Vertica configuration is invalid
    """
    try:
        # Import here to avoid dependency if not using Vertica
        from microsoft_ads_etl.app.db_manager import VerticaDBManager

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

        # Get optional port
        config["port"] = int(os.getenv("VERTICA_PORT", "5433"))

        # Initialize Vertica client
        vertica_client = VerticaDBManager(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            port=config["port"],
        )

        logger.success("Vertica data sink initialized")
        return vertica_client

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
        table_name = os.getenv("AZURE_TABLE_NAME", "microsoftads")

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
    config: Any,
    authenticator: MicrosoftAdsAuthenticator,
    data_sink: Optional[Any],
) -> bool:
    """
    Run the complete Microsoft Ads ETL pipeline.

    Args:
        config: Platform configuration
        authenticator: Configured authenticator
        data_sink: Data sink for loading data

    Returns:
        True if all tables processed successfully

    Raises:
        PipelineError: If pipeline execution fails
    """
    try:
        logger.info("Initializing pipeline")

        # Create pipeline
        pipeline = MicrosoftAdsPipeline(
            config=config,
            authenticator=authenticator,
            data_sink=data_sink,
        )

        # Run all configured tables
        logger.info("Starting pipeline execution for all tables")
        start_time = datetime.now()

        results = pipeline.run_all_tables(load_to_sink=(data_sink is not None))

        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()

        # Log summary
        logger.success(
            f"Pipeline completed successfully: "
            f"{len(results)} table(s) processed in {duration:.2f}s"
        )

        for table_name, df in results.items():
            logger.info(f"  {table_name}: {len(df)} rows")

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
    logger.info("Microsoft Ads ETL Pipeline")
    logger.info("=" * 60)

    try:
        # Step 1: Load configuration
        logger.info("\n[1/5] Loading configuration...")
        config = load_configuration()

        # Step 2: Setup authenticator
        logger.info("\n[2/5] Setting up authenticator...")
        authenticator = setup_authenticator()

        # Step 3: Authenticate
        logger.info("\n[3/5] Authenticating...")
        authenticate(authenticator)

        # Step 4: Setup data sink
        logger.info("\n[4/5] Setting up data sink...")
        data_sink = setup_data_sink()

        # Step 5: Run pipeline
        logger.info("\n[5/5] Running pipeline...")
        run_pipeline(config, authenticator, data_sink)

        # Success
        logger.info("\n" + "=" * 60)
        logger.success("Microsoft Ads ETL Pipeline completed successfully")
        logger.info("=" * 60)
        return 0

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1

    except RuntimeError as e:
        if "authentication" in str(e).lower():
            logger.error(f"Authentication error: {e}")
            return 2
        else:
            logger.error(f"Runtime error: {e}")
            return 3

    except PipelineError as e:
        logger.error(f"Pipeline error: {e}")
        return 3

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 4


if __name__ == "__main__":
    sys.exit(main())
