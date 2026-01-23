#!/usr/bin/env python3
"""Social Ads Orchestrator Entry Point.

This is the main entry point for running the unified Social Ads ETL orchestrator
that coordinates all 4 platforms: Microsoft, LinkedIn, Facebook, and Google Ads.

Features:
- Environment-driven configuration
- Token provider setup for each platform
- Flexible data sink selection (Vertica, Azure, etc.)
- Comprehensive logging and error handling
- Exit codes for container orchestration

Environment Variables:
    Required:
    - ORCHESTRATOR_CONFIG: Path to orchestrator config YAML (default: social/orchestrator/orchestrator_config.yml)
    - STORAGE_TYPE: "vertica" (default) or "azure_table" or "none"

    For Vertica:
    - VERTICA_HOST, VERTICA_PORT, VERTICA_DATABASE
    - VERTICA_USER, VERTICA_PASSWORD

    For Azure:
    - AZURE_STORAGE_CONNECTION_STRING
    - AZURE_TABLE_NAME

    Platform-specific (token providers):
    - MICROSOFT_CLIENT_ID, MICROSOFT_CLIENT_SECRET, MICROSOFT_TENANT_ID
    - LINKEDIN_CLIENT_ID, LINKEDIN_CLIENT_SECRET, LINKEDIN_ACCESS_TOKEN
    - FACEBOOK_APP_ID, FACEBOOK_APP_SECRET, FACEBOOK_ACCESS_TOKEN, FACEBOOK_AD_ACCOUNT_IDS
    - GOOGLE_CONFIG_FILE, GOOGLE_MANAGER_CUSTOMER_ID

    Optional:
    - LOG_LEVEL: Logging level (default: INFO)
    - LOG_TO_FILE: Enable file logging (default: false)
    - EXPORT_REPORT: Export execution report (default: true)
    - REPORT_FORMAT: Report format - json or csv (default: json)
    - REPORT_PATH: Report output path (default: execution_report.{format})

Exit Codes:
    0: All platforms completed successfully
    1: Configuration error
    2: Some platforms failed (partial success)
    3: All platforms failed
    4: Orchestrator error
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from social.core.exceptions import AuthenticationError, ConfigurationError, PipelineError
from social.core.protocols import DataSink, TokenProvider
from social.infrastructure.file_token_provider import FileBasedTokenProvider
from social.orchestrator.config import load_orchestrator_config
from social.orchestrator.orchestrator import SocialAdsOrchestrator
from social.orchestrator.registry import create_default_registry


def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging with console and optional file output.

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

    # Add file handler if enabled
    if os.getenv("LOG_TO_FILE", "false").lower() == "true":
        log_file = Path("logs") / f"orchestrator_{datetime.now():%Y%m%d}.log"
        log_file.parent.mkdir(exist_ok=True)

        logger.add(
            log_file,
            rotation="50 MB",
            retention="14 days",
            level="DEBUG",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{function}:{line} - {message}",
        )

        logger.info(f"File logging enabled: {log_file}")


def load_orchestrator_configuration():
    """Load orchestrator configuration from YAML file.

    Returns:
        OrchestratorConfig instance

    Raises:
        ConfigurationError: If configuration loading fails
    """
    try:
        logger.info("Loading orchestrator configuration")

        # Get config file path from environment
        config_path_str = os.getenv(
            "ORCHESTRATOR_CONFIG",
            str(Path(__file__).parent / "orchestrator_config.yml")
        )
        config_path = Path(config_path_str)

        if not config_path.exists():
            raise ConfigurationError(f"Orchestrator config not found: {config_path}")

        # Load configuration
        config = load_orchestrator_config(config_path)

        logger.success(f"Configuration loaded: {len(config.platforms)} platforms configured")
        return config

    except Exception as e:
        logger.error(f"Failed to load orchestrator configuration: {e}")
        raise ConfigurationError(f"Configuration loading failed: {e}") from e


def setup_token_provider(platform: str) -> TokenProvider:
    """Setup token provider for a specific platform.

    Args:
        platform: Platform name (microsoft, linkedin, facebook, google)

    Returns:
        TokenProvider instance

    Raises:
        AuthenticationError: If token provider setup fails
    """
    try:
        logger.debug(f"Setting up token provider for: {platform}")

        # Get credentials file path (optional)
        credentials_file = os.getenv("CREDENTIALS_FILE")

        # Initialize file-based token provider
        token_provider = FileBasedTokenProvider(
            platform=platform,
            credentials_file=credentials_file
        )

        # Validate token
        access_token = token_provider.get_access_token()
        if not access_token:
            raise AuthenticationError(f"No access token available for {platform}")

        logger.debug(f"Token provider initialized for: {platform}")
        return token_provider

    except Exception as e:
        logger.error(f"Failed to setup token provider for {platform}: {e}")
        raise AuthenticationError(f"Token provider setup failed for {platform}: {e}") from e


def setup_data_sink() -> Optional[Any]:
    """Setup data sink based on STORAGE_TYPE environment variable.

    Returns:
        Data sink instance or None

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
    """Setup Vertica database sink.

    Returns:
        VerticaDataSink instance

    Raises:
        ConfigurationError: If Vertica configuration is invalid
    """
    try:
        # Import here to avoid dependency if not using Vertica
        from social.infrastructure.vertica_sink import VerticaDataSink

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
        vertica_sink = VerticaDataSink(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            port=config["port"],
            schema="GoogleAnalytics"
        )

        logger.success("Vertica data sink initialized")
        return vertica_sink

    except ConfigurationError:
        raise
    except Exception as e:
        logger.error(f"Failed to setup Vertica sink: {e}")
        raise ConfigurationError(f"Vertica setup failed: {e}") from e


def setup_azure_table_sink() -> Any:
    """Setup Azure Table Storage sink.

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
        table_name = os.getenv("AZURE_TABLE_NAME", "socialads")

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
        raise ConfigurationError(f"Azure Table setup failed: {e}") from e


def run_orchestrator() -> int:
    """Run the Social Ads orchestrator.

    Returns:
        Exit code (0=success, 1=config error, 2=partial failure, 3=all failed, 4=orchestrator error)
    """
    try:
        logger.info("Initializing Social Ads Orchestrator")

        # Load configuration
        config = load_orchestrator_configuration()

        # Setup data sink
        data_sink = setup_data_sink()

        # Setup token provider (using generic provider for all platforms)
        # Each platform will get its token from the provider
        token_provider = setup_token_provider("multi-platform")

        # Create platform registry with all platforms
        registry = create_default_registry()

        # Platform-specific configuration from environment
        # Facebook requires additional parameters
        if "facebook" in config.platforms and config.platforms["facebook"].enabled:
            fb_account_ids = os.getenv("FACEBOOK_AD_ACCOUNT_IDS", "").split(",")
            fb_account_ids = [aid.strip() for aid in fb_account_ids if aid.strip()]

            if fb_account_ids:
                registry._platforms["facebook"]["additional_params"]["ad_account_ids"] = fb_account_ids
                registry._platforms["facebook"]["additional_params"]["app_id"] = os.getenv("FACEBOOK_APP_ID", "")
                registry._platforms["facebook"]["additional_params"]["app_secret"] = os.getenv("FACEBOOK_APP_SECRET", "")

        # Google requires config file
        if "google" in config.platforms and config.platforms["google"].enabled:
            google_config = os.getenv("GOOGLE_CONFIG_FILE", "")
            if google_config:
                registry._platforms["google"]["additional_params"]["google_config_file"] = google_config

            google_mcc = os.getenv("GOOGLE_MANAGER_CUSTOMER_ID", "9474097201")
            registry._platforms["google"]["additional_params"]["manager_customer_id"] = google_mcc

        # Create orchestrator
        orchestrator = SocialAdsOrchestrator(
            config=config,
            registry=registry,
            token_provider=token_provider,
            data_sink=data_sink,
        )

        # Run all platforms
        logger.info("\nStarting orchestrator execution...")
        result = orchestrator.run_all_platforms()

        # Export report if enabled
        if os.getenv("EXPORT_REPORT", "true").lower() == "true":
            report_format = os.getenv("REPORT_FORMAT", "json")
            report_path = Path(os.getenv("REPORT_PATH", f"execution_report.{report_format}"))
            orchestrator.export_report(report_format, report_path)

        # Determine exit code
        if result.success:
            logger.success("\n" + "=" * 60)
            logger.success("All platforms completed successfully!")
            logger.success(f"Total rows processed: {result.total_rows_processed:,}")
            logger.success(f"Total duration: {result.total_duration_seconds:.2f}s")
            logger.success("=" * 60)
            return 0

        elif result.completed_platforms and result.failed_platforms:
            logger.warning("\n" + "=" * 60)
            logger.warning("Partial success - some platforms failed")
            logger.warning(f"Completed: {', '.join(result.completed_platforms)}")
            logger.warning(f"Failed: {', '.join(result.failed_platforms)}")
            logger.warning("=" * 60)
            return 2

        elif result.failed_platforms:
            logger.error("\n" + "=" * 60)
            logger.error("All platforms failed!")
            logger.error(f"Failed platforms: {', '.join(result.failed_platforms)}")
            logger.error("=" * 60)
            return 3

        else:
            logger.warning("No platforms were executed")
            return 0

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1

    except PipelineError as e:
        logger.error(f"Pipeline error: {e}")
        return 4

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 4


def main() -> int:
    """Main entry point.

    Returns:
        Exit code
    """
    # Setup logging
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    setup_logging(log_level)

    logger.info("=" * 60)
    logger.info("Social Ads Orchestrator - Multi-Platform ETL")
    logger.info("=" * 60)
    logger.info(f"Platforms: Microsoft, LinkedIn, Facebook, Google")
    logger.info("=" * 60)

    try:
        return run_orchestrator()

    except KeyboardInterrupt:
        logger.warning("\nOrchestrator interrupted by user")
        return 130

    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        return 4


if __name__ == "__main__":
    sys.exit(main())
