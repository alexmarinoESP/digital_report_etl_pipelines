"""Google Ads Pipeline Entry Point.

This script is the main entry point for running the Google Ads ETL pipeline
in Azure Container Apps or other containerized environments.

Key Features:
- Environment variable configuration
- Multiple storage backend support (Vertica, Azure Blob)
- Comprehensive error handling and exit codes
- Detailed logging
- Production-ready for containerized deployment

Exit Codes:
- 0: Success
- 1: Configuration error
- 2: Authentication error
- 3: Pipeline execution error
- 4: Data sink error

Environment Variables:
- GOOGLE_ADS_CONFIG_FILE: Path to google-ads.yaml config file
- GOOGLE_MANAGER_CUSTOMER_ID: Manager account ID (MCC)
- GOOGLE_API_VERSION: Google Ads API version (default: v18)
- STORAGE_TYPE: Storage backend (vertica, azure, or none)
- VERTICA_HOST, VERTICA_PORT, VERTICA_DATABASE, VERTICA_USER, VERTICA_PASSWORD
- AZURE_STORAGE_CONNECTION_STRING, AZURE_CONTAINER_NAME
"""

import os
import sys
from pathlib import Path
from typing import Dict, Optional

from loguru import logger

from social.core.exceptions import (
    AuthenticationError,
    ConfigurationError,
    PipelineError,
)
from social.infrastructure.file_token_provider import FileBasedTokenProvider
from social.platforms.google.constants import API_VERSION
from social.platforms.google.pipeline import GooglePipeline, load_config


def setup_logging() -> None:
    """Configure logging for the pipeline."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
    )
    logger.info("Logging configured")


def get_config_path() -> Path:
    """
    Get path to googleads_config.yml configuration file.

    Returns:
        Path to config file

    Raises:
        ConfigurationError: If config file not found
    """
    # Try multiple locations
    possible_paths = [
        Path("social/platforms/google/googleads_config.yml"),
        Path(__file__).parent / "googleads_config.yml",
        Path("/app/social/platforms/google/googleads_config.yml"),  # Container path
    ]

    for path in possible_paths:
        if path.exists():
            logger.info(f"Found config file: {path}")
            return path

    raise ConfigurationError(
        f"Configuration file not found. Tried: {[str(p) for p in possible_paths]}"
    )


def get_google_ads_config_file() -> str:
    """
    Get path to google-ads.yaml credentials file from environment.

    Returns:
        Path to google-ads.yaml file

    Raises:
        ConfigurationError: If config file not specified or not found
    """
    config_file = os.getenv("GOOGLE_ADS_CONFIG_FILE")

    if not config_file:
        # Try default locations
        default_paths = [
            "social/platforms/google/google-ads-9474097201.yml",
            Path(__file__).parent / "google-ads-9474097201.yml",
        ]

        for path in default_paths:
            path_obj = Path(path)
            if path_obj.exists():
                logger.info(f"Using default Google Ads config: {path_obj}")
                return str(path_obj)

        raise ConfigurationError(
            "GOOGLE_ADS_CONFIG_FILE environment variable not set and no default config found"
        )

    config_path = Path(config_file)
    if not config_path.exists():
        raise ConfigurationError(f"Google Ads config file not found: {config_file}")

    logger.info(f"Using Google Ads config file: {config_file}")
    return config_file


def get_data_sink() -> Optional[object]:
    """
    Initialize data sink based on STORAGE_TYPE environment variable.

    Returns:
        Data sink instance or None if storage not configured

    Raises:
        ConfigurationError: If storage configuration is invalid
    """
    storage_type = os.getenv("STORAGE_TYPE", "none").lower()

    if storage_type == "none":
        logger.warning("No storage backend configured (STORAGE_TYPE=none)")
        return None

    elif storage_type == "vertica":
        logger.info("Initializing Vertica storage backend")

        try:
            from social.infrastructure.database import VerticaDataSink
            from social.core.config import DatabaseConfig

            # Create database config from environment variables
            db_config = DatabaseConfig(
                host=os.getenv("VERTICA_HOST"),
                port=int(os.getenv("VERTICA_PORT", "5433")),
                database=os.getenv("VERTICA_DATABASE"),
                user=os.getenv("VERTICA_USER"),
                password=os.getenv("VERTICA_PASSWORD"),
            )

            # Check if running in test mode
            test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
            logger.info(f"Test mode: {test_mode}")

            return VerticaDataSink(config=db_config, test_mode=test_mode)
        except ImportError:
            raise ConfigurationError("Vertica dependencies not installed")
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize Vertica: {str(e)}")

    elif storage_type == "azure":
        logger.info("Initializing Azure Blob Storage backend")

        try:
            from social.infrastructure.azure_blob_manager import AzureBlobManager

            return AzureBlobManager(
                connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
                container_name=os.getenv("AZURE_CONTAINER_NAME", "google-ads-data"),
            )
        except ImportError:
            raise ConfigurationError("Azure dependencies not installed")
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize Azure: {str(e)}")

    else:
        raise ConfigurationError(f"Unknown storage type: {storage_type}")


def main() -> int:
    """
    Main entry point for Google Ads pipeline.

    Returns:
        Exit code (0 = success, non-zero = error)
    """
    # Setup logging
    setup_logging()
    logger.info("=" * 80)
    logger.info("Google Ads ETL Pipeline Starting")
    logger.info("=" * 80)

    # Initialize execution summary writer
    from datetime import datetime
    from shared.monitoring import ExecutionSummaryWriter

    summary_writer = ExecutionSummaryWriter(
        platform="google",
        storage_connection_string=os.getenv("SUMMARY_STORAGE_CONNECTION_STRING"),
    )

    pipeline_start = datetime.now()

    try:
        # Load configuration
        logger.info("Loading configuration...")
        config_path = get_config_path()
        config = load_config(config_path)

        # Get Google Ads credentials file
        google_config_file = get_google_ads_config_file()

        # Get environment variables
        manager_customer_id = os.getenv("GOOGLE_MANAGER_CUSTOMER_ID", "9474097201")
        api_version = os.getenv("GOOGLE_API_VERSION", API_VERSION)

        logger.info(f"Manager Customer ID: {manager_customer_id}")
        logger.info(f"API Version: {api_version}")

        # Initialize token provider (not directly used, kept for protocol compatibility)
        token_provider = FileBasedTokenProvider(
            platform="google",
            credentials_file=None  # Placeholder, not used for Google Ads
        )

        # Initialize data sink
        logger.info("Initializing data sink...")
        data_sink = get_data_sink()

        if data_sink:
            logger.success("Data sink initialized successfully")
        else:
            logger.warning("Running without data sink (data will not be persisted)")

        # Initialize pipeline
        logger.info("Initializing Google Ads pipeline...")
        start_time = datetime.now()

        pipeline = GooglePipeline(
            config=config,
            token_provider=token_provider,
            google_config_file=google_config_file,
            manager_customer_id=manager_customer_id,
            api_version=api_version,
            data_sink=data_sink,
        )

        logger.success("Pipeline initialized successfully")

        # Run all tables
        logger.info("Running pipeline for all tables...")
        results, errors = pipeline.run_all_tables(load_to_sink=(data_sink is not None))
        end_time = datetime.now()

        # Analyze results based on error tracking, not empty DataFrames
        tables_succeeded = {name: df for name, df in results.items() if name not in errors}
        tables_failed = list(errors.keys())
        total = len(results)

        logger.info("=" * 80)
        logger.info(f"Pipeline Execution Complete: {len(tables_succeeded)}/{total} tables successful")
        logger.info("=" * 80)

        # Close pipeline
        pipeline.close()

        # Write execution summary
        metadata = {
            "manager_customer_id": manager_customer_id,
            "api_version": api_version,
            "tables_successful": len(tables_succeeded),
            "tables_total": total,
            "tables_failed": len(tables_failed),
        }

        if not tables_failed:
            # All tables succeeded (even if some returned 0 rows)
            logger.success("All tables processed successfully")
            summary_writer.write_success(
                start_time=start_time,
                end_time=end_time,
                tables_processed=results,
                exit_code=0,
                metadata=metadata,
            )
            return 0
        elif not tables_succeeded:
            # All tables failed with exceptions
            logger.error("All tables failed")
            summary_writer.write_failure(
                start_time=start_time,
                end_time=end_time,
                error=f"All {len(tables_failed)} tables failed to process",
                exit_code=3,
            )
            return 3
        else:
            # Partial success: some tables succeeded, some failed with exceptions
            logger.warning(f"Partial success: {len(tables_succeeded)}/{total} tables completed")
            summary_writer.write_partial_success(
                start_time=start_time,
                end_time=end_time,
                tables_succeeded=tables_succeeded,
                tables_failed=tables_failed,
                errors=[{"table": name, "message": errors[name]} for name in tables_failed],
                exit_code=3,
                metadata=metadata,
            )
            return 3

    except ConfigurationError as e:
        logger.error(f"Configuration error: {str(e)}")
        summary_writer.write_failure(
            start_time=pipeline_start,
            end_time=datetime.now(),
            error=e,
            exit_code=1,
        )
        return 1

    except AuthenticationError as e:
        logger.error(f"Authentication error: {str(e)}")
        summary_writer.write_failure(
            start_time=pipeline_start,
            end_time=datetime.now(),
            error=e,
            exit_code=2,
        )
        return 2

    except PipelineError as e:
        logger.error(f"Pipeline execution error: {str(e)}")
        summary_writer.write_failure(
            start_time=pipeline_start,
            end_time=datetime.now(),
            error=e,
            exit_code=3,
        )
        return 3

    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        summary_writer.write_failure(
            start_time=pipeline_start,
            end_time=datetime.now(),
            error=e,
            exit_code=4,
        )
        return 4


if __name__ == "__main__":
    sys.exit(main())
