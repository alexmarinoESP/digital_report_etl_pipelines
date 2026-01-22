"""Social Media Advertising ETL Pipeline - Main Entry Point.

This script orchestrates the extraction, transformation, and loading of
advertising data from multiple platforms (LinkedIn, Google Ads) into
the Vertica database.

Designed for deployment as an Azure Container App Job, this script:
- Loads configuration from environment variables and files
- Manages authentication and token refresh
- Coordinates multi-platform data extraction
- Handles errors and retries
- Provides detailed logging

Usage:
    # Run all platforms, all tables
    python run_pipeline.py

    # Run specific platform
    python run_pipeline.py --platform linkedin

    # Run specific tables
    python run_pipeline.py --platform linkedin --tables linkedin_ads_campaign,linkedin_ads_insights

    # Test mode (writes to *_TEST tables)
    python run_pipeline.py --test-mode

    # Dry run (no database writes)
    python run_pipeline.py --dry-run

Environment Variables:
    TEST_MODE: Set to 'true' for test mode
    DRY_RUN: Set to 'true' for dry run
    VERTICA_HOST: Database host
    VERTICA_PORT: Database port
    VERTICA_DATABASE: Database name
    VERTICA_USER: Database user
    VERTICA_PASSWORD: Database password
"""

import sys
import argparse
from pathlib import Path
from typing import List, Optional, Dict
from datetime import datetime
from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from social.core.config import ConfigurationManager, AppConfig
from social.core.exceptions import SocialError, ConfigurationError, APIError
from social.core.constants import Platform, LOG_FORMAT, LOG_LEVEL_DEFAULT
from social.infrastructure.database import VerticaDataSink
from social.infrastructure.file_token_provider import FileBasedTokenProvider
from social.infrastructure.database_token_provider import DatabaseTokenProvider
from social.adapters.linkedin_adapter import LinkedInAdsAdapter
from social.adapters.google_adapter import GoogleAdsAdapter
from social.adapters.facebook_adapter import FacebookAdsAdapter


class SocialPipeline:
    """Main pipeline orchestrator for social media advertising data.

    This class coordinates the entire ETL process across multiple
    advertising platforms, handling configuration, authentication,
    data extraction, and error management.
    """

    def __init__(self, config: AppConfig):
        """Initialize the pipeline.

        Args:
            config: Application configuration
        """
        self.config = config
        self.data_sink = None
        self.token_providers: Dict[str, FileBasedTokenProvider] = {}
        self.adapters: Dict[str, any] = {}

    def initialize(self) -> None:
        """Initialize pipeline components.

        Sets up database connection, token providers, and platform adapters.

        Raises:
            ConfigurationError: If initialization fails
        """
        logger.info("Initializing Social Pipeline...")

        # Initialize database connection
        if self.config.database:
            logger.info(f"Connecting to database: {self.config.database.host}")
            self.data_sink = VerticaDataSink(
                config=self.config.database,
                test_mode=self.config.test_mode
            )
        else:
            if not self.config.dry_run:
                raise ConfigurationError("Database configuration required for non-dry-run mode")
            logger.warning("Running without database connection (dry run mode)")

        # Initialize platform adapters
        for platform_name, platform_config in self.config.platforms.items():
            try:
                logger.info(f"Initializing {platform_name} adapter...")

                # Get token provider
                token_provider = self._get_token_provider(platform_name)

                # Enrich platform config with credentials from file
                self._enrich_platform_config(platform_name, platform_config, token_provider)

                # Create platform-specific adapter
                if platform_name == Platform.LINKEDIN.value:
                    adapter = LinkedInAdsAdapter(
                        config=platform_config,
                        token_provider=token_provider,
                        data_sink=self.data_sink
                    )
                elif platform_name == Platform.GOOGLE.value:
                    adapter = GoogleAdsAdapter(
                        config=platform_config,
                        token_provider=token_provider,
                        data_sink=self.data_sink
                    )
                elif platform_name == Platform.FACEBOOK.value:
                    adapter = FacebookAdsAdapter(
                        config=platform_config,
                        token_provider=token_provider,
                        data_sink=self.data_sink
                    )
                else:
                    logger.warning(f"Unknown platform: {platform_name}, skipping...")
                    continue

                self.adapters[platform_name] = adapter
                logger.info(f"✓ {platform_name} adapter initialized")

            except Exception as e:
                logger.error(f"Failed to initialize {platform_name} adapter: {e}")
                # Continue with other platforms
                continue

        if not self.adapters:
            raise ConfigurationError("No platform adapters could be initialized")

        logger.info(f"Pipeline initialized with {len(self.adapters)} platform(s)")

    def _get_token_provider(self, platform: str):
        """Get or create token provider for a platform.

        Args:
            platform: Platform name

        Returns:
            TokenProvider instance (FileBasedTokenProvider or DatabaseTokenProvider)

        Raises:
            ConfigurationError: If token provider cannot be created
        """
        if platform not in self.token_providers:
            # LinkedIn uses database for tokens (access_token/refresh_token from DB)
            if platform == Platform.LINKEDIN.value and self.data_sink:
                logger.debug(f"Creating DatabaseTokenProvider for LinkedIn")
                self.token_providers[platform] = DatabaseTokenProvider(
                    platform=platform,
                    data_sink=self.data_sink
                )
            else:
                # Other platforms use file-based tokens
                logger.debug(f"Creating FileBasedTokenProvider for {platform}")
                self.token_providers[platform] = FileBasedTokenProvider(
                    platform=platform
                )

        return self.token_providers[platform]

    def _enrich_platform_config(
        self,
        platform_name: str,
        platform_config: any,
        token_provider: FileBasedTokenProvider
    ) -> None:
        """Enrich platform configuration with credentials from file.

        Args:
            platform_name: Platform name (linkedin, google, facebook)
            platform_config: Platform configuration to enrich
            token_provider: Token provider with credentials
        """
        if platform_name == Platform.FACEBOOK.value:
            # Facebook needs: app_id, app_secret, ad_account_ids
            platform_config.additional_config["app_id"] = token_provider.get_app_id()
            platform_config.additional_config["app_secret"] = token_provider.get_app_secret()
            platform_config.additional_config["ad_account_ids"] = token_provider.get_account_ids()
            logger.debug(f"Enriched Facebook config with {len(token_provider.get_account_ids())} account IDs")

        elif platform_name == Platform.GOOGLE.value:
            # Google needs: manager_id, config_file, api_version
            platform_config.additional_config["manager_customer_id"] = token_provider.get_manager_ids()[0] if token_provider.get_manager_ids() else None
            platform_config.additional_config["api_version"] = token_provider.get_additional_config("version", "v18")
            platform_config.additional_config["google_config_file"] = token_provider.get_additional_config("config_file")
            logger.debug(f"Enriched Google config with manager ID: {platform_config.additional_config['manager_customer_id']}")

        elif platform_name == Platform.LINKEDIN.value:
            # LinkedIn uses client_id and client_secret from token provider
            logger.debug("LinkedIn config enriched (uses client_id/secret from token provider)")

    def run(
        self,
        platforms: Optional[List[str]] = None,
        tables: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, int]]:
        """Run the pipeline for specified platforms and tables.

        Args:
            platforms: List of platform names to run (None = all)
            tables: List of specific tables to extract (None = all)

        Returns:
            Dictionary mapping platform -> table -> row count

        Raises:
            SocialError: If pipeline execution fails critically
        """
        start_time = datetime.now()
        logger.info("=" * 80)
        logger.info(f"Starting Social Pipeline Run at {start_time}")
        logger.info(f"Mode: {'TEST' if self.config.test_mode else 'PRODUCTION'}")
        logger.info(f"Dry Run: {self.config.dry_run}")
        logger.info("=" * 80)

        results = {}
        platforms_to_run = platforms if platforms else list(self.adapters.keys())

        for platform_name in platforms_to_run:
            if platform_name not in self.adapters:
                logger.warning(f"Platform {platform_name} not initialized, skipping...")
                continue

            logger.info(f"\n{'=' * 80}")
            logger.info(f"Processing Platform: {platform_name.upper()}")
            logger.info(f"{'=' * 80}")

            try:
                platform_results = self._run_platform(platform_name, tables)
                results[platform_name] = platform_results

                # Summary for this platform
                total_rows = sum(platform_results.values())
                logger.info(f"✓ {platform_name} completed: {total_rows} total rows across {len(platform_results)} tables")

            except Exception as e:
                logger.error(f"✗ {platform_name} failed: {e}")
                logger.exception(e)
                results[platform_name] = {"error": str(e)}
                # Continue with next platform

        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Start Time: {start_time}")
        logger.info(f"End Time: {end_time}")
        logger.info(f"Duration: {duration}")
        logger.info(f"\nResults by Platform:")

        for platform, tables_result in results.items():
            if isinstance(tables_result, dict) and "error" in tables_result:
                logger.error(f"  {platform}: FAILED - {tables_result['error']}")
            else:
                total = sum(tables_result.values())
                logger.info(f"  {platform}: {total} rows across {len(tables_result)} tables")
                for table, count in tables_result.items():
                    logger.info(f"    - {table}: {count} rows")

        logger.info("=" * 80)

        return results

    def _run_platform(self, platform_name: str, tables: Optional[List[str]] = None) -> Dict[str, int]:
        """Run extraction for a single platform.

        Args:
            platform_name: Name of the platform
            tables: Optional list of specific tables to extract

        Returns:
            Dictionary mapping table names to row counts

        Raises:
            APIError: If extraction fails
        """
        adapter = self.adapters[platform_name]
        results = {}

        # Get tables to extract
        tables_to_extract = tables if tables else adapter.get_all_tables()

        logger.info(f"Extracting {len(tables_to_extract)} tables: {', '.join(tables_to_extract)}")

        # Extract all tables (adapter handles dependency ordering)
        try:
            dataframes = adapter.extract_all_tables(tables=tables_to_extract)

            # Load each table to database
            for table_name, df in dataframes.items():
                row_count = len(df)

                if self.config.dry_run:
                    logger.info(f"[DRY RUN] Would load {row_count} rows to {table_name}")
                else:
                    logger.info(f"Loading {row_count} rows to {table_name}...")
                    loaded_count = self.data_sink.load(
                        df=df,
                        table_name=table_name,
                        mode="append"  # Use merge/upsert logic in the sink
                    )
                    logger.info(f"✓ Loaded {loaded_count} rows to {table_name}")
                    row_count = loaded_count

                results[table_name] = row_count

        except Exception as e:
            logger.error(f"Failed to extract/load tables for {platform_name}: {e}")
            raise

        return results

    def cleanup(self) -> None:
        """Clean up resources (close connections, etc.)."""
        logger.info("Cleaning up pipeline resources...")

        if self.data_sink:
            try:
                self.data_sink.close()
                logger.info("✓ Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}")


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the pipeline.

    Args:
        verbose: Enable debug-level logging
    """
    logger.remove()  # Remove default handler

    log_level = "DEBUG" if verbose else LOG_LEVEL_DEFAULT

    logger.add(
        sys.stderr,
        format=LOG_FORMAT,
        level=log_level,
        colorize=True,
    )

    # Also log to file
    logger.add(
        "logs/social_pipeline_{time:YYYY-MM-DD}.log",
        format=LOG_FORMAT,
        level=log_level,
        rotation="1 day",
        retention="30 days",
        compression="zip",
    )


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Social Media Advertising ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--platform",
        type=str,
        choices=["linkedin", "google", "facebook", "all"],
        default="all",
        help="Platform to run (default: all)",
    )

    parser.add_argument(
        "--tables",
        type=str,
        help="Comma-separated list of specific tables to extract (default: all)",
    )

    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Run in test mode (writes to *_TEST tables)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode (no database writes)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug-level logging",
    )

    return parser.parse_args()


def main() -> int:
    """Main entry point for the pipeline.

    Returns:
        Exit code (0 = success, non-zero = failure)
    """
    args = parse_arguments()

    # Setup logging
    setup_logging(verbose=args.verbose)

    logger.info("Social Media Advertising ETL Pipeline")
    logger.info(f"Version: 2.0.0 (SOLID Refactored)")

    try:
        # Load configuration
        config_manager = ConfigurationManager()
        config = config_manager.load_config(
            platform=None if args.platform == "all" else args.platform,
            test_mode=args.test_mode,
            dry_run=args.dry_run,
        )

        # Parse tables list
        tables = args.tables.split(",") if args.tables else None

        # Parse platforms list
        platforms = None if args.platform == "all" else [args.platform]

        # Initialize and run pipeline
        pipeline = SocialPipeline(config)
        pipeline.initialize()

        results = pipeline.run(platforms=platforms, tables=tables)

        # Check for any failures
        has_failures = any(
            isinstance(v, dict) and "error" in v
            for v in results.values()
        )

        # Cleanup
        pipeline.cleanup()

        if has_failures:
            logger.warning("Pipeline completed with some failures")
            return 1
        else:
            logger.info("Pipeline completed successfully!")
            return 0

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 2
    except APIError as e:
        logger.error(f"API error: {e}")
        return 3
    except SocialError as e:
        logger.error(f"Pipeline error: {e}")
        return 4
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        return 130
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
