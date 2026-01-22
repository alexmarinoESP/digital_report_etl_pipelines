"""Configuration management for the social module.

This module provides a unified configuration system with clear precedence:
CLI arguments > Environment variables > YAML files > Database > Defaults

The configuration is type-safe using dataclasses and supports validation.
"""

import os
import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, Optional, List
from loguru import logger

from social.core.exceptions import ConfigurationError
from social.core.constants import (
    DATABASE_SCHEMA,
    DATABASE_TEST_SUFFIX,
    DEFAULT_COMPANY_ID,
    ENV_TEST_MODE,
    ENV_DRY_RUN,
    ENV_DATABASE_HOST,
    ENV_DATABASE_PORT,
    ENV_DATABASE_NAME,
    ENV_DATABASE_USER,
    ENV_DATABASE_PASSWORD,
)


@dataclass
class DatabaseConfig:
    """Database connection configuration."""

    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = DATABASE_SCHEMA

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create configuration from environment variables.

        Returns:
            DatabaseConfig instance

        Raises:
            ConfigurationError: If required environment variables are missing
        """
        required_vars = {
            "host": ENV_DATABASE_HOST,
            "port": ENV_DATABASE_PORT,
            "database": ENV_DATABASE_NAME,
            "user": ENV_DATABASE_USER,
            "password": ENV_DATABASE_PASSWORD,
        }

        config_values = {}
        missing_vars = []

        for key, env_var in required_vars.items():
            value = os.getenv(env_var)
            if value is None:
                missing_vars.append(env_var)
            else:
                # Convert port to int
                if key == "port":
                    try:
                        value = int(value)
                    except ValueError:
                        raise ConfigurationError(
                            f"Invalid port number: {value}",
                            details={"env_var": env_var}
                        )
                config_values[key] = value

        if missing_vars:
            raise ConfigurationError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        return cls(**config_values)


@dataclass
class TableConfig:
    """Configuration for a specific table/entity."""

    name: str
    endpoint: str
    request_type: str = "GET"
    page_size: int = 100
    fields: Optional[List[str]] = None
    processing_steps: List[str] = field(default_factory=list)
    additional_params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.name:
            raise ConfigurationError("Table name cannot be empty")
        if not self.endpoint:
            raise ConfigurationError(f"Endpoint not defined for table: {self.name}")


@dataclass
class PlatformConfig:
    """Configuration for an advertising platform."""

    name: str
    api_base_url: str
    api_version: str
    tables: Dict[str, TableConfig] = field(default_factory=dict)
    account_to_company_mapping: Dict[str, int] = field(default_factory=dict)
    default_company_id: int = DEFAULT_COMPANY_ID
    additional_config: Dict[str, Any] = field(default_factory=dict)

    def get_table_config(self, table_name: str) -> TableConfig:
        """Get configuration for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            TableConfig instance

        Raises:
            ConfigurationError: If table configuration not found
        """
        if table_name not in self.tables:
            raise ConfigurationError(
                f"Table '{table_name}' not found in platform '{self.name}' configuration"
            )
        return self.tables[table_name]

    def get_company_id(self, account_id: str) -> int:
        """Get company ID for an account.

        Args:
            account_id: Account identifier

        Returns:
            Company ID (integer)
        """
        return self.account_to_company_mapping.get(account_id, self.default_company_id)


@dataclass
class AppConfig:
    """Application-wide configuration."""

    test_mode: bool = False
    dry_run: bool = False
    platforms: Dict[str, PlatformConfig] = field(default_factory=dict)
    database: Optional[DatabaseConfig] = None

    def get_table_suffix(self) -> str:
        """Get table name suffix for test mode.

        Returns:
            Empty string in production, '_TEST' in test mode
        """
        return DATABASE_TEST_SUFFIX if self.test_mode else ""

    def get_platform_config(self, platform_name: str) -> PlatformConfig:
        """Get configuration for a platform.

        Args:
            platform_name: Name of the platform (linkedin, google, etc.)

        Returns:
            PlatformConfig instance

        Raises:
            ConfigurationError: If platform not configured
        """
        if platform_name not in self.platforms:
            raise ConfigurationError(
                f"Platform '{platform_name}' not configured. "
                f"Available platforms: {', '.join(self.platforms.keys())}"
            )
        return self.platforms[platform_name]


class ConfigurationManager:
    """Manages configuration loading from multiple sources with precedence.

    Configuration precedence (highest to lowest):
    1. CLI arguments (passed to methods)
    2. Environment variables
    3. YAML configuration files
    4. Database configuration
    5. Default values
    """

    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize the configuration manager.

        Args:
            config_dir: Directory containing YAML configuration files.
                       Defaults to social/platforms/{platform}/ directories.
        """
        self.config_dir = config_dir
        self._app_config: Optional[AppConfig] = None

    def load_config(
        self,
        platform: Optional[str] = None,
        test_mode: Optional[bool] = None,
        dry_run: Optional[bool] = None,
    ) -> AppConfig:
        """Load application configuration.

        Args:
            platform: Specific platform to load (or None for all)
            test_mode: Override test mode setting
            dry_run: Override dry run setting

        Returns:
            AppConfig instance with loaded configuration

        Raises:
            ConfigurationError: If configuration loading fails
        """
        # Start with environment variables
        app_config = AppConfig(
            test_mode=self._get_bool_env(ENV_TEST_MODE, False),
            dry_run=self._get_bool_env(ENV_DRY_RUN, False),
        )

        # Override with CLI arguments if provided
        if test_mode is not None:
            app_config.test_mode = test_mode
        if dry_run is not None:
            app_config.dry_run = dry_run

        # Load database configuration
        try:
            app_config.database = DatabaseConfig.from_env()
        except ConfigurationError as e:
            logger.warning(f"Database configuration not available: {e}")

        # Load platform configurations
        if platform:
            platforms_to_load = [platform]
        else:
            platforms_to_load = ["linkedin", "google", "facebook"]

        for platform_name in platforms_to_load:
            try:
                platform_config = self._load_platform_config(platform_name)
                app_config.platforms[platform_name] = platform_config
            except ConfigurationError as e:
                logger.error(f"Failed to load {platform_name} configuration: {e}")

        self._app_config = app_config
        return app_config

    def _load_platform_config(self, platform: str) -> PlatformConfig:
        """Load configuration for a specific platform.

        Args:
            platform: Platform name (linkedin, google, etc.)

        Returns:
            PlatformConfig instance

        Raises:
            ConfigurationError: If configuration file not found or invalid
        """
        # Determine config file path
        if self.config_dir:
            config_path = self.config_dir / f"config_{platform}_ads.yml"
        else:
            # Default to platform directory
            module_dir = Path(__file__).parent.parent
            config_path = module_dir / "platforms" / platform / f"config_{platform}_ads.yml"

        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")

        # Load YAML file
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                yaml_config = yaml.safe_load(f)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to parse YAML configuration: {config_path}",
                details={"error": str(e)}
            )

        # Extract platform-level settings
        platform_settings = yaml_config.get("platform", {})

        # Build table configurations
        tables = {}
        for table_name, table_data in yaml_config.items():
            if table_name == "platform" or not isinstance(table_data, dict):
                continue

            # Determine endpoint based on platform structure
            # LinkedIn uses "request", Google uses "queryget", Facebook uses "type"
            endpoint = table_data.get("request") or table_data.get("queryget") or table_data.get("type") or table_name

            # Convert endpoint to string if it's a list (for Google queryget)
            if isinstance(endpoint, list):
                endpoint = ",".join(endpoint)

            # Build additional_params - keep "type" for Facebook/Google platforms
            # as they use it for API method names
            exclude_keys = ["request", "pageSize", "fields", "processing"]
            if platform == "linkedin":
                exclude_keys.append("type")  # LinkedIn doesn't need type in additional_params

            tables[table_name] = TableConfig(
                name=table_name,
                endpoint=str(endpoint),
                request_type=table_data.get("type", "GET"),
                page_size=int(table_data.get("pageSize", 100)),
                fields=table_data.get("fields"),
                processing_steps=table_data.get("processing", []),
                additional_params={
                    k: v for k, v in table_data.items()
                    if k not in exclude_keys
                }
            )

        # Load company mapping if available
        mapping_file = config_path.parent / f"{platform}_company_mapping.yml"
        account_to_company = {}
        if mapping_file.exists():
            try:
                with open(mapping_file, "r", encoding="utf-8") as f:
                    mapping_data = yaml.safe_load(f)
                    account_to_company = mapping_data.get("account_to_company", {})
            except Exception as e:
                logger.warning(f"Failed to load company mapping: {e}")

        return PlatformConfig(
            name=platform,
            api_base_url=platform_settings.get("api_base_url", ""),
            api_version=platform_settings.get("api_version", ""),
            tables=tables,
            account_to_company_mapping=account_to_company,
        )

    def _get_bool_env(self, env_var: str, default: bool = False) -> bool:
        """Get boolean value from environment variable.

        Args:
            env_var: Environment variable name
            default: Default value if not set

        Returns:
            Boolean value
        """
        value = os.getenv(env_var)
        if value is None:
            return default
        return value.lower() in ("true", "1", "yes", "on")

    def get_config(self) -> AppConfig:
        """Get the current application configuration.

        Returns:
            AppConfig instance

        Raises:
            ConfigurationError: If configuration not loaded yet
        """
        if self._app_config is None:
            raise ConfigurationError(
                "Configuration not loaded. Call load_config() first."
            )
        return self._app_config
