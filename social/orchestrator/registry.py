"""Platform Registry Module.

This module provides a registry for managing platform pipelines in the orchestrator.
It allows registration and instantiation of platform pipelines without tight coupling.

Key Features:
- Dynamic platform registration
- Factory pattern for pipeline instantiation
- Pre-registered support for all 4 platforms
- Flexible configuration loading per platform
- Type-safe protocol-based interfaces

Architecture:
- Registry pattern for loose coupling
- Factory methods for pipeline creation
- Protocol-based contracts (no inheritance required)
- Dependency injection for token providers and data sinks
"""

from pathlib import Path
from typing import Any, Callable, Dict, Optional

import yaml
from loguru import logger

from social.core.exceptions import ConfigurationError
from social.core.protocols import DataSink, TokenProvider


class PlatformRegistry:
    """Registry for platform pipelines.

    This class maintains a registry of available platforms and provides
    factory methods to instantiate pipeline instances with proper configuration.

    The registry stores:
    - Pipeline class references
    - Configuration loader functions
    - Platform metadata

    Example:
        ```python
        registry = PlatformRegistry()
        registry.register_platform("linkedin", LinkedInPipeline, load_linkedin_config)

        # Later, instantiate pipeline
        pipeline = registry.get_pipeline("linkedin", token_provider, data_sink)
        pipeline.run_all_tables()
        ```
    """

    def __init__(self):
        """Initialize the platform registry."""
        self._platforms: Dict[str, Dict[str, Any]] = {}
        logger.debug("PlatformRegistry initialized")

    def register_platform(
        self,
        name: str,
        pipeline_class: type,
        config_loader: Callable[[Path], Dict[str, Any]],
        config_path: Optional[Path] = None,
        additional_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a platform pipeline.

        Args:
            name: Platform name (microsoft, linkedin, facebook, google)
            pipeline_class: Pipeline class (not instantiated)
            config_loader: Function to load platform configuration
            config_path: Optional path to platform config file
            additional_params: Optional additional parameters for pipeline initialization

        Raises:
            ConfigurationError: If platform already registered or invalid parameters
        """
        if not name:
            raise ConfigurationError("Platform name cannot be empty")

        if name in self._platforms:
            logger.warning(f"Platform '{name}' already registered, overwriting")

        if not pipeline_class:
            raise ConfigurationError(f"Pipeline class cannot be None for platform '{name}'")

        if not config_loader:
            raise ConfigurationError(f"Config loader cannot be None for platform '{name}'")

        # Determine default config path if not provided
        if config_path is None:
            # Default: social/platforms/{name}/config_{name}_ads.yml
            module_dir = Path(__file__).parent.parent
            config_path = module_dir / "platforms" / name / f"config_{name}_ads.yml"

        self._platforms[name] = {
            "pipeline_class": pipeline_class,
            "config_loader": config_loader,
            "config_path": config_path,
            "additional_params": additional_params or {},
        }

        logger.info(f"Registered platform: {name} -> {pipeline_class.__name__}")

    def unregister_platform(self, name: str) -> None:
        """Unregister a platform.

        Args:
            name: Platform name to unregister

        Raises:
            ConfigurationError: If platform not registered
        """
        if name not in self._platforms:
            raise ConfigurationError(f"Platform '{name}' not registered")

        del self._platforms[name]
        logger.info(f"Unregistered platform: {name}")

    def is_registered(self, name: str) -> bool:
        """Check if a platform is registered.

        Args:
            name: Platform name

        Returns:
            True if platform is registered
        """
        return name in self._platforms

    def list_platforms(self) -> list[str]:
        """Get list of all registered platform names.

        Returns:
            List of platform names
        """
        return list(self._platforms.keys())

    def get_pipeline(
        self,
        name: str,
        token_provider: TokenProvider,
        data_sink: Optional[DataSink] = None,
        override_config: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Instantiate a platform pipeline.

        Args:
            name: Platform name
            token_provider: Token provider for authentication
            data_sink: Optional data sink for loading data
            override_config: Optional configuration to override loaded config

        Returns:
            Instantiated pipeline instance

        Raises:
            ConfigurationError: If platform not registered or instantiation fails
        """
        if name not in self._platforms:
            raise ConfigurationError(
                f"Platform '{name}' not registered. "
                f"Available platforms: {', '.join(self.list_platforms())}"
            )

        platform_info = self._platforms[name]
        pipeline_class = platform_info["pipeline_class"]
        config_loader = platform_info["config_loader"]
        config_path = platform_info["config_path"]
        additional_params = platform_info["additional_params"]

        try:
            # Load platform configuration
            logger.debug(f"Loading configuration for platform '{name}' from: {config_path}")
            config = config_loader(config_path)

            # Override with provided config if any
            if override_config:
                config.update(override_config)

            # Prepare initialization parameters
            init_params = {
                "config": config,
                "token_provider": token_provider,
                "data_sink": data_sink,
            }

            # Add platform-specific additional parameters
            init_params.update(additional_params)

            # Instantiate pipeline
            logger.debug(f"Instantiating pipeline for platform '{name}'")
            pipeline = pipeline_class(**init_params)

            logger.success(f"Pipeline created for platform '{name}'")
            return pipeline

        except Exception as e:
            logger.error(f"Failed to instantiate pipeline for '{name}': {e}")
            raise ConfigurationError(
                f"Failed to instantiate pipeline for '{name}': {e}"
            ) from e

    def get_pipeline_class(self, name: str) -> type:
        """Get pipeline class for a platform without instantiating.

        Args:
            name: Platform name

        Returns:
            Pipeline class

        Raises:
            ConfigurationError: If platform not registered
        """
        if name not in self._platforms:
            raise ConfigurationError(f"Platform '{name}' not registered")

        return self._platforms[name]["pipeline_class"]

    def get_config_path(self, name: str) -> Path:
        """Get configuration file path for a platform.

        Args:
            name: Platform name

        Returns:
            Path to configuration file

        Raises:
            ConfigurationError: If platform not registered
        """
        if name not in self._platforms:
            raise ConfigurationError(f"Platform '{name}' not registered")

        return self._platforms[name]["config_path"]


def load_linkedin_config(config_path: Path) -> Dict[str, Any]:
    """Load LinkedIn platform configuration.

    Args:
        config_path: Path to LinkedIn config YAML file

    Returns:
        Configuration dictionary

    Raises:
        ConfigurationError: If loading fails
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logger.debug(f"Loaded LinkedIn config from: {config_path}")
        return config
    except Exception as e:
        raise ConfigurationError(f"Failed to load LinkedIn config: {e}") from e


def load_facebook_config(config_path: Path) -> Dict[str, Any]:
    """Load Facebook platform configuration.

    Args:
        config_path: Path to Facebook config YAML file

    Returns:
        Configuration dictionary

    Raises:
        ConfigurationError: If loading fails
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logger.debug(f"Loaded Facebook config from: {config_path}")
        return config
    except Exception as e:
        raise ConfigurationError(f"Failed to load Facebook config: {e}") from e


def load_google_config(config_path: Path) -> Dict[str, Any]:
    """Load Google platform configuration.

    Args:
        config_path: Path to Google config YAML file

    Returns:
        Configuration dictionary

    Raises:
        ConfigurationError: If loading fails
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logger.debug(f"Loaded Google config from: {config_path}")
        return config
    except Exception as e:
        raise ConfigurationError(f"Failed to load Google config: {e}") from e


def load_microsoft_config(config_path: Path) -> Dict[str, Any]:
    """Load Microsoft platform configuration.

    Args:
        config_path: Path to Microsoft config YAML file

    Returns:
        Configuration dictionary (PlatformConfig structure)

    Raises:
        ConfigurationError: If loading fails
    """
    try:
        # Microsoft uses PlatformConfig dataclass, need special handling
        from social.core.config import ConfigurationManager

        # Load using configuration manager
        config_manager = ConfigurationManager(config_dir=config_path.parent)
        app_config = config_manager.load_config(platform="microsoft")

        # Return the platform config (not the full AppConfig)
        return app_config.get_platform_config("microsoft")

    except Exception as e:
        raise ConfigurationError(f"Failed to load Microsoft config: {e}") from e


def create_default_registry() -> PlatformRegistry:
    """Create a platform registry pre-registered with all 4 platforms.

    Returns:
        PlatformRegistry with Microsoft, LinkedIn, Facebook, and Google registered

    Example:
        ```python
        registry = create_default_registry()
        pipeline = registry.get_pipeline("linkedin", token_provider, data_sink)
        ```
    """
    registry = PlatformRegistry()

    try:
        # Import platform pipeline classes
        from social.platforms.microsoft.pipeline import MicrosoftAdsPipeline
        from social.platforms.linkedin.pipeline import LinkedInPipeline
        from social.platforms.facebook.pipeline import FacebookPipeline
        from social.platforms.google.pipeline import GooglePipeline

        # Register Microsoft Ads
        registry.register_platform(
            name="microsoft",
            pipeline_class=MicrosoftAdsPipeline,
            config_loader=load_microsoft_config,
        )

        # Register LinkedIn Ads
        registry.register_platform(
            name="linkedin",
            pipeline_class=LinkedInPipeline,
            config_loader=load_linkedin_config,
        )

        # Register Facebook Ads
        # Note: Facebook requires additional parameters (ad_account_ids, app_id, app_secret)
        # These should be provided via environment variables when instantiating
        registry.register_platform(
            name="facebook",
            pipeline_class=FacebookPipeline,
            config_loader=load_facebook_config,
            additional_params={
                "ad_account_ids": [],  # Will be populated from environment
                "app_id": "",  # Will be populated from environment
                "app_secret": "",  # Will be populated from environment
            },
        )

        # Register Google Ads
        # Note: Google requires google_config_file and manager_customer_id
        # These should be provided via environment variables when instantiating
        registry.register_platform(
            name="google",
            pipeline_class=GooglePipeline,
            config_loader=load_google_config,
            additional_params={
                "google_config_file": "",  # Will be populated from environment
                "manager_customer_id": "9474097201",  # Default MCC ID
            },
        )

        logger.success(f"Default registry created with {len(registry.list_platforms())} platforms")
        return registry

    except ImportError as e:
        logger.error(f"Failed to import platform pipeline: {e}")
        raise ConfigurationError(f"Failed to create default registry: {e}") from e
