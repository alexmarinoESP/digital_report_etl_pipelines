"""Orchestrator Configuration Module.

This module provides configuration management for the Social Ads Orchestrator,
including platform settings, execution order, dependencies, and retry policies.

Key Features:
- Platform-specific configuration with timeouts and priorities
- Retry policies with exponential backoff
- Parallel execution group definitions
- YAML-based configuration loading
- Full type safety with dataclasses

Architecture:
- Dataclass-based configuration for type safety
- YAML configuration file support
- Validation methods for configuration integrity
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from loguru import logger

from social.core.exceptions import ConfigurationError


@dataclass
class RetryConfig:
    """Retry configuration for platform execution.

    Attributes:
        max_attempts: Maximum number of retry attempts (default: 3)
        backoff_seconds: Initial backoff time in seconds (default: 60)
        backoff_multiplier: Multiplier for exponential backoff (default: 2.0)
        max_backoff_seconds: Maximum backoff time (default: 600)
    """
    max_attempts: int = 3
    backoff_seconds: int = 60
    backoff_multiplier: float = 2.0
    max_backoff_seconds: int = 600

    def get_backoff_time(self, attempt: int) -> int:
        """Calculate backoff time for a given attempt.

        Args:
            attempt: Attempt number (1-indexed)

        Returns:
            Backoff time in seconds
        """
        backoff = self.backoff_seconds * (self.backoff_multiplier ** (attempt - 1))
        return min(int(backoff), self.max_backoff_seconds)

    def validate(self) -> None:
        """Validate retry configuration.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if self.max_attempts < 1:
            raise ConfigurationError("max_attempts must be at least 1")
        if self.backoff_seconds < 0:
            raise ConfigurationError("backoff_seconds must be non-negative")
        if self.backoff_multiplier < 1.0:
            raise ConfigurationError("backoff_multiplier must be at least 1.0")


@dataclass
class PlatformConfig:
    """Configuration for a single platform.

    Attributes:
        name: Platform name (microsoft, linkedin, facebook, google)
        enabled: Whether platform is enabled for execution
        priority: Execution priority (lower = higher priority)
        timeout: Maximum execution time in seconds
        retry: Retry configuration
        dependencies: List of platform names that must complete first
    """
    name: str
    enabled: bool = True
    priority: int = 1
    timeout: int = 1800  # 30 minutes default
    retry: RetryConfig = field(default_factory=RetryConfig)
    dependencies: List[str] = field(default_factory=list)

    def validate(self) -> None:
        """Validate platform configuration.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not self.name:
            raise ConfigurationError("Platform name cannot be empty")

        valid_platforms = ["microsoft", "linkedin", "facebook", "google"]
        if self.name not in valid_platforms:
            raise ConfigurationError(
                f"Invalid platform name: '{self.name}'. "
                f"Must be one of: {', '.join(valid_platforms)}"
            )

        if self.priority < 1:
            raise ConfigurationError("Priority must be at least 1")

        if self.timeout < 1:
            raise ConfigurationError("Timeout must be at least 1 second")

        # Validate retry config
        self.retry.validate()


@dataclass
class OrchestratorConfig:
    """Main orchestrator configuration.

    Attributes:
        platforms: Dictionary of platform configurations
        parallel_execution: Enable parallel execution
        max_parallel: Maximum number of platforms to run in parallel
        continue_on_failure: Continue with other platforms if one fails
        global_timeout: Global timeout for all platforms in seconds
        parallel_groups: Groups of platforms that can run in parallel
    """
    platforms: Dict[str, PlatformConfig] = field(default_factory=dict)
    parallel_execution: bool = True
    max_parallel: int = 2
    continue_on_failure: bool = True
    global_timeout: int = 7200  # 2 hours default
    parallel_groups: List[List[str]] = field(default_factory=list)

    def get_enabled_platforms(self) -> List[PlatformConfig]:
        """Get list of enabled platforms sorted by priority.

        Returns:
            List of enabled PlatformConfig instances sorted by priority
        """
        enabled = [p for p in self.platforms.values() if p.enabled]
        return sorted(enabled, key=lambda p: p.priority)

    def get_platform(self, name: str) -> Optional[PlatformConfig]:
        """Get configuration for a specific platform.

        Args:
            name: Platform name

        Returns:
            PlatformConfig instance or None if not found
        """
        return self.platforms.get(name)

    def validate(self) -> None:
        """Validate orchestrator configuration.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not self.platforms:
            raise ConfigurationError("No platforms configured")

        if self.max_parallel < 1:
            raise ConfigurationError("max_parallel must be at least 1")

        if self.global_timeout < 1:
            raise ConfigurationError("global_timeout must be at least 1 second")

        # Validate each platform
        for platform_name, platform_config in self.platforms.items():
            try:
                platform_config.validate()
            except ConfigurationError as e:
                raise ConfigurationError(
                    f"Invalid configuration for platform '{platform_name}': {e}"
                )

        # Validate dependencies exist
        all_platforms = set(self.platforms.keys())
        for platform_name, platform_config in self.platforms.items():
            for dep in platform_config.dependencies:
                if dep not in all_platforms:
                    raise ConfigurationError(
                        f"Platform '{platform_name}' has unknown dependency: '{dep}'"
                    )

        # Validate parallel groups
        for group_idx, group in enumerate(self.parallel_groups):
            for platform_name in group:
                if platform_name not in all_platforms:
                    raise ConfigurationError(
                        f"Parallel group {group_idx} contains unknown platform: '{platform_name}'"
                    )


def load_orchestrator_config(config_path: Path) -> OrchestratorConfig:
    """Load orchestrator configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        OrchestratorConfig instance

    Raises:
        ConfigurationError: If configuration loading or validation fails

    Example YAML structure:
        ```yaml
        orchestrator:
          parallel_execution: true
          max_parallel: 2
          continue_on_failure: true
          global_timeout: 7200

        platforms:
          - name: microsoft
            enabled: true
            priority: 1
            timeout: 1800
            retry:
              max_attempts: 2
              backoff_seconds: 60
            dependencies: []
        ```
    """
    try:
        logger.info(f"Loading orchestrator configuration from: {config_path}")

        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")

        # Load YAML
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)

        if not yaml_data:
            raise ConfigurationError("Configuration file is empty")

        # Extract orchestrator settings
        orchestrator_settings = yaml_data.get("orchestrator", {})

        # Parse platform configurations
        platforms_data = yaml_data.get("platforms", [])
        if not platforms_data:
            raise ConfigurationError("No platforms defined in configuration")

        platforms = {}
        for platform_data in platforms_data:
            # Extract retry config if present
            retry_data = platform_data.get("retry", {})
            retry_config = RetryConfig(
                max_attempts=retry_data.get("max_attempts", 3),
                backoff_seconds=retry_data.get("backoff_seconds", 60),
                backoff_multiplier=retry_data.get("backoff_multiplier", 2.0),
                max_backoff_seconds=retry_data.get("max_backoff_seconds", 600),
            )

            # Create platform config
            platform_name = platform_data.get("name")
            if not platform_name:
                logger.warning("Skipping platform without name")
                continue

            platform_config = PlatformConfig(
                name=platform_name,
                enabled=platform_data.get("enabled", True),
                priority=platform_data.get("priority", 1),
                timeout=platform_data.get("timeout", 1800),
                retry=retry_config,
                dependencies=platform_data.get("dependencies", []),
            )

            platforms[platform_name] = platform_config

        # Extract parallel groups
        parallel_groups = yaml_data.get("parallel_groups", [])

        # Create orchestrator config
        config = OrchestratorConfig(
            platforms=platforms,
            parallel_execution=orchestrator_settings.get("parallel_execution", True),
            max_parallel=orchestrator_settings.get("max_parallel", 2),
            continue_on_failure=orchestrator_settings.get("continue_on_failure", True),
            global_timeout=orchestrator_settings.get("global_timeout", 7200),
            parallel_groups=parallel_groups,
        )

        # Validate configuration
        config.validate()

        logger.success(f"Configuration loaded successfully: {len(platforms)} platforms configured")
        return config

    except yaml.YAMLError as e:
        logger.error(f"Failed to parse YAML: {e}")
        raise ConfigurationError(f"Invalid YAML format: {e}")

    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise ConfigurationError(f"Configuration loading failed: {e}")


def save_orchestrator_config(config: OrchestratorConfig, config_path: Path) -> None:
    """Save orchestrator configuration to YAML file.

    Args:
        config: OrchestratorConfig instance to save
        config_path: Path where to save the configuration

    Raises:
        ConfigurationError: If saving fails
    """
    try:
        # Validate before saving
        config.validate()

        # Build YAML structure
        yaml_data = {
            "orchestrator": {
                "parallel_execution": config.parallel_execution,
                "max_parallel": config.max_parallel,
                "continue_on_failure": config.continue_on_failure,
                "global_timeout": config.global_timeout,
            },
            "platforms": [
                {
                    "name": p.name,
                    "enabled": p.enabled,
                    "priority": p.priority,
                    "timeout": p.timeout,
                    "retry": {
                        "max_attempts": p.retry.max_attempts,
                        "backoff_seconds": p.retry.backoff_seconds,
                        "backoff_multiplier": p.retry.backoff_multiplier,
                        "max_backoff_seconds": p.retry.max_backoff_seconds,
                    },
                    "dependencies": p.dependencies,
                }
                for p in config.platforms.values()
            ],
            "parallel_groups": config.parallel_groups,
        }

        # Save to file
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False)

        logger.success(f"Configuration saved to: {config_path}")

    except Exception as e:
        logger.error(f"Failed to save configuration: {e}")
        raise ConfigurationError(f"Configuration saving failed: {e}")
