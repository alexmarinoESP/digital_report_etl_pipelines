"""Social Ads Orchestrator Package.

This package provides the unified orchestrator for coordinating all 4 platform
pipelines: Microsoft, LinkedIn, Facebook, and Google Ads.

Key Components:
- OrchestratorConfig: Configuration management
- PlatformRegistry: Platform pipeline registration
- PlatformScheduler: Execution scheduling and dependencies
- ExecutionMonitor: Execution tracking and reporting
- SocialAdsOrchestrator: Main orchestrator coordinator

Quick Start:
    ```python
    from pathlib import Path
    from social.orchestrator import (
        load_orchestrator_config,
        create_default_registry,
        SocialAdsOrchestrator
    )

    # Load configuration
    config = load_orchestrator_config(Path("orchestrator_config.yml"))

    # Create registry and orchestrator
    registry = create_default_registry()
    orchestrator = SocialAdsOrchestrator(
        config=config,
        registry=registry,
        token_provider=token_provider,
        data_sink=data_sink
    )

    # Run all platforms
    result = orchestrator.run_all_platforms()
    ```

Architecture:
- Platform-agnostic coordination
- Protocol-based interfaces (no inheritance)
- Dependency injection for flexibility
- Configuration-driven execution
- Comprehensive monitoring and reporting

Version: 1.0.0
"""

from social.orchestrator.config import (
    OrchestratorConfig,
    PlatformConfig,
    RetryConfig,
    load_orchestrator_config,
    save_orchestrator_config,
)
from social.orchestrator.monitor import (
    ExecutionMonitor,
    ExecutionStatus,
    ExecutionSummary,
    PlatformExecution,
)
from social.orchestrator.orchestrator import (
    SocialAdsOrchestrator,
    OrchestratorResult,
)
from social.orchestrator.registry import (
    PlatformRegistry,
    create_default_registry,
    load_facebook_config,
    load_google_config,
    load_linkedin_config,
    load_microsoft_config,
)
from social.orchestrator.scheduler import PlatformScheduler

__version__ = "1.0.0"
__author__ = "Social Ads ETL Team"
__all__ = [
    # Configuration
    "OrchestratorConfig",
    "PlatformConfig",
    "RetryConfig",
    "load_orchestrator_config",
    "save_orchestrator_config",
    # Registry
    "PlatformRegistry",
    "create_default_registry",
    "load_facebook_config",
    "load_google_config",
    "load_linkedin_config",
    "load_microsoft_config",
    # Scheduler
    "PlatformScheduler",
    # Monitor
    "ExecutionMonitor",
    "ExecutionStatus",
    "ExecutionSummary",
    "PlatformExecution",
    # Orchestrator
    "SocialAdsOrchestrator",
    "OrchestratorResult",
    # Version
    "__version__",
]
