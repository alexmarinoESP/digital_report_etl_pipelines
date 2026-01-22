"""Core abstractions and interfaces for the social module."""

from social.core.protocols import (
    DataSource,
    DataSink,
    TokenProvider,
    AdsPlatformClient,
    DataProcessor,
    ConfigProvider,
)
from social.core.exceptions import (
    SocialError,
    AuthenticationError,
    APIError,
    ConfigurationError,
    DataValidationError,
    DatabaseError,
)
from social.core.config import (
    PlatformConfig,
    TableConfig,
    DatabaseConfig,
    AppConfig,
    ConfigurationManager,
)

__all__ = [
    # Protocols
    "DataSource",
    "DataSink",
    "TokenProvider",
    "AdsPlatformClient",
    "DataProcessor",
    "ConfigProvider",
    # Exceptions
    "SocialError",
    "AuthenticationError",
    "APIError",
    "ConfigurationError",
    "DataValidationError",
    "DatabaseError",
    # Configuration
    "PlatformConfig",
    "TableConfig",
    "DatabaseConfig",
    "AppConfig",
    "ConfigurationManager",
]
