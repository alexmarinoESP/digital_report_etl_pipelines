"""Infrastructure layer for external dependencies.

This layer contains implementations for interacting with external systems
like databases, file storage, APIs, etc.
"""

from social.infrastructure.database import VerticaDataSink
from social.infrastructure.token_provider import DatabaseTokenProvider

__all__ = [
    "VerticaDataSink",
    "DatabaseTokenProvider",
]
