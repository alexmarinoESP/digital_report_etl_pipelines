"""Infrastructure layer for external dependencies.

This layer contains implementations for interacting with external systems
like databases, file storage, APIs, etc.
"""

from social.infrastructure.database import VerticaDataSink

__all__ = [
    "VerticaDataSink",
]
