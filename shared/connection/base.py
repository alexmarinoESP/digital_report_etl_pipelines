"""
Base database connection module.
Provides abstract interface for database connections (DIP).
"""

from abc import ABC, abstractmethod
from typing import Any, Optional
from contextlib import contextmanager


class DatabaseConnection(ABC):
    """Abstract base class for database connections."""

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        port: int,
        database: Optional[str] = None,
        service: Optional[str] = None,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.service = service

    @abstractmethod
    def connect(self) -> Any:
        """Create and return a database connection."""
        pass

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        connection = self.connect()
        try:
            yield connection
        finally:
            connection.close()

    @abstractmethod
    def get_connection_info(self) -> dict:
        """Return connection info dictionary."""
        pass
