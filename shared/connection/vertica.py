"""
Vertica database connection module.
"""

import logging
from typing import Optional

import vertica_python
from vertica_python.vertica.connection import Connection

from shared.connection.base import DatabaseConnection
from shared.utils.env import get_env_or_raise, get_env


class VerticaConnection(DatabaseConnection):
    """Vertica database connection implementation."""

    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
    ):
        """
        Initialize Vertica connection.
        If parameters are not provided, they are read from environment variables.
        """
        super().__init__(
            host=host or get_env_or_raise("VERTICA_HOST"),
            user=user or get_env_or_raise("VERTICA_USER"),
            password=password or get_env_or_raise("VERTICA_PASSWORD"),
            port=port or int(get_env("VERTICA_PORT", "5433")),
            database=database or get_env_or_raise("VERTICA_DATABASE"),
        )

    def get_connection_info(self) -> dict:
        """Return Vertica connection info dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "unicode_error": "replace",
            "log_level": logging.ERROR,
        }

    def connect(self) -> Connection:
        """
        Create and return a Vertica connection.

        Returns:
            Vertica connection object
        """
        return vertica_python.connect(**self.get_connection_info())

    def connect_to_vertica(self) -> Connection:
        """Alias for connect() for backward compatibility."""
        return self.connect()
