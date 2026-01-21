"""
Oracle database connection module.
"""

from typing import Optional

import oracledb

from shared.connection.base import DatabaseConnection
from shared.utils.env import get_env_or_raise, get_env


class OracleConnection(DatabaseConnection):
    """Oracle database connection implementation."""

    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
        service: Optional[str] = None,
    ):
        """
        Initialize Oracle connection.
        If parameters are not provided, they are read from environment variables.
        """
        super().__init__(
            host=host or get_env_or_raise("ORACLE_HOST"),
            user=user or get_env_or_raise("ORACLE_USER"),
            password=password or get_env_or_raise("ORACLE_PASSWORD"),
            port=port or int(get_env("ORACLE_PORT", "1521")),
            service=service or get_env_or_raise("ORACLE_SERVICE"),
        )

    def get_connection_info(self) -> dict:
        """Return Oracle connection info dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "service": self.service,
        }

    def connect(self) -> oracledb.Connection:
        """
        Create and return an Oracle connection.

        Returns:
            Oracle connection object
        """
        dsn = f"{self.host}:{self.port}/{self.service}"
        return oracledb.connect(
            user=self.user,
            password=self.password,
            dsn=dsn,
        )

    def connect_to_oracle(self) -> oracledb.Connection:
        """Alias for connect() for backward compatibility."""
        return self.connect()
