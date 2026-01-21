"""Database connection module."""

from shared.connection.vertica import VerticaConnection

# Lazy import for Oracle to avoid requiring oracledb when not needed
# Use: from shared.connection.oracle import OracleConnection
# instead of importing from this __init__.py

__all__ = ["VerticaConnection"]
