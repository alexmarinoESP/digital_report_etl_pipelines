"""
Shared module for digital report ETL pipelines.
Contains common utilities, database connections, and storage handlers.
"""

from shared.connection.vertica import VerticaConnection
from shared.utils.logging import setup_logging
from shared.utils.files import dump_files, read_files, create_folder
from shared.utils.env import get_env, get_env_or_raise

# Lazy imports for modules with optional dependencies:
# - OracleConnection: requires oracledb (newsletter only)
# - S3Handler: requires minio/boto3 (newsletter only)
# Use direct imports when needed:
#   from shared.connection.oracle import OracleConnection
#   from shared.storage.s3_handler import S3Handler

__all__ = [
    "VerticaConnection",
    "setup_logging",
    "dump_files",
    "read_files",
    "create_folder",
    "get_env",
    "get_env_or_raise",
]
