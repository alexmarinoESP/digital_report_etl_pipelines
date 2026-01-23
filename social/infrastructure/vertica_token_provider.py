"""
Vertica-based Token Provider.

This module provides a token provider that retrieves OAuth tokens from a Vertica database,
following the EXACT pattern used in the old social_posts project.

IMPORTANT - Verified from social_posts project:
============================================
Table: ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE
Query (from social_posts/connection/templatesql.py line 87-92):
    SELECT ACCESS_TOKEN, REFRESH_TOKEN, EXPIRES
    FROM ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE
    WHERE SOCIAL='LinkedinADS' AND
    ROW_LOADED_DATE IN (SELECT MAX(G.ROW_LOADED_DATE) from ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE G)

Schema structure (from generate_token_linkedin_workaround.py line 86-95):
  - SOCIAL: VARCHAR ('LinkedinADS')
  - ACCESS_TOKEN: VARCHAR
  - REFRESH_TOKEN: VARCHAR
  - EXPIRES: TIMESTAMP
  - ROW_LOADED_DATE: TIMESTAMP
"""

from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from loguru import logger

from social.core.exceptions import AuthenticationError
from social.core.protocols import TokenProvider


class VerticaTokenProvider(TokenProvider):
    """
    Token provider that retrieves tokens from Vertica database.

    This implementation replicates the EXACT behavior of the old social_posts project
    which stored LinkedIn OAuth tokens in ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE table.

    The platform name "linkedin" is converted to "LinkedinADS" for the SOCIAL column.

    Attributes:
        platform: Platform name (e.g., "linkedin")
        host: Vertica host
        user: Vertica user
        password: Vertica password
        database: Vertica database
        schema: Vertica schema (ESPDM for tokens)
        table: Token table name (SOCIAL_ADS_POSTS_ACCESS_CODE)
    """

    # Mapping of platform names to SOCIAL column values (from social_posts)
    PLATFORM_MAPPING = {
        "linkedin": "LinkedinADS",
        "facebook": "FacebookADS",  # If needed in future
        "google": "GoogleADS",      # If needed in future
    }

    def __init__(
        self,
        platform: str,
        host: str,
        user: str,
        password: str,
        database: str,
        schema: str = "ESPDM",
        table: str = "SOCIAL_ADS_POSTS_ACCESS_CODE",
    ):
        """
        Initialize Vertica token provider.

        Args:
            platform: Platform name (e.g., "linkedin")
            host: Vertica host
            user: Vertica username
            password: Vertica password
            database: Vertica database name
            schema: Vertica schema (default: "ESPDM" as per social_posts)
            table: Token table name (default: "SOCIAL_ADS_POSTS_ACCESS_CODE")
        """
        self.platform = platform
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.table = table
        self._cached_token: Optional[Dict[str, str]] = None

        # Get the SOCIAL column value for this platform
        self.social_value = self.PLATFORM_MAPPING.get(platform, f"{platform}ADS")

        logger.info(f"VerticaTokenProvider initialized for {platform} (schema={schema}, table={table}, social={self.social_value})")

    def get_access_token(self) -> str:
        """
        Retrieve access token from Vertica database using EXACT query from social_posts.

        Query from social_posts/connection/templatesql.py:
            SELECT ACCESS_TOKEN, REFRESH_TOKEN, EXPIRES
            FROM ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE
            WHERE SOCIAL='LinkedinADS' AND
            ROW_LOADED_DATE IN (SELECT MAX(G.ROW_LOADED_DATE) from ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE G)

        Returns:
            Access token string

        Raises:
            AuthenticationError: If token cannot be retrieved
        """
        try:
            import vertica_python

            # Connect to Vertica (same parameters as social_posts/connection/connectdb.py line 818-819)
            conn_info = {
                "host": self.host,
                "port": 5433,
                "user": self.user,
                "password": self.password,
                "database": self.database,
                "autocommit": True,
            }

            with vertica_python.connect(**conn_info) as conn:
                cursor = conn.cursor()

                # EXACT query from social_posts/connection/templatesql.py line 87-92
                query = f"""
                    SELECT ACCESS_TOKEN, REFRESH_TOKEN, EXPIRES
                    FROM {self.schema}.{self.table}
                    WHERE SOCIAL='{self.social_value}' AND
                    ROW_LOADED_DATE IN (SELECT MAX(G.ROW_LOADED_DATE) from {self.schema}.{self.table} G)
                """

                logger.debug(f"Executing query: {query.strip()}")
                cursor.execute(query)
                result = cursor.fetchone()

                if not result:
                    raise AuthenticationError(
                        f"No token found in database for platform: {self.platform} (SOCIAL='{self.social_value}')",
                        details={
                            "platform": self.platform,
                            "table": f"{self.schema}.{self.table}",
                            "social_value": self.social_value
                        },
                    )

                # Unpack result (ACCESS_TOKEN, REFRESH_TOKEN, EXPIRES)
                access_token, refresh_token, expires = result

                # Cache the full token data
                self._cached_token = {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "expires": str(expires) if expires else None,
                }

                logger.info(f"Retrieved token from Vertica for {self.platform} (SOCIAL='{self.social_value}', expires: {expires})")

                return access_token

        except ImportError:
            raise AuthenticationError(
                "vertica_python package not installed",
                details={"platform": self.platform, "required_package": "vertica-python"},
            )
        except Exception as e:
            raise AuthenticationError(
                f"Failed to retrieve token from Vertica: {str(e)}",
                details={"platform": self.platform, "error": str(e)},
            )

    def get_refresh_token(self) -> Optional[str]:
        """
        Get refresh token (cached from last get_access_token call).

        Returns:
            Refresh token if available, None otherwise
        """
        if not self._cached_token:
            # Force token retrieval
            self.get_access_token()

        return self._cached_token.get("refresh_token") if self._cached_token else None

    def get_token_metadata(self) -> Dict[str, str]:
        """
        Get full token metadata from database.

        Returns:
            Dictionary with access_token, refresh_token, expires
        """
        if not self._cached_token:
            # Force token retrieval
            self.get_access_token()

        return self._cached_token or {}

    @classmethod
    def from_env(cls, platform: str = "linkedin") -> "VerticaTokenProvider":
        """
        Create VerticaTokenProvider from environment variables.

        Args:
            platform: Platform name (default: "linkedin")

        Returns:
            Configured VerticaTokenProvider instance

        Environment Variables:
            VERTICA_HOST: Vertica host
            VERTICA_USER: Vertica username
            VERTICA_PASSWORD: Vertica password
            VERTICA_DATABASE: Vertica database name
            VERTICA_TOKEN_SCHEMA: Vertica schema for tokens (optional, default: "ESPDM")
        """
        import os

        host = os.getenv("VERTICA_HOST")
        user = os.getenv("VERTICA_USER")
        password = os.getenv("VERTICA_PASSWORD")
        database = os.getenv("VERTICA_DATABASE")
        # Use ESPDM as default schema for tokens (as per social_posts)
        schema = os.getenv("VERTICA_TOKEN_SCHEMA", "ESPDM")

        if not all([host, user, password, database]):
            raise AuthenticationError(
                "Missing required Vertica environment variables",
                details={
                    "VERTICA_HOST": bool(host),
                    "VERTICA_USER": bool(user),
                    "VERTICA_PASSWORD": bool(password),
                    "VERTICA_DATABASE": bool(database),
                },
            )

        return cls(
            platform=platform,
            host=host,
            user=user,
            password=password,
            database=database,
            schema=schema,
        )
