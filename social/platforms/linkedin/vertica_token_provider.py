"""Vertica-based token provider for LinkedIn.

This module provides token management by reading credentials from Vertica database,
matching the legacy social_posts behavior where tokens are stored in
ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE table.
"""

from typing import Optional, Dict, Any
from loguru import logger

from shared.connection.vertica import VerticaConnection
from social.core.exceptions import AuthenticationError


class VerticaTokenProvider:
    """Token provider that reads LinkedIn credentials from Vertica database.

    This implementation reads from ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE table,
    matching the legacy social_posts LinkedIn Ads authentication pattern.
    """

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """Initialize Vertica-based token provider.

        Args:
            host: Vertica host
            port: Vertica port
            database: Vertica database name
            user: Vertica username
            password: Vertica password
        """
        self._connection = VerticaConnection(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._expires: Optional[str] = None

        # Load tokens from database
        self._load_tokens()

        logger.info("VerticaTokenProvider initialized for LinkedIn")

    def _load_tokens(self) -> None:
        """Load tokens from Vertica database.

        Raises:
            AuthenticationError: If token retrieval fails
        """
        query = """
        SELECT ACCESS_TOKEN, REFRESH_TOKEN, EXPIRES
        FROM ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE
        WHERE SOCIAL='LinkedinADS' AND
        ROW_LOADED_DATE IN (SELECT MAX(G.ROW_LOADED_DATE) FROM ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE G)
        """

        try:
            conn = self._connection.connect()
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()

            if not result:
                raise AuthenticationError(
                    "No LinkedIn token found in database",
                    details={"query": query, "table": "ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE"}
                )

            self._access_token = result[0]
            self._refresh_token = result[1] if len(result) > 1 else None
            self._expires = result[2] if len(result) > 2 else None

            logger.info("Successfully loaded LinkedIn token from Vertica")

        except Exception as e:
            raise AuthenticationError(
                f"Failed to load token from Vertica: {str(e)}",
                details={"query": query}
            ) from e
        finally:
            if cursor:
                cursor.close()

    def get_access_token(self) -> str:
        """Get access token for API requests.

        Returns:
            LinkedIn access token

        Raises:
            AuthenticationError: If no token available
        """
        if not self._access_token:
            raise AuthenticationError(
                "No access token available",
                details={"platform": "linkedin"}
            )
        return self._access_token

    def get_refresh_token(self) -> Optional[str]:
        """Get refresh token for token renewal.

        Returns:
            LinkedIn refresh token or None if not available
        """
        return self._refresh_token

    def refresh_access_token(self) -> str:
        """Refresh the access token.

        Note: This implementation simply reloads from database.
        Token refresh logic should be handled externally.

        Returns:
            New access token
        """
        self._load_tokens()
        return self.get_access_token()
