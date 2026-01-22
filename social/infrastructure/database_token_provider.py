"""Database token provider for LinkedIn.

This module provides a token provider that retrieves LinkedIn access tokens
from the database table SOCIAL_ADS_POSTS_ACCESS_CODE, following the same
pattern as the legacy social_posts system.
"""

from typing import Optional, Dict, Any
from loguru import logger
import pandas as pd

from social.core.protocols import TokenProvider, DataSink
from social.core.exceptions import AuthenticationError
from social.infrastructure.file_token_provider import FileBasedTokenProvider


class DatabaseTokenProvider(TokenProvider):
    """Token provider that retrieves LinkedIn tokens from database.

    This provider:
    1. Gets client_id/client_secret from file/env (via FileBasedTokenProvider)
    2. Retrieves access_token/refresh_token from database table SOCIAL_ADS_POSTS_ACCESS_CODE

    This maintains backward compatibility with the legacy social_posts system.
    """

    def __init__(
        self,
        platform: str,
        data_sink: DataSink,
        credentials_file: Optional[str] = None
    ):
        """Initialize database token provider.

        Args:
            platform: Platform name (should be 'linkedin')
            data_sink: Database connection for token retrieval
            credentials_file: Optional path to credentials file for client_id/secret
        """
        self.platform = platform.lower()
        self.data_sink = data_sink

        # Use file provider for static credentials (client_id, client_secret)
        self.file_provider = FileBasedTokenProvider(platform, credentials_file)

        # Load dynamic tokens from database
        self._db_tokens = self._load_tokens_from_db()

        logger.info(f"DatabaseTokenProvider initialized for {platform}")

    def _load_tokens_from_db(self) -> Dict[str, str]:
        """Load access_token and refresh_token from database.

        Returns:
            Dictionary with ACCESS_TOKEN and REFRESH_TOKEN

        Raises:
            AuthenticationError: If tokens not found in database
        """
        if self.platform != "linkedin":
            logger.warning(f"Database token retrieval only supported for LinkedIn, got: {self.platform}")
            return {}

        # Query from legacy table (same as old system)
        query = """
            SELECT ACCESS_TOKEN, REFRESH_TOKEN, EXPIRES
            FROM ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE
            WHERE SOCIAL='LinkedinADS'
            AND ROW_LOADED_DATE IN (
                SELECT MAX(G.ROW_LOADED_DATE)
                FROM ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE G
            )
        """

        try:
            logger.debug(f"Querying database for LinkedIn tokens...")
            df = self.data_sink.query(query)

            if df.empty:
                raise AuthenticationError(
                    "No LinkedIn tokens found in database",
                    details={"platform": self.platform, "table": "SOCIAL_ADS_POSTS_ACCESS_CODE"}
                )

            # Extract first row
            row = df.iloc[0]
            tokens = {
                "access_token": row["ACCESS_TOKEN"],
                "refresh_token": row["REFRESH_TOKEN"],
                "expires": row.get("EXPIRES")
            }

            logger.info(f"Successfully loaded LinkedIn tokens from database")
            return tokens

        except Exception as e:
            # If database query fails, check if tokens are in file as fallback
            logger.warning(f"Failed to load tokens from database: {e}")
            logger.info("Attempting to use tokens from credentials file as fallback...")

            # Try to get from file provider
            file_creds = self.file_provider._credentials
            if "access_token" in file_creds and "refresh_token" in file_creds:
                logger.info("Using tokens from credentials file")
                return {
                    "access_token": file_creds["access_token"],
                    "refresh_token": file_creds["refresh_token"]
                }

            raise AuthenticationError(
                f"Failed to load LinkedIn tokens from database and file",
                details={"error": str(e), "platform": self.platform}
            )

    def get_access_token(self) -> str:
        """Get access token from database.

        Returns:
            Access token string

        Raises:
            AuthenticationError: If token not found
        """
        token = self._db_tokens.get("access_token")

        if not token:
            raise AuthenticationError(
                f"No access_token found for platform: {self.platform}",
                details={"platform": self.platform}
            )

        return token

    def refresh_access_token(self) -> str:
        """Refresh access token from database.

        Re-queries the database to get the latest token.

        Returns:
            Refreshed access token

        Raises:
            AuthenticationError: If refresh fails
        """
        logger.info(f"Refreshing access token for {self.platform} from database...")
        self._db_tokens = self._load_tokens_from_db()
        return self.get_access_token()

    def get_client_id(self) -> Optional[str]:
        """Get client ID from file/env.

        Returns:
            Client ID
        """
        return self.file_provider.get_client_id()

    def get_client_secret(self) -> Optional[str]:
        """Get client secret from file/env.

        Returns:
            Client secret
        """
        return self.file_provider.get_client_secret()

    def get_refresh_token(self) -> Optional[str]:
        """Get refresh token from database.

        Returns:
            Refresh token
        """
        return self._db_tokens.get("refresh_token")

    def get_app_id(self) -> Optional[str]:
        """Get app ID (not applicable for LinkedIn).

        Returns:
            None
        """
        return None

    def get_app_secret(self) -> Optional[str]:
        """Get app secret (not applicable for LinkedIn).

        Returns:
            None
        """
        return None

    def get_account_ids(self) -> list:
        """Get account IDs (not applicable for LinkedIn).

        Returns:
            Empty list
        """
        return []

    def get_manager_ids(self) -> list:
        """Get manager IDs (not applicable for LinkedIn).

        Returns:
            Empty list
        """
        return []

    def get_additional_config(self, key: str, default: Any = None) -> Any:
        """Get additional configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value
        """
        # Check database tokens first
        if key in self._db_tokens:
            return self._db_tokens[key]

        # Fall back to file provider
        return self.file_provider.get_additional_config(key, default)
