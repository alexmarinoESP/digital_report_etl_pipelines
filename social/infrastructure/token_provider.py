"""Token provider implementation for OAuth authentication.

This module implements the TokenProvider protocol, retrieving and
refreshing authentication tokens from the database.
"""

from datetime import datetime, timedelta
from typing import Optional
import requests
from loguru import logger

from social.core.protocols import TokenProvider, DataSink
from social.core.exceptions import AuthenticationError
from social.core.constants import DATABASE_SCHEMA


class DatabaseTokenProvider:
    """Token provider that stores tokens in Vertica database.

    This implementation retrieves OAuth tokens from the database and
    handles token refresh when needed. Tokens are stored per platform
    (linkedin, google, etc.).
    """

    def __init__(self, platform: str, data_sink: DataSink):
        """Initialize the token provider.

        Args:
            platform: Platform name (linkedin, google, etc.)
            data_sink: Data sink for database queries
        """
        self.platform = platform.lower()
        self.data_sink = data_sink
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._expires_at: Optional[datetime] = None
        self._token_data: Optional[dict] = None

        # Load initial tokens
        self._load_tokens()

    def get_access_token(self) -> str:
        """Get current access token, refreshing if needed.

        Returns:
            Valid access token

        Raises:
            AuthenticationError: If token retrieval or refresh fails
        """
        # Check if token is expired or about to expire (5 minute buffer)
        if self._is_token_expired():
            logger.info(f"Access token expired for {self.platform}, refreshing...")
            self.refresh_access_token()

        if not self._access_token:
            raise AuthenticationError(
                f"No access token available for platform: {self.platform}"
            )

        return self._access_token

    def get_refresh_token(self) -> str:
        """Get refresh token.

        Returns:
            Refresh token

        Raises:
            AuthenticationError: If refresh token not available
        """
        if not self._refresh_token:
            raise AuthenticationError(
                f"No refresh token available for platform: {self.platform}"
            )

        return self._refresh_token

    def refresh_access_token(self) -> str:
        """Refresh the access token using the refresh token.

        Returns:
            New access token

        Raises:
            AuthenticationError: If token refresh fails
        """
        if not self._token_data:
            raise AuthenticationError(
                f"No token data loaded for platform: {self.platform}"
            )

        try:
            if self.platform == "linkedin":
                new_token = self._refresh_linkedin_token()
            elif self.platform == "google":
                new_token = self._refresh_google_token()
            else:
                raise AuthenticationError(
                    f"Token refresh not implemented for platform: {self.platform}"
                )

            self._access_token = new_token
            self._expires_at = datetime.now() + timedelta(hours=1)  # Default 1 hour expiry

            logger.info(f"✓ Access token refreshed for {self.platform}")
            return new_token

        except Exception as e:
            logger.error(f"Failed to refresh access token: {e}")
            raise AuthenticationError(
                f"Failed to refresh access token for {self.platform}",
                details={"error": str(e)}
            )

    def get_token_expiry(self) -> datetime:
        """Get token expiration datetime.

        Returns:
            Token expiration datetime
        """
        if self._expires_at:
            return self._expires_at

        # Default to 1 hour from now if not set
        return datetime.now() + timedelta(hours=1)

    def _load_tokens(self) -> None:
        """Load tokens from database.

        Raises:
            AuthenticationError: If tokens cannot be loaded
        """
        query = f"""
            SELECT
                access_token,
                refresh_token,
                expires_at,
                client_id,
                client_secret
            FROM {DATABASE_SCHEMA}.social_tokens
            WHERE platform = '{self.platform}'
            AND active = true
            ORDER BY updated_at DESC
            LIMIT 1
        """

        try:
            result_df = self.data_sink.query(query)

            if result_df.empty:
                raise AuthenticationError(
                    f"No tokens found in database for platform: {self.platform}"
                )

            row = result_df.iloc[0]

            self._access_token = row["access_token"]
            self._refresh_token = row["refresh_token"]

            # Parse expiry if present
            if row.get("expires_at"):
                if isinstance(row["expires_at"], str):
                    self._expires_at = datetime.fromisoformat(row["expires_at"])
                else:
                    self._expires_at = row["expires_at"]

            # Store full token data for refresh
            self._token_data = {
                "client_id": row.get("client_id"),
                "client_secret": row.get("client_secret"),
                "refresh_token": self._refresh_token,
            }

            logger.info(f"✓ Loaded tokens for {self.platform} from database")

        except Exception as e:
            logger.error(f"Failed to load tokens from database: {e}")
            raise AuthenticationError(
                f"Failed to load tokens for platform: {self.platform}",
                details={"error": str(e)}
            )

    def _is_token_expired(self) -> bool:
        """Check if access token is expired or about to expire.

        Returns:
            True if token needs refresh, False otherwise
        """
        if not self._expires_at:
            # If we don't know expiry, assume it's valid
            return False

        # Refresh if token expires within 5 minutes
        buffer = timedelta(minutes=5)
        return datetime.now() + buffer >= self._expires_at

    def _refresh_linkedin_token(self) -> str:
        """Refresh LinkedIn access token.

        Returns:
            New access token

        Raises:
            AuthenticationError: If refresh fails
        """
        url = "https://www.linkedin.com/oauth/v2/accessToken"

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self._token_data["refresh_token"],
            "client_id": self._token_data["client_id"],
            "client_secret": self._token_data["client_secret"],
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        response = requests.post(url, data=payload, headers=headers, timeout=30)

        if response.status_code != 200:
            raise AuthenticationError(
                f"LinkedIn token refresh failed with status {response.status_code}",
                details={"response": response.text[:500]}
            )

        response_data = response.json()

        if "access_token" not in response_data:
            raise AuthenticationError(
                "No access_token in LinkedIn refresh response",
                details={"response": response_data}
            )

        # Update token in database
        self._update_token_in_db(response_data["access_token"])

        return response_data["access_token"]

    def _refresh_google_token(self) -> str:
        """Refresh Google Ads access token.

        Returns:
            New access token

        Raises:
            AuthenticationError: If refresh fails
        """
        url = "https://oauth2.googleapis.com/token"

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self._token_data["refresh_token"],
            "client_id": self._token_data["client_id"],
            "client_secret": self._token_data["client_secret"],
        }

        response = requests.post(url, data=payload, timeout=30)

        if response.status_code != 200:
            raise AuthenticationError(
                f"Google token refresh failed with status {response.status_code}",
                details={"response": response.text[:500]}
            )

        response_data = response.json()

        if "access_token" not in response_data:
            raise AuthenticationError(
                "No access_token in Google refresh response",
                details={"response": response_data}
            )

        # Update token in database
        self._update_token_in_db(response_data["access_token"])

        return response_data["access_token"]

    def _update_token_in_db(self, new_access_token: str) -> None:
        """Update access token in database.

        Args:
            new_access_token: New access token to store
        """
        try:
            # Note: This requires a proper UPDATE implementation in DataSink
            # For now, we'll just log - actual implementation depends on your needs
            logger.debug(f"Would update token in database for {self.platform}")

            # If you need to actually update the database, you could:
            # 1. Add an execute() method to DataSink
            # 2. Use the repository pattern with update methods
            # 3. Create a separate token storage service

        except Exception as e:
            logger.warning(f"Failed to update token in database: {e}")
            # Don't fail the whole refresh if DB update fails
