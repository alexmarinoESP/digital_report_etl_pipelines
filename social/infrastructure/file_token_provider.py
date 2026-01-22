"""File-based token provider implementation.

This module provides token management using YAML configuration files,
compatible with the legacy social_posts credentials structure.
"""

from typing import Optional, Dict, Any
from pathlib import Path
import yaml
from loguru import logger

from social.core.protocols import TokenProvider
from social.core.exceptions import AuthenticationError


class FileBasedTokenProvider:
    """Token provider that reads credentials from YAML configuration files.

    This implementation is compatible with the legacy social_posts structure
    where credentials are stored in credentials.yml files.
    """

    def __init__(
        self,
        platform: str,
        credentials_file: Optional[str] = None
    ):
        """Initialize file-based token provider.

        Args:
            platform: Platform name (linkedin, google, facebook)
            credentials_file: Path to credentials YAML file. If None, uses default path.
        """
        self.platform = platform.lower()

        # Determine credentials file path
        if credentials_file:
            self.credentials_file = Path(credentials_file)
        else:
            # Default to social_posts credentials file for backward compatibility
            module_dir = Path(__file__).parent.parent.parent
            self.credentials_file = module_dir / "social_posts" / "social_posts" / "credentials.yml"

        # Load credentials
        self._credentials = self._load_credentials()

        logger.info(f"FileBasedTokenProvider initialized for {platform}")

    def _load_credentials(self) -> Dict[str, Any]:
        """Load credentials from YAML file.

        Returns:
            Dictionary with platform credentials

        Raises:
            AuthenticationError: If credentials file not found or invalid
        """
        if not self.credentials_file.exists():
            raise AuthenticationError(
                f"Credentials file not found: {self.credentials_file}",
                details={"platform": self.platform, "file": str(self.credentials_file)}
            )

        try:
            with open(self.credentials_file, "r", encoding="utf-8") as f:
                all_credentials = yaml.safe_load(f)

            if self.platform not in all_credentials:
                raise AuthenticationError(
                    f"Platform '{self.platform}' not found in credentials file",
                    details={"platform": self.platform, "file": str(self.credentials_file)}
                )

            return all_credentials[self.platform]

        except yaml.YAMLError as e:
            raise AuthenticationError(
                f"Failed to parse credentials file: {str(e)}",
                details={"platform": self.platform, "file": str(self.credentials_file)}
            )
        except Exception as e:
            raise AuthenticationError(
                f"Failed to load credentials: {str(e)}",
                details={"platform": self.platform, "file": str(self.credentials_file)}
            )

    def get_access_token(self) -> str:
        """Get access token for the platform.

        Returns:
            Access token string

        Raises:
            AuthenticationError: If token not found
        """
        token = self._credentials.get("access_token")

        if not token:
            raise AuthenticationError(
                f"No access_token found for platform: {self.platform}",
                details={"platform": self.platform}
            )

        return token

    def refresh_access_token(self) -> str:
        """Refresh access token.

        For file-based tokens, this is a no-op since tokens are static in the file.
        In production, this should be implemented to actually refresh tokens.

        Returns:
            Current access token

        Raises:
            AuthenticationError: If refresh fails
        """
        logger.warning(
            f"Token refresh not implemented for file-based provider. "
            f"Returning existing token for {self.platform}"
        )
        return self.get_access_token()

    def get_client_id(self) -> Optional[str]:
        """Get client ID for the platform.

        Returns:
            Client ID or None if not available
        """
        return self._credentials.get("client_id")

    def get_client_secret(self) -> Optional[str]:
        """Get client secret for the platform.

        Returns:
            Client secret or None if not available
        """
        return self._credentials.get("client_secret")

    def get_app_id(self) -> Optional[str]:
        """Get app ID (Facebook-specific).

        Returns:
            App ID or None if not available
        """
        return self._credentials.get("app_id")

    def get_app_secret(self) -> Optional[str]:
        """Get app secret (Facebook-specific).

        Returns:
            App secret or None if not available
        """
        return self._credentials.get("app_secret")

    def get_account_ids(self) -> list:
        """Get account IDs (Facebook-specific).

        Returns:
            List of account IDs
        """
        id_accounts = self._credentials.get("id_account", [])

        # Ensure it's a list
        if isinstance(id_accounts, str):
            return [id_accounts]

        return id_accounts

    def get_manager_ids(self) -> list:
        """Get manager IDs (Google-specific).

        Returns:
            List of manager IDs
        """
        manager_ids = self._credentials.get("manager_id", [])

        # Ensure it's a list
        if isinstance(manager_ids, (int, str)):
            return [str(manager_ids)]

        return [str(mid) for mid in manager_ids]

    def get_additional_config(self, key: str, default: Any = None) -> Any:
        """Get additional configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value
        """
        return self._credentials.get(key, default)
