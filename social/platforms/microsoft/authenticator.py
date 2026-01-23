"""
Microsoft Ads Authentication Module

Provides OAuth2 authentication with multiple fallback strategies:
1. Stored refresh token (from tokens.json)
2. Service Principal (Azure AD client credentials)
3. Browser OAuth flow with local HTTP callback server

SOLID Principles Applied:
- Single Responsibility: Each class has one clear purpose
- Open/Closed: Extensible through strategy pattern
- Liskov Substitution: Implements TokenProvider Protocol
- Interface Segregation: Clean Protocol interface
- Dependency Inversion: Depends on abstractions (Protocol)
"""

import json
import os
import threading
import time
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Dict, Optional, Union
from urllib.parse import parse_qs, urlencode, urlparse

import requests
from bingads.authorization import (
    AuthorizationData,
    OAuthDesktopMobileAuthCodeGrant,
    OAuthWebAuthCodeGrant,
)
from loguru import logger

from social.core.protocols import TokenProvider


class CallbackHandler(BaseHTTPRequestHandler):
    """HTTP request handler for OAuth callback.

    Handles GET requests from Microsoft OAuth redirect and extracts authorization code.
    """

    def do_GET(self) -> None:
        """Handle GET request for OAuth callback."""
        parsed_path = urlparse(self.path)
        query_params = parse_qs(parsed_path.query)

        if "code" in query_params:
            self.server.auth_code = query_params["code"][0]  # type: ignore
            self._send_success_response()
        elif "error" in query_params:
            self.server.auth_error = query_params["error"][0]  # type: ignore
            self._send_error_response(query_params["error"][0])
        else:
            self._send_invalid_response()

    def _send_success_response(self) -> None:
        """Send success HTML response."""
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        html = """
            <html>
            <head><title>Authorization Successful</title></head>
            <body style="font-family: Arial, sans-serif; text-align: center; padding: 50px;">
                <h1 style="color: #28a745;">✓ Authorization Successful!</h1>
                <p>You can now close this window and return to your application.</p>
                <script>setTimeout(function() { window.close(); }, 3000);</script>
            </body>
            </html>
        """
        self.wfile.write(html.encode("utf-8"))

    def _send_error_response(self, error: str) -> None:
        """Send error HTML response."""
        self.send_response(400)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        html = f"""
            <html>
            <head><title>Authorization Failed</title></head>
            <body style="font-family: Arial, sans-serif; text-align: center; padding: 50px;">
                <h1 style="color: #dc3545;">✗ Authorization Failed</h1>
                <p>Error: {error}</p>
                <p>You can close this window and try again.</p>
            </body>
            </html>
        """
        self.wfile.write(html.encode("utf-8"))

    def _send_invalid_response(self) -> None:
        """Send invalid callback response."""
        self.send_response(400)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        html = "<html><body><h1>Invalid callback</h1></body></html>"
        self.wfile.write(html.encode("utf-8"))

    def log_message(self, format: str, *args) -> None:
        """Override to suppress default HTTP logging."""
        pass


class CallbackServer:
    """Local HTTP server for handling OAuth callbacks.

    Starts a temporary server on localhost to receive OAuth redirect.
    """

    def __init__(self, port: int = 8080):
        """Initialize callback server.

        Args:
            port: Preferred port (will try this and next 9 ports if busy)
        """
        self.port = port
        self.server: Optional[HTTPServer] = None
        self.thread: Optional[threading.Thread] = None

    def start(self) -> str:
        """Start the callback server in a background thread.

        Returns:
            The callback URL (http://localhost:{port})

        Raises:
            RuntimeError: If no available port found
        """
        # Try to find an available port
        for port in range(self.port, self.port + 10):
            try:
                self.server = HTTPServer(("localhost", port), CallbackHandler)
                self.server.auth_code = None  # type: ignore
                self.server.auth_error = None  # type: ignore
                self.port = port
                break
            except OSError:
                continue
        else:
            raise RuntimeError(
                f"Could not find an available port between {self.port} and {self.port+9}"
            )

        # Start server in daemon thread
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        callback_url = f"http://localhost:{self.port}"
        logger.info(f"OAuth callback server started on {callback_url}")
        return callback_url

    def wait_for_callback(self, timeout: int = 30) -> Optional[str]:
        """Wait for OAuth callback with timeout.

        Args:
            timeout: Maximum seconds to wait

        Returns:
            Authorization code if successful, None otherwise
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            if hasattr(self.server, "auth_code") and self.server.auth_code:  # type: ignore
                return self.server.auth_code  # type: ignore
            elif hasattr(self.server, "auth_error") and self.server.auth_error:  # type: ignore
                logger.error(f"OAuth error: {self.server.auth_error}")  # type: ignore
                return None
            time.sleep(0.5)

        logger.error(f"Timeout waiting for OAuth callback ({timeout}s)")
        return None

    def stop(self) -> None:
        """Stop the callback server and cleanup resources."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            if self.thread:
                self.thread.join(timeout=1)
            logger.info("OAuth callback server stopped")


class MicrosoftAdsAuthenticator:
    """Microsoft Ads API Authentication Manager.

    Handles OAuth 2.0 authentication with multiple fallback strategies:
    1. Stored refresh token (fast, automatic)
    2. Service Principal / Azure AD (for serverless environments)
    3. Browser OAuth flow (for initial setup)

    Implements best practices:
    - Token persistence with file storage
    - Automatic token refresh (5min buffer before expiry)
    - Multiple authentication fallbacks
    - Container-friendly (service principal priority)
    """

    # OAuth endpoints
    AUTH_URL = "https://login.live.com/oauth20_authorize.srf"
    TOKEN_URL = "https://login.live.com/oauth20_token.srf"
    DEFAULT_SCOPE = "https://ads.microsoft.com/msads.manage offline_access"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        developer_token: str,
        customer_id: str,
        account_id: str,
        callback_port: int = 8080,
        scope: Optional[str] = None,
        token_file: Optional[Union[str, Path]] = None,
    ):
        """Initialize Microsoft Ads authenticator.

        Args:
            client_id: OAuth application client ID
            client_secret: OAuth application client secret
            developer_token: Microsoft Ads developer token
            customer_id: Microsoft Ads customer ID
            account_id: Microsoft Ads account ID
            callback_port: Port for local callback server (default: 8080)
            scope: OAuth scope (default: msads.manage + offline_access)
            token_file: Path to store tokens (default: tokens.json in current dir)
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.developer_token = developer_token
        self.customer_id = customer_id
        self.account_id = account_id
        self.callback_port = callback_port
        self.scope = scope or self.DEFAULT_SCOPE
        self.token_file = Path(token_file) if token_file else Path("tokens.json")

        self._tokens: Optional[Dict[str, Union[str, int]]] = None
        self._authorization_data: Optional[AuthorizationData] = None

        logger.debug(
            f"MicrosoftAdsAuthenticator initialized (customer: {customer_id}, "
            f"account: {account_id}, token_file: {self.token_file})"
        )

    def get_authorization_url(self, redirect_uri: str) -> str:
        """Generate the authorization URL for OAuth flow.

        Args:
            redirect_uri: The callback URL

        Returns:
            Full authorization URL to open in browser
        """
        params = {
            "client_id": self.client_id,
            "scope": self.scope,
            "response_type": "code",
            "redirect_uri": redirect_uri,
        }
        return f"{self.AUTH_URL}?{urlencode(params)}"

    def exchange_code_for_tokens(
        self, authorization_code: str, redirect_uri: str
    ) -> Dict[str, Union[str, int]]:
        """Exchange authorization code for access and refresh tokens.

        Args:
            authorization_code: Authorization code from OAuth flow
            redirect_uri: The redirect URI used in authorization request

        Returns:
            Dictionary containing tokens and metadata

        Raises:
            requests.RequestException: If token exchange fails
        """
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": authorization_code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        }

        try:
            logger.info("Exchanging authorization code for tokens")
            response = requests.post(self.TOKEN_URL, data=data, timeout=30)
            response.raise_for_status()

            tokens = response.json()

            # Add metadata
            tokens["expires_at"] = (
                datetime.now().timestamp() + tokens.get("expires_in", 3600 * 24 * 90)
            )
            tokens["created_at"] = datetime.now().timestamp()

            self._tokens = tokens
            logger.success("Successfully exchanged code for tokens")
            return tokens

        except requests.RequestException as e:
            logger.error(f"Failed to exchange code for tokens: {e}")
            raise

    def refresh_access_token(
        self, refresh_token: Optional[str] = None
    ) -> Dict[str, Union[str, int]]:
        """Refresh the access token using refresh token.

        Args:
            refresh_token: Refresh token (uses stored if not provided)

        Returns:
            Dictionary containing new tokens

        Raises:
            requests.RequestException: If token refresh fails
            ValueError: If refresh token is not available
        """
        if refresh_token is None:
            if self._tokens is None:
                self.load_tokens()

            if self._tokens is None or "refresh_token" not in self._tokens:
                raise ValueError("No refresh token available")

            refresh_token = self._tokens["refresh_token"]

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
            "scope": self.scope,
        }

        try:
            logger.info("Refreshing access token")
            response = requests.post(self.TOKEN_URL, data=data, timeout=30)
            response.raise_for_status()

            tokens = response.json()

            # Preserve refresh token if not returned
            if "refresh_token" not in tokens and self._tokens:
                tokens["refresh_token"] = self._tokens["refresh_token"]

            # Update metadata
            tokens["expires_at"] = (
                datetime.now().timestamp() + tokens.get("expires_in", 3600 * 24 * 90)
            )
            tokens["refreshed_at"] = datetime.now().timestamp()

            self._tokens = tokens
            logger.success("Successfully refreshed access token")
            return tokens

        except requests.RequestException as e:
            logger.error(f"Failed to refresh access token: {e}")
            raise

    def is_token_expired(self, buffer_seconds: int = 300) -> bool:
        """Check if the current access token is expired.

        Args:
            buffer_seconds: Safety buffer before actual expiry (default: 5min)

        Returns:
            True if token is expired or will expire within buffer, False otherwise
        """
        if self._tokens is None or "expires_at" not in self._tokens:
            return True

        return (
            datetime.now().timestamp() + buffer_seconds >= self._tokens["expires_at"]
        )

    def get_valid_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary.

        Returns:
            Valid access token

        Raises:
            ValueError: If no tokens are available
        """
        if self._tokens is None:
            self.load_tokens()

        if self._tokens is None:
            raise ValueError("No tokens available. Please authenticate first.")

        if self.is_token_expired():
            logger.info("Access token expired, refreshing...")
            self.refresh_access_token()

        return str(self._tokens["access_token"])

    def save_tokens(
        self, tokens: Optional[Dict[str, Union[str, int]]] = None
    ) -> None:
        """Save tokens to file.

        Args:
            tokens: Tokens to save (uses stored tokens if not provided)
        """
        tokens_to_save = tokens or self._tokens

        if tokens_to_save is None:
            logger.warning("No tokens to save")
            return

        try:
            self.token_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.token_file, "w") as f:
                json.dump(tokens_to_save, f, indent=2)
            logger.info(f"Tokens saved to {self.token_file}")
        except Exception as e:
            logger.error(f"Failed to save tokens: {e}")

    def load_tokens(self) -> Optional[Dict[str, Union[str, int]]]:
        """Load tokens from file.

        Returns:
            Loaded tokens or None if file doesn't exist
        """
        if not self.token_file.exists():
            logger.debug(f"Token file not found: {self.token_file}")
            return None

        try:
            with open(self.token_file, "r") as f:
                tokens = json.load(f)
            self._tokens = tokens
            logger.info(f"Tokens loaded from {self.token_file}")
            return tokens
        except Exception as e:
            logger.error(f"Failed to load tokens: {e}")
            return None

    def authenticate_automatic_flow(self, timeout: int = 30) -> bool:
        """Perform automatic authentication flow with local callback server.

        Opens browser for user authorization and captures code automatically.

        Args:
            timeout: Timeout in seconds for waiting for callback

        Returns:
            True if authentication successful, False otherwise
        """
        logger.info("Starting automatic OAuth flow with browser")

        callback_server = CallbackServer(self.callback_port)

        try:
            callback_url = callback_server.start()

            # Generate and open authorization URL
            auth_url = self.get_authorization_url(callback_url)
            logger.info("Opening authorization URL in browser")
            webbrowser.open(auth_url)

            print(f"\n🔐 Microsoft Ads Authorization")
            print(f"A browser window should open for authorization.")
            print(f"If it doesn't, please visit: {auth_url}")
            print(f"Waiting for callback (timeout: {timeout}s)...\n")

            # Wait for callback
            auth_code = callback_server.wait_for_callback(timeout)

            if auth_code:
                # Exchange code for tokens
                tokens = self.exchange_code_for_tokens(auth_code, callback_url)
                self.save_tokens(tokens)
                logger.success("Automatic authentication completed successfully")
                return True
            else:
                logger.error("Failed to receive authorization callback")
                return False

        except Exception as e:
            logger.error(f"Automatic authentication failed: {e}")
            return False
        finally:
            callback_server.stop()
            time.sleep(0.5)  # Ensure cleanup

    def authenticate_with_service_principal(
        self,
        tenant_id: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ) -> bool:
        """Authenticate using Azure Service Principal (Client Credentials flow).

        Fully automatic, no user interaction required. Ideal for Container Apps.

        Args:
            tenant_id: Azure tenant ID
            client_id: Service principal client ID (uses instance client_id if not provided)
            client_secret: Service principal client secret (uses instance client_secret if not provided)

        Returns:
            True if authentication successful, False otherwise
        """
        client_id = client_id or self.client_id
        client_secret = client_secret or self.client_secret

        token_url = (
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        )

        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://ads.microsoft.com/.default",
            "grant_type": "client_credentials",
        }

        try:
            logger.info("Authenticating with Azure Service Principal")
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()

            tokens = response.json()

            # Add metadata
            tokens["expires_at"] = (
                datetime.now().timestamp() + tokens.get("expires_in", 3600)
            )
            tokens["created_at"] = datetime.now().timestamp()
            tokens["auth_type"] = "service_principal"

            self._tokens = tokens
            self.save_tokens(tokens)

            logger.success("Service principal authentication completed")
            return True

        except requests.RequestException as e:
            logger.error(f"Service principal authentication failed: {e}")
            return False

    def authenticate(
        self,
        refresh_token: Optional[str] = None,
        tenant_id: Optional[str] = None,
        timeout: int = 30,
    ) -> bool:
        """Attempt authentication using available methods with fallback.

        Priority order:
        1. Existing valid tokens (from file)
        2. Stored refresh token
        3. Provided refresh token
        4. Service principal (if tenant_id provided)
        5. Browser OAuth flow with local callback

        Args:
            refresh_token: Optional refresh token to use
            tenant_id: Optional tenant ID for service principal auth
            timeout: Timeout for OAuth flow (default: 30s)

        Returns:
            True if authentication successful, False otherwise
        """
        logger.info("Starting authentication process")

        # 1. Try existing valid tokens
        if self.load_tokens() and not self.is_token_expired():
            logger.info("Using existing valid tokens")
            return True

        # 2. Try refreshing with stored refresh token
        if self._tokens and "refresh_token" in self._tokens:
            try:
                logger.info("Attempting refresh with stored refresh token")
                self.refresh_access_token(self._tokens["refresh_token"])
                self.save_tokens(self._tokens)
                logger.success("Refreshed with stored token")
                return True
            except Exception as e:
                logger.warning(f"Stored refresh token failed: {e}")

        # 3. Try provided refresh token
        if refresh_token:
            try:
                logger.info("Attempting refresh with provided refresh token")
                tokens = self.refresh_access_token(refresh_token)
                self.save_tokens(tokens)
                logger.success("Refreshed with provided token")
                return True
            except Exception as e:
                logger.warning(f"Provided refresh token failed: {e}")

        # 4. Try service principal
        if tenant_id:
            if self.authenticate_with_service_principal(tenant_id):
                return True

        # 5. Try browser OAuth flow
        logger.info("Attempting browser OAuth flow")
        return self.authenticate_automatic_flow(timeout)

    def authenticate_for_container_app(self, tenant_id: Optional[str] = None) -> bool:
        """Authenticate specifically for Container App environment (no browser).

        Priority order for Container App:
        1. Stored refresh token (from mounted volume)
        2. Service Principal (Azure AD)
        3. FAIL (no browser available)

        Args:
            tenant_id: Azure tenant ID for service principal

        Returns:
            True if authentication successful, False otherwise
        """
        logger.info("Container App authentication mode (no browser)")

        # 1. Try stored refresh token
        if self.load_tokens() and not self.is_token_expired():
            logger.info("Using existing valid tokens from mounted volume")
            return True

        if self._tokens and "refresh_token" in self._tokens:
            try:
                self.refresh_access_token()
                self.save_tokens(self._tokens)
                logger.success("Refreshed access token successfully")
                return True
            except Exception as e:
                logger.warning(f"Token refresh failed: {e}")

        # 2. Try service principal
        if tenant_id:
            if self.authenticate_with_service_principal(tenant_id):
                return True
        else:
            logger.warning("No tenant_id provided for service principal auth")

        # 3. FAIL
        logger.error(
            "Container App authentication failed. "
            "Requires either valid refresh token or service principal credentials."
        )
        return False

    def get_authorization_data(self) -> AuthorizationData:
        """Get configured AuthorizationData for BingAds SDK.

        Returns:
            Configured AuthorizationData instance

        Raises:
            ValueError: If authentication has not been completed
        """
        if self._authorization_data is None:
            # Ensure we have valid tokens
            access_token = self.get_valid_access_token()

            # Handle different authentication types
            if self._tokens and self._tokens.get("auth_type") == "service_principal":
                # Service principal authentication
                authentication = OAuthDesktopMobileAuthCodeGrant(
                    client_id=self.client_id
                )

                # Manually set access token
                authentication._access_token = access_token
                authentication._access_token_expires_in_seconds = self._tokens.get(
                    "expires_in", 3600
                )
                authentication._token_received_datetime = datetime.fromtimestamp(
                    self._tokens.get("created_at", datetime.now().timestamp())
                )

            else:
                # Standard OAuth flow
                authentication = OAuthWebAuthCodeGrant(
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    redirection_uri=f"http://localhost:{self.callback_port}",
                )

                # Set tokens
                if self._tokens and "refresh_token" in self._tokens:
                    authentication.request_oauth_tokens_by_refresh_token(
                        self._tokens["refresh_token"]
                    )

            # Create authorization data
            self._authorization_data = AuthorizationData(
                developer_token=self.developer_token,
                authentication=authentication,
                customer_id=self.customer_id,
                account_id=self.account_id,
            )

            logger.info("AuthorizationData configured for BingAds SDK")

        return self._authorization_data

    def reset_authentication(self) -> None:
        """Reset authentication state and clear stored tokens."""
        self._tokens = None
        self._authorization_data = None

        if self.token_file.exists():
            self.token_file.unlink()
            logger.info(f"Token file deleted: {self.token_file}")

        logger.info("Authentication reset")


class MicrosoftAdsTokenProvider(TokenProvider):
    """Protocol-compatible wrapper for MicrosoftAdsAuthenticator.

    Implements TokenProvider Protocol for dependency injection compatibility.
    """

    def __init__(self, authenticator: MicrosoftAdsAuthenticator):
        """Initialize token provider wrapper.

        Args:
            authenticator: Configured MicrosoftAdsAuthenticator instance
        """
        self.authenticator = authenticator

    def get_access_token(self) -> str:
        """Get valid access token (refreshes if necessary).

        Returns:
            Valid access token
        """
        return self.authenticator.get_valid_access_token()

    def get_refresh_token(self) -> str:
        """Get refresh token.

        Returns:
            Refresh token

        Raises:
            ValueError: If no refresh token available
        """
        if (
            self.authenticator._tokens is None
            or "refresh_token" not in self.authenticator._tokens
        ):
            raise ValueError("No refresh token available")
        return str(self.authenticator._tokens["refresh_token"])

    def refresh_access_token(self) -> str:
        """Force refresh of access token.

        Returns:
            New access token
        """
        tokens = self.authenticator.refresh_access_token()
        return str(tokens["access_token"])

    def get_token_expiry(self) -> datetime:
        """Get token expiration datetime.

        Returns:
            Expiration datetime

        Raises:
            ValueError: If no tokens available
        """
        if (
            self.authenticator._tokens is None
            or "expires_at" not in self.authenticator._tokens
        ):
            raise ValueError("No token expiration info available")
        return datetime.fromtimestamp(self.authenticator._tokens["expires_at"])

    def get_authorization_data(self) -> AuthorizationData:
        """Get BingAds SDK AuthorizationData (Microsoft-specific method).

        Returns:
            Configured AuthorizationData for BingAds SDK
        """
        return self.authenticator.get_authorization_data()
