"""
Custom requests session for LinkedIn API.
Handles parameters without URL encoding for certain fields.
"""

import requests
from urllib.parse import urlencode


class NoQuotedCommasSession(requests.Session):
    """
    Custom session that allows parameters without URL encoding.
    Required for LinkedIn API which expects specific format for List() parameters.
    """

    def request(
        self, method, url, no_encoded_args=None, params=None, **kwargs
    ):
        """
        Make request with optional non-encoded parameters.

        Args:
            method: HTTP method
            url: Request URL
            no_encoded_args: Parameters that should not be URL encoded
            params: Regular parameters
            **kwargs: Additional arguments

        Returns:
            Response object
        """
        if no_encoded_args:
            # Build URL with non-encoded parameters
            encoded_params = urlencode(params) if params else ""

            # Add non-encoded params directly
            no_encoded_str = "&".join(
                f"{k}={v}" for k, v in no_encoded_args.items()
            )

            if encoded_params and no_encoded_str:
                url = f"{url}?{encoded_params}&{no_encoded_str}"
            elif encoded_params:
                url = f"{url}?{encoded_params}"
            elif no_encoded_str:
                url = f"{url}?{no_encoded_str}"

            return super().request(method, url, **kwargs)

        return super().request(method, url, params=params, **kwargs)
