"""NoQuotedCommasSession - Custom requests session for LinkedIn API.

LinkedIn's adAnalytics API requires certain parameters to NOT be URL-encoded:
- campaigns: List(urn:li:sponsoredCampaign:123)
- dateRange: (start:(year:2024,month:1,day:1))
- fields: field1,field2,field3
- search: (status:(values:List(ACTIVE,PAUSED)))

This session class ensures these parameters remain unencoded in the final URL.
"""

import re
import requests
from requests import Request
from typing import Any, Dict, Optional


class NoQuotedCommasSession(requests.Session):
    """Custom requests session that prevents URL encoding of specific parameters.

    This is required for LinkedIn Marketing API which expects certain complex
    parameters to remain in their raw format without URL encoding.
    """

    def send(self, *a, **kw):
        """Override send to replace URL-encoded params with raw values.

        Args:
            a[0]: PreparedRequest object
            a[1]: Dictionary of parameters that should NOT be URL-encoded
            **kw: Additional keyword arguments

        Returns:
            Response object
        """
        prep_request = a[0]

        # Check if we have no_encoded_args (passed as second positional arg)
        if len(a) > 1 and a[1]:
            no_encoded_args = a[1]

            # For each no_encoded parameter, check if it exists in URL and replace it
            # OR append it if it doesn't exist
            for key, value in no_encoded_args.items():
                # Pattern matches: key=<anything until & or end of string>
                pattern = f"{key}=([^&]+|[^&]+?$)"

                if re.search(pattern, prep_request.url):
                    # Replace existing parameter with raw value
                    prep_request.url = re.sub(pattern, f"{key}={value}", prep_request.url)
                else:
                    # Append parameter if it doesn't exist
                    separator = '&' if '?' in prep_request.url else '?'
                    prep_request.url = f"{prep_request.url}{separator}{key}={value}"

        # Call parent send with only the PreparedRequest
        return requests.Session.send(self, prep_request, **kw)

    def request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, Any]] = None,
        auth: Optional[Any] = None,
        timeout: Optional[int] = None,
        allow_redirects: bool = True,
        proxies: Optional[Dict[str, str]] = None,
        hooks: Optional[Dict[str, Any]] = None,
        stream: Optional[bool] = None,
        verify: Optional[bool] = None,
        cert: Optional[Any] = None,
        json: Optional[Dict[str, Any]] = None,
        no_encoded_args: Optional[Dict[str, Any]] = None,
    ):
        """Execute HTTP request with optional non-encoded parameters.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            params: Regular URL-encoded query parameters
            data: Request body data
            headers: HTTP headers
            cookies: Cookies to send
            files: Files to upload
            auth: Authentication credentials
            timeout: Request timeout in seconds
            allow_redirects: Whether to follow redirects
            proxies: Proxy configuration
            hooks: Request hooks
            stream: Stream response
            verify: Verify SSL certificates
            cert: Client certificate
            json: JSON data to send
            no_encoded_args: Parameters that should NOT be URL-encoded

        Returns:
            Response object
        """
        # Create request object with standard parameters
        req = Request(
            method=method.upper(),
            url=url,
            headers=headers,
            files=files,
            data=data or {},
            json=json,
            params=params or {},  # These will be URL-encoded by requests
            auth=auth,
            cookies=cookies,
            hooks=hooks,
        )

        # Prepare the request (this URL-encodes the params)
        prep = self.prepare_request(req)

        # Merge environment settings
        proxies = proxies or {}
        settings = self.merge_environment_settings(
            prep.url, proxies, stream, verify, cert
        )

        # Prepare send kwargs
        send_kwargs = {
            "timeout": timeout,
            "allow_redirects": allow_redirects,
        }
        send_kwargs.update(settings)

        # Send with no_encoded_args as second positional argument
        # This will be intercepted by our custom send() method
        resp = self.send(prep, no_encoded_args, **send_kwargs)

        return resp
