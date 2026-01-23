"""
Utility functions for extracting Bitly links from SMS text.

Simple stateless functions following functional programming principles.
"""

import re
from typing import List

from loguru import logger

from sms.domain.models import BitlyLink


# Regex pattern for bit.ly URLs (matches http:// or https://)
BITLY_PATTERN = re.compile(r'https?://bit\.ly/\S+', re.IGNORECASE)


def extract_bitly_links(
    sms_text: str,
    message_id: int,
    activity_id: str,
) -> List[BitlyLink]:
    """
    Extract all Bitly links from SMS text.

    Uses regex pattern to find all bit.ly URLs and creates BitlyLink objects.
    Handles duplicates by removing them while preserving order.

    Args:
        sms_text: SMS message text to extract links from
        message_id: MAPP message ID (for BitlyLink reference)
        activity_id: Activity code (for BitlyLink reference)

    Returns:
        List of BitlyLink objects (without enriched data from API)

    Examples:
        >>> text = "Check https://bit.ly/abc123 and https://bit.ly/xyz789"
        >>> links = extract_bitly_links(text, 123, "ACT001")
        >>> len(links)
        2
        >>> links[0].bitly_short_url
        'https://bit.ly/abc123'
    """
    if not sms_text:
        return []

    # Find all matches
    matches = BITLY_PATTERN.findall(sms_text)

    if not matches:
        logger.debug(f"No Bitly links found in SMS {message_id}")
        return []

    # Remove duplicates while preserving order
    unique_urls = list(dict.fromkeys(matches))

    # Create BitlyLink objects
    links = [
        BitlyLink(
            message_id=message_id,
            activity_id=activity_id,
            bitly_short_url=url,
            # Note: bitly_long_url and total_clicks will be enriched later
        )
        for url in unique_urls
    ]

    logger.info(
        f"Extracted {len(links)} unique Bitly link(s) from SMS {message_id}"
    )

    return links


def has_bitly_links(sms_text: str) -> bool:
    """
    Check if SMS text contains any Bitly links.

    Args:
        sms_text: SMS message text to check

    Returns:
        True if text contains bit.ly URLs, False otherwise

    Examples:
        >>> has_bitly_links("Visit https://bit.ly/test")
        True
        >>> has_bitly_links("No links here")
        False
    """
    if not sms_text:
        return False

    return BITLY_PATTERN.search(sms_text) is not None


def count_bitly_links(sms_text: str) -> int:
    """
    Count number of unique Bitly links in SMS text.

    Args:
        sms_text: SMS message text to count links in

    Returns:
        Number of unique bit.ly URLs found

    Examples:
        >>> count_bitly_links("https://bit.ly/a and https://bit.ly/b")
        2
        >>> count_bitly_links("https://bit.ly/a and https://bit.ly/a")
        1
    """
    if not sms_text:
        return 0

    matches = BITLY_PATTERN.findall(sms_text)
    unique_urls = list(dict.fromkeys(matches))
    return len(unique_urls)
