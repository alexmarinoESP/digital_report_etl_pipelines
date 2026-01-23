"""URN utility functions for LinkedIn API."""
import re
from typing import Optional
from loguru import logger


def extract_id_from_urn(urn: str) -> Optional[str]:
    """
    Extract numeric ID from LinkedIn URN.

    Args:
        urn: LinkedIn URN (e.g., "urn:li:sponsoredCampaign:12345")

    Returns:
        Numeric ID as string, or None if not found

    Examples:
        >>> extract_id_from_urn("urn:li:sponsoredCampaign:12345")
        "12345"
    """
    if not urn or not isinstance(urn, str):
        return None

    match = re.search(r"\d+", urn)
    return match.group(0) if match else None


def extract_urn_segment(urn: str, segment_type: str) -> Optional[str]:
    """
    Extract specific segment from URN.

    Args:
        urn: LinkedIn URN
        segment_type: Segment type to extract (e.g., "sponsoredCampaign")

    Returns:
        Segment value or None
    """
    if not urn or not isinstance(urn, str):
        return None

    pattern = f"{segment_type}:([^:]+)"
    match = re.search(pattern, urn)
    return match.group(1) if match else None


def build_linkedin_urn(entity_type: str, entity_id: str) -> str:
    """
    Build LinkedIn URN from entity type and ID.

    Args:
        entity_type: Entity type (e.g., "sponsoredCampaign")
        entity_id: Entity ID

    Returns:
        Formatted URN

    Examples:
        >>> build_linkedin_urn("sponsoredCampaign", "12345")
        "urn:li:sponsoredCampaign:12345"
    """
    return f"urn:li:{entity_type}:{entity_id}"