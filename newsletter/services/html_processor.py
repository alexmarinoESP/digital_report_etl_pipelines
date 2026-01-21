"""
HTML processing service.
Handles HTML cleaning and placeholder removal.
"""

import re
from typing import Optional

from bs4 import BeautifulSoup


class HtmlProcessor:
    """
    Service for processing HTML content.
    Removes placeholders and cleans HTML for rendering.
    """

    # Placeholder patterns for different companies
    PLACEHOLDER_PATTERNS = {
        "it": [
            r"\{\{.*?\}\}",  # Double curly braces
            r"\[\[.*?\]\]",  # Double square brackets
            r"<mj-raw>.*?</mj-raw>",  # MJ raw tags
        ],
        "es": [
            r"\{\{.*?\}\}",
            r"\[\[.*?\]\]",
            r"<mj-raw>.*?</mj-raw>",
        ],
        "vvit": [
            r"\{\{.*?\}\}",
            r"\[\[.*?\]\]",
            r"<mj-raw>.*?</mj-raw>",
        ],
    }

    def remove_placeholders(
        self, html_str: str, comp_flag: str = "it"
    ) -> str:
        """
        Remove placeholders from HTML string.

        Args:
            html_str: HTML string with placeholders
            comp_flag: Company flag for pattern selection

        Returns:
            Cleaned HTML string
        """
        patterns = self.PLACEHOLDER_PATTERNS.get(comp_flag, [])

        for pattern in patterns:
            html_str = re.sub(pattern, "", html_str, flags=re.DOTALL)

        return html_str

    def clean_html(self, html_str: str) -> str:
        """
        Clean HTML by removing unnecessary whitespace and fixing structure.

        Args:
            html_str: Raw HTML string

        Returns:
            Cleaned HTML string
        """
        soup = BeautifulSoup(html_str, "html.parser")
        return str(soup)

    def extract_body(self, html_str: str) -> Optional[str]:
        """
        Extract body content from HTML.

        Args:
            html_str: Full HTML string

        Returns:
            Body content or original string if no body found
        """
        soup = BeautifulSoup(html_str, "html.parser")
        body = soup.find("body")

        if body:
            return str(body)
        return html_str


def string_decoding(string: str) -> str:
    """
    Decode string from bytes if needed.

    Args:
        string: String or bytes to decode

    Returns:
        Decoded string
    """
    if isinstance(string, bytes):
        return string.decode("utf-8")
    return str(string)


def remove_placeholders(html_str: str, comp_flag: str) -> str:
    """
    Remove placeholders from HTML (legacy function).

    Args:
        html_str: HTML string
        comp_flag: Company flag

    Returns:
        Cleaned HTML string
    """
    processor = HtmlProcessor()
    return processor.remove_placeholders(html_str, comp_flag)
