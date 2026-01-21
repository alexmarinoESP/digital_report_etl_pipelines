"""
HTML cleaner service.
Implements IHtmlProcessor interface for HTML content cleaning.
"""

import re
from typing import Dict, List

from bs4 import BeautifulSoup

from newsletter.domain.interfaces import IHtmlProcessor
from newsletter.domain.models import Company


class HtmlCleanerService(IHtmlProcessor):
    """
    Service for cleaning HTML content.
    Implements IHtmlProcessor interface.

    Follows:
    - Single Responsibility: Only handles HTML cleaning
    - Open/Closed: Patterns can be extended via configuration
    - Dependency Inversion: Implements abstract interface
    """

    # Default placeholder patterns per company
    DEFAULT_PATTERNS: Dict[str, List[str]] = {
        "it": [
            r"\{\{.*?\}\}",      # Double curly braces {{...}}
            r"\[\[.*?\]\]",      # Double square brackets [[...]]
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

    def __init__(self, custom_patterns: Dict[str, List[str]] = None):
        """
        Initialize HTML cleaner.

        Args:
            custom_patterns: Optional custom patterns per company code
        """
        self._patterns = custom_patterns or self.DEFAULT_PATTERNS

    def _remove_placeholders(self, html: str, company_code: str) -> str:
        """
        Remove placeholder patterns from HTML.

        Args:
            html: HTML string
            company_code: Company code for pattern selection

        Returns:
            HTML with placeholders removed
        """
        patterns = self._patterns.get(company_code, self._patterns.get("it", []))

        for pattern in patterns:
            html = re.sub(pattern, "", html, flags=re.DOTALL)

        return html

    def _normalize_whitespace(self, html: str) -> str:
        """
        Normalize whitespace in HTML.

        Args:
            html: HTML string

        Returns:
            HTML with normalized whitespace
        """
        # Remove excessive whitespace between tags
        html = re.sub(r">\s+<", "><", html)
        # Normalize multiple spaces to single space
        html = re.sub(r" +", " ", html)
        return html.strip()

    def _fix_structure(self, html: str) -> str:
        """
        Fix HTML structure using BeautifulSoup.

        Args:
            html: HTML string

        Returns:
            Structurally valid HTML
        """
        soup = BeautifulSoup(html, "html.parser")
        return str(soup)

    def clean(self, html: str, company: Company) -> str:
        """
        Clean HTML content.

        Args:
            html: Raw HTML string
            company: Company for company-specific processing

        Returns:
            Cleaned HTML string
        """
        if not html:
            return ""

        # Step 1: Decode if bytes
        if isinstance(html, bytes):
            html = html.decode("utf-8", errors="ignore")

        # Step 2: Remove placeholders
        html = self._remove_placeholders(html, company.code)

        # Step 3: Fix structure
        html = self._fix_structure(html)

        return html

    def extract_body(self, html: str) -> str:
        """
        Extract body content from HTML.

        Args:
            html: Full HTML string

        Returns:
            Body content or original if no body found
        """
        soup = BeautifulSoup(html, "html.parser")
        body = soup.find("body")

        if body:
            return str(body)
        return html
