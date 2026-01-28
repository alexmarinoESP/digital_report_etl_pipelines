"""LinkedIn Organic Posts Data Processor Module.

This module provides a chainable processor for transforming LinkedIn organic posts data.
It implements the Fluent Interface pattern for clean, readable data transformations.

Key Features:
- Chainable methods (fluent interface)
- Type-safe transformations
- URN extraction and date handling
- Emoji and special character cleaning

Architecture:
- LinkedInPostsProcessor: Main processor class with chainable methods
- Each method returns self for chaining
- get_df() returns the final DataFrame
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from social.platforms.linkedin_posts.constants import ORGANIZATION_MAP
from social.platforms.linkedin_posts.http_client import LinkedInPostsHTTPClient


class LinkedInPostsProcessor:
    """Chainable data processor for LinkedIn organic posts data.

    This processor provides a fluent interface for transforming raw API responses
    into clean, database-ready DataFrames.

    Example:
        >>> processor = LinkedInPostsProcessor(raw_data)
        >>> clean_df = (processor
        ...     .to_posts_dataframe()
        ...     .add_row_loaded_date()
        ...     .get_df())

    Attributes:
        data: The raw data being processed
        df: The DataFrame after transformation
    """

    def __init__(self, data: Any):
        """Initialize processor with raw data.

        Args:
            data: Raw data from API response (list or dict)
        """
        self.data = data
        self.df = pd.DataFrame()
        logger.debug(f"LinkedInPostsProcessor initialized")

    def get_df(self) -> pd.DataFrame:
        """Get the processed DataFrame.

        Returns:
            Processed DataFrame
        """
        return self.df

    # ============================================================================
    # Posts Transformation
    # ============================================================================

    def to_posts_dataframe(self) -> "LinkedInPostsProcessor":
        """Transform raw posts data to DataFrame.

        Extracts relevant fields from post objects.

        Returns:
            Self for chaining
        """
        if not self.data:
            self.df = pd.DataFrame()
            return self

        posts_data = []
        for post in self.data:
            try:
                post_id = LinkedInPostsHTTPClient.extract_id_from_urn(post.get("id", ""))
                org_id = post.get("_organization_id") or LinkedInPostsHTTPClient.extract_id_from_urn(post.get("author", ""))

                # Determine content type
                content = post.get("content", {})
                content_type = self._determine_content_type(content)

                # Extract media URL if available
                media_url = self._extract_media_url(content)
                media_title = self._extract_media_title(content)

                # Check if reshare
                reshare_context = post.get("reshareContext", {})
                is_reshare = bool(reshare_context)
                original_post_id = ""
                if is_reshare:
                    original_post_id = LinkedInPostsHTTPClient.extract_id_from_urn(
                        reshare_context.get("root", "") or reshare_context.get("parent", "")
                    )

                post_record = {
                    "post_id": post_id,
                    "organization_id": org_id,
                    "commentary": self._clean_text(post.get("commentary", "")),
                    "visibility": post.get("visibility", ""),
                    "lifecycle_state": post.get("lifecycleState", ""),
                    "content_type": content_type,
                    "media_url": media_url,
                    "media_title": media_title,
                    "post_url": f"https://www.linkedin.com/feed/update/{post.get('id', '')}",
                    "is_reshare": is_reshare,
                    "original_post_id": original_post_id if original_post_id else None,
                    "created_at": self._ms_to_datetime(post.get("createdAt")),
                    "published_at": self._ms_to_datetime(post.get("publishedAt")),
                    "last_modified_at": self._ms_to_datetime(post.get("lastModifiedAt")),
                }
                posts_data.append(post_record)

            except Exception as e:
                logger.warning(f"Failed to process post {post.get('id', 'unknown')}: {e}")

        self.df = pd.DataFrame(posts_data)
        logger.info(f"Transformed {len(self.df)} posts to DataFrame")
        return self

    def to_posts_insights_dataframe(
        self,
        posts: List[Dict],
        stats: List[Dict],
    ) -> "LinkedInPostsProcessor":
        """Transform posts statistics to insights DataFrame.

        Matches posts with their statistics.

        Args:
            posts: List of post dictionaries
            stats: List of statistics dictionaries

        Returns:
            Self for chaining
        """
        if not posts:
            self.df = pd.DataFrame()
            return self

        # Create lookup for stats by post URN
        stats_lookup = {}
        for stat in stats:
            post_urn = stat.get("share") or stat.get("ugcPost")
            if post_urn:
                stats_lookup[post_urn] = stat.get("totalShareStatistics", {})

        insights_data = []
        for post in posts:
            post_urn = post.get("id", "")
            post_id = LinkedInPostsHTTPClient.extract_id_from_urn(post_urn)
            org_id = post.get("_organization_id") or LinkedInPostsHTTPClient.extract_id_from_urn(post.get("author", ""))

            # Get stats for this post
            post_stats = stats_lookup.get(post_urn, {})

            insight_record = {
                "post_id": post_id,
                "organization_id": org_id,
                "impression_count": post_stats.get("impressionCount", 0),
                "unique_impression_count": post_stats.get("uniqueImpressionsCount", 0),
                "click_count": post_stats.get("clickCount", 0),
                "like_count": post_stats.get("likeCount", 0),
                "comment_count": post_stats.get("commentCount", 0),
                "share_count": post_stats.get("shareCount", 0),
                "engagement": post_stats.get("engagement", 0),
                "video_views": 0,  # Not available in share stats
                "video_view_time_ms": 0,
            }
            insights_data.append(insight_record)

        self.df = pd.DataFrame(insights_data)
        logger.info(f"Transformed {len(self.df)} post insights to DataFrame")
        return self

    # ============================================================================
    # Page Statistics Transformation
    # ============================================================================

    def to_page_stats_dataframe(self) -> "LinkedInPostsProcessor":
        """Transform page statistics to DataFrame.

        Returns:
            Self for chaining
        """
        if not self.data:
            self.df = pd.DataFrame()
            return self

        stats_data = []
        for elem in self.data:
            org_id = elem.get("_organization_id", "")
            total_stats = elem.get("totalPageStatistics", {})
            views = total_stats.get("views", {})

            # Check if time-bound data
            time_range = elem.get("timeRange", {})
            if time_range:
                date = self._ms_to_datetime(time_range.get("start"))
                if date:
                    date = date.date()
            else:
                date = datetime.now().date()

            stat_record = {
                "organization_id": org_id,
                "date": date,
                "all_page_views": views.get("allPageViews", {}).get("pageViews", 0),
                "unique_page_views": views.get("uniquePageViews", {}).get("pageViews", 0) if "uniquePageViews" in views else 0,
                "all_desktop_page_views": views.get("allDesktopPageViews", {}).get("pageViews", 0),
                "all_mobile_page_views": views.get("allMobilePageViews", {}).get("pageViews", 0),
                "overview_page_views": views.get("overviewPageViews", {}).get("pageViews", 0),
                "careers_page_views": views.get("careersPageViews", {}).get("pageViews", 0),
                "jobs_page_views": views.get("jobsPageViews", {}).get("pageViews", 0),
                "life_at_page_views": views.get("lifeAtPageViews", {}).get("pageViews", 0),
                "desktop_custom_button_clicks": 0,  # May not be in response
                "mobile_custom_button_clicks": 0,
            }
            stats_data.append(stat_record)

        self.df = pd.DataFrame(stats_data)
        logger.info(f"Transformed {len(self.df)} page stats to DataFrame")
        return self

    # ============================================================================
    # Follower Statistics Transformation
    # ============================================================================

    def to_follower_timebound_dataframe(self) -> "LinkedInPostsProcessor":
        """Transform time-bound follower statistics to DataFrame.

        Returns:
            Self for chaining
        """
        if not self.data:
            self.df = pd.DataFrame()
            return self

        follower_data = []
        for elem in self.data:
            org_id = elem.get("_organization_id", "")
            time_range = elem.get("timeRange", {})
            gains = elem.get("followerGains", {})

            date = self._ms_to_datetime(time_range.get("start"))
            if date:
                date = date.date()

            organic_gain = gains.get("organicFollowerGain", 0)
            paid_gain = gains.get("paidFollowerGain", 0)

            record = {
                "organization_id": org_id,
                "date": date,
                "total_followers": None,  # Not available in time-bound response
                "organic_follower_gain": organic_gain,
                "paid_follower_gain": paid_gain,
                "net_follower_change": organic_gain + paid_gain,
            }
            follower_data.append(record)

        self.df = pd.DataFrame(follower_data)
        logger.info(f"Transformed {len(self.df)} follower entries to DataFrame")
        return self

    def to_follower_demographics_dataframe(self) -> "LinkedInPostsProcessor":
        """Transform follower demographics to DataFrame.

        Flattens the demographics breakdowns into rows.

        Returns:
            Self for chaining
        """
        if not self.data:
            self.df = pd.DataFrame()
            return self

        demo_data = []

        # Process each demographic type
        demo_types = [
            ("followerCountsByFunction", "FUNCTION", "function"),
            ("followerCountsByIndustry", "INDUSTRY", "industry"),
            ("followerCountsBySeniority", "SENIORITY", "seniority"),
            ("followerCountsByGeoCountry", "GEO_COUNTRY", "geo"),
            ("followerCountsByStaffCountRange", "STAFF_COUNT", "staffCountRange"),
        ]

        for elem in self.data:
            org_id = elem.get("_organization_id", "")

            for key, demo_type, value_key in demo_types:
                entries = elem.get(key, [])
                for entry in entries:
                    counts = entry.get("followerCounts", {})
                    demo_urn = entry.get(value_key, "")
                    demo_value = LinkedInPostsHTTPClient.extract_id_from_urn(demo_urn) if demo_urn else ""

                    record = {
                        "organization_id": org_id,
                        "demographic_type": demo_type,
                        "demographic_value": demo_value,
                        "demographic_urn": demo_urn,
                        "organic_follower_count": counts.get("organicFollowerCount", 0),
                        "paid_follower_count": counts.get("paidFollowerCount", 0),
                    }
                    demo_data.append(record)

        self.df = pd.DataFrame(demo_data)
        logger.info(f"Transformed {len(self.df)} demographic entries to DataFrame")
        return self

    # ============================================================================
    # Common Transformations
    # ============================================================================

    def add_row_loaded_date(self) -> "LinkedInPostsProcessor":
        """Add row_loaded_date column with current timestamp.

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        self.df["row_loaded_date"] = datetime.now()
        logger.debug("Added row_loaded_date column")
        return self

    def add_last_updated_date(self) -> "LinkedInPostsProcessor":
        """Add last_updated_date column with current timestamp.

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        self.df["last_updated_date"] = datetime.now()
        logger.debug("Added last_updated_date column")
        return self

    def clean_text_columns(self, columns: List[str]) -> "LinkedInPostsProcessor":
        """Clean text columns by removing emojis and special characters.

        Args:
            columns: List of column names to clean

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(
                    lambda x: self._clean_text(str(x)) if pd.notna(x) else x
                )

        logger.debug(f"Cleaned text columns: {columns}")
        return self

    # ============================================================================
    # Helper Methods
    # ============================================================================

    @staticmethod
    def _ms_to_datetime(ms: Optional[int]) -> Optional[datetime]:
        """Convert milliseconds timestamp to datetime.

        Args:
            ms: Milliseconds since epoch

        Returns:
            datetime object or None
        """
        if ms is None:
            return None
        try:
            return datetime.fromtimestamp(ms / 1000)
        except (ValueError, OSError):
            return None

    @staticmethod
    def _determine_content_type(content: Dict) -> str:
        """Determine content type from content dictionary.

        Args:
            content: Content dictionary from post

        Returns:
            Content type string
        """
        if not content:
            return "NONE"
        if "multiImage" in content:
            return "MULTI_IMAGE"
        if "media" in content:
            media = content["media"]
            media_id = media.get("id", "")
            if "video" in media_id.lower():
                return "VIDEO"
            elif "image" in media_id.lower():
                return "IMAGE"
            elif "document" in media_id.lower():
                return "DOCUMENT"
            return "MEDIA"
        if "article" in content:
            return "ARTICLE"
        if "poll" in content:
            return "POLL"
        return "NONE"

    @staticmethod
    def _extract_media_url(content: Dict) -> str:
        """Extract media URL from content.

        Args:
            content: Content dictionary

        Returns:
            Media URL or empty string
        """
        if not content:
            return ""

        if "article" in content:
            return content["article"].get("source", "")

        if "media" in content:
            media_id = content["media"].get("id", "")
            # Media IDs are URNs, not actual URLs
            return media_id

        return ""

    @staticmethod
    def _extract_media_title(content: Dict) -> str:
        """Extract media title from content.

        Args:
            content: Content dictionary

        Returns:
            Media title or empty string
        """
        if not content:
            return ""

        if "article" in content:
            return content["article"].get("title", "")

        if "media" in content:
            return content["media"].get("title", "")

        return ""

    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean text by removing emojis and problematic characters.

        Args:
            text: Input text

        Returns:
            Cleaned text
        """
        if not text:
            return ""

        # Remove emoji characters
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags (iOS)
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "\U0001D400-\U0001D7FF"  # Mathematical Alphanumeric Symbols
            "]+",
            flags=re.UNICODE
        )
        text = emoji_pattern.sub('', text)

        # Replace newlines and carriage returns (break COPY FROM STDIN)
        text = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")

        # Replace pipe characters (used as delimiter in COPY statements)
        text = text.replace("|", "-")

        # Remove null characters
        text = text.replace("\x00", "")

        return text
