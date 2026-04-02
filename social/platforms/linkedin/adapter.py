"""LinkedIn Ads Adapter Module.

This module provides a completely independent adapter for LinkedIn Ads API v202509.
It follows SOLID principles with no base class inheritance, using only protocol contracts.

Key Features:
- Independent implementation (no base classes)
- Protocol compliance (TokenProvider, DataSink)
- Complete type hints and docstrings
- URN-based resource identification
- Dependency management (insights needs campaigns, creatives needs insights)

Architecture:
- LinkedInAdapter: Main adapter class
- LinkedInHTTPClient: HTTP communication layer
- Protocol-based dependency injection
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
import urllib.parse

import pandas as pd
from loguru import logger

from social.core.exceptions import APIError, ConfigurationError
from social.core.protocols import DataSink, TokenProvider
from social.platforms.linkedin.http_client import LinkedInHTTPClient


class LinkedInAdapter:
    """Independent adapter for LinkedIn Marketing API v202509.

    This adapter provides methods for extracting data from LinkedIn Ads API
    without inheriting from any base class. It uses protocol-based contracts
    for flexibility and testability.

    Attributes:
        token_provider: Provider for OAuth2 access tokens
        http_client: LinkedIn-specific HTTP client
        data_sink: Optional data sink for database queries
    """

    def __init__(
        self,
        token_provider: TokenProvider,
        data_sink: Optional[DataSink] = None,
    ):
        """Initialize LinkedIn adapter.

        Args:
            token_provider: Provider for authentication tokens
            data_sink: Optional data sink for database queries
        """
        self.token_provider = token_provider
        self.data_sink = data_sink

        # Initialize HTTP client with access token
        access_token = token_provider.get_access_token()
        self.http_client = LinkedInHTTPClient(access_token=access_token)

        logger.info("LinkedInAdapter initialized")

    def get_campaigns(self, account_id: str) -> List[Dict[str, Any]]:
        """Get campaigns for a specific account.

        Args:
            account_id: LinkedIn account ID (numeric string)

        Returns:
            List of campaign dictionaries with metadata

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching campaigns for account {account_id}")

        try:
            url = f"https://api.linkedin.com/rest/adAccounts/{account_id}/adCampaigns"

            # LinkedIn API 202509 - MUST use exact format from old working code
            params = {
                "q": "search",
            }

            # CRITICAL: Use exact format from old working code
            # Format: search=(status:(values:List(ACTIVE,PAUSED,COMPLETED,ARCHIVED)))
            no_encoded_params = {
                "search": "(status:(values:List(ACTIVE,PAUSED,COMPLETED,ARCHIVED)))"
            }

            response = self.http_client.get(url=url, params=params, no_encoded_params=no_encoded_params)

            campaigns = response.get("elements", [])
            logger.success(f"Retrieved {len(campaigns)} campaigns for account {account_id}")
            return campaigns

        except Exception as e:
            logger.error(f"Failed to fetch campaigns for account {account_id}: {e}")
            raise APIError(
                f"Failed to fetch campaigns for account {account_id}",
                details={"account_id": account_id, "error": str(e)}
            )

    def get_insights(
        self,
        campaign_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Get insights (performance metrics) for a specific campaign.

        Args:
            campaign_id: Campaign ID (numeric string)
            start_date: Start date for metrics
            end_date: End date for metrics

        Returns:
            List of insight records with daily metrics

        Raises:
            APIError: If API request fails
        """
        logger.debug(f"Fetching insights for campaign {campaign_id}")

        try:
            url = "https://api.linkedin.com/rest/adAnalytics"

            # Non-encoded parameters (LinkedIn special format)
            campaign_urn_param = self.http_client.format_campaign_urns_for_insights([campaign_id])
            date_param = self.http_client.format_date_range(
                start_date.year,
                start_date.month,
                start_date.day,
                end_date.year,
                end_date.month,
                end_date.day
            )

            fields = [
                "actionClicks",
                "adUnitClicks",
                "clicks",
                "comments",
                "costInLocalCurrency",
                "landingPageClicks",
                "likes",
                "reactions",
                "shares",
                "totalEngagements",
                "dateRange",
                "pivotValues",
                "impressions",
                "externalWebsiteConversions",
                "conversionValueInLocalCurrency"
            ]
            fields_str = self.http_client.format_fields(fields)

            # Simple parameters that will be URL-encoded normally
            # timeGranularity=DAILY returns daily breakdown with date dimension
            # This allows maintaining historical data in source table
            params = {
                "q": "analytics",
                "pivot": "CREATIVE",
                "timeGranularity": "DAILY",
            }

            # Complex parameters must NOT be URL-encoded
            no_encoded_params = {
                "campaigns": campaign_urn_param,
                "dateRange": date_param,
                "fields": fields_str,
            }

            response = self.http_client.get(
                url=url,
                params=params,
                no_encoded_params=no_encoded_params
            )

            insights = response.get("elements", [])
            logger.debug(f"Retrieved {len(insights)} insight records for campaign {campaign_id}")
            return insights

        except Exception as e:
            logger.error(f"Failed to fetch insights for campaign {campaign_id}: {e}")
            raise APIError(
                f"Failed to fetch insights for campaign {campaign_id}",
                details={"campaign_id": campaign_id, "error": str(e)}
            )

    def get_creatives(
        self,
        account_id: str,
        creative_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get creative details for a specific creative.

        LinkedIn API v202601 for creatives endpoint returns all fields by default,
        including: id, campaign, status, type, intendedStatus, createdAt, lastModifiedAt.
        The 'fields' parameter is NOT supported for the creatives endpoint.

        Args:
            account_id: Account ID (numeric string)
            creative_id: Creative ID (numeric string)

        Returns:
            Creative dictionary or None if not found

        Raises:
            APIError: If API request fails (except 404)
        """
        logger.debug(f"Fetching creative {creative_id} from account {account_id}")

        try:
            # URL-encode the creative URN for path parameter
            creative_urn = self.http_client.format_creative_urn_encoded(creative_id)
            url = f"https://api.linkedin.com/rest/adAccounts/{account_id}/creatives/{creative_urn}"

            # Note: Do NOT use 'fields' parameter - it's not supported by this endpoint
            # API returns all fields by default
            response = self.http_client.get(url=url, params={})

            logger.debug(f"Retrieved creative {creative_id}")
            return response

        except Exception as e:
            # 404 is expected when creative not in this account
            if "404" in str(e):
                logger.debug(f"Creative {creative_id} not found in account {account_id}")
                return None
            else:
                logger.error(f"Failed to fetch creative {creative_id}: {e}")
                raise APIError(
                    f"Failed to fetch creative {creative_id}",
                    details={"account_id": account_id, "creative_id": creative_id, "error": str(e)}
                )

    def get_audiences(self, account_id: str) -> List[Dict[str, Any]]:
        """Get audience segments for a specific account.

        Args:
            account_id: Account ID (numeric string)

        Returns:
            List of audience dictionaries

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching audiences for account {account_id}")

        try:
            url = "https://api.linkedin.com/rest/adSegments"

            # Create account URN parameter - MUST match old working code
            # Format: List(urn%3Ali%3AsponsoredAccount%3A{id})
            account_urn_encoded = self.http_client.format_account_urn_for_audiences(account_id)

            params = {
                "q": "accounts",
                "count": "400",
            }

            # CRITICAL: accounts parameter must NOT be URL-encoded (old working format)
            no_encoded_params = {
                "accounts": account_urn_encoded
            }

            response = self.http_client.get(url=url, params=params, no_encoded_params=no_encoded_params)

            audiences = response.get("elements", [])
            logger.success(f"Retrieved {len(audiences)} audiences for account {account_id}")
            return audiences

        except Exception as e:
            logger.error(f"Failed to fetch audiences for account {account_id}: {e}")
            raise APIError(
                f"Failed to fetch audiences for account {account_id}",
                details={"account_id": account_id, "error": str(e)}
            )

    def get_account(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get account details.

        Args:
            account_id: Account ID (numeric string)

        Returns:
            Account dictionary or None if not found

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching account details for {account_id}")

        try:
            url = "https://api.linkedin.com/rest/adAccounts"
            params = {
                "q": "search",
            }

            response = self.http_client.get(url=url, params=params)

            # Find the specific account in the results
            accounts = response.get("elements", [])
            for account in accounts:
                # Extract ID from URN or compare directly
                acc_id = str(account.get("id", ""))
                if ":" in acc_id:
                    acc_id = acc_id.split(":")[-1]

                if acc_id == str(account_id):
                    logger.success(f"Found account {account_id}")
                    return account

            logger.warning(f"Account {account_id} not found in response")
            return None

        except Exception as e:
            logger.error(f"Failed to fetch account {account_id}: {e}")
            raise APIError(
                f"Failed to fetch account {account_id}",
                details={"account_id": account_id, "error": str(e)}
            )

    def get_demographics_insights(
        self,
        pivot: str,
        account_ids: Optional[List[str]] = None,
        campaign_ids: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        time_granularity: str = "ALL"
    ) -> List[Dict[str, Any]]:
        """Get demographic breakdown insights from LinkedIn Ads API.

        This method retrieves demographic data (company, job title, seniority, etc.)
        using the Analytics Finder with demographic pivots.

        Args:
            pivot: Demographic pivot type. Valid values:
                - MEMBER_COMPANY: Company breakdown
                - MEMBER_JOB_TITLE: Job title breakdown
                - MEMBER_SENIORITY: Seniority level breakdown
                - MEMBER_INDUSTRY: Industry breakdown
                - MEMBER_JOB_FUNCTION: Job function breakdown
                - MEMBER_COMPANY_SIZE: Company size breakdown
            account_ids: Optional list of account IDs to filter
            campaign_ids: Optional list of campaign IDs to filter
            start_date: Start date for insights
            end_date: End date for insights
            time_granularity: Time granularity (ALL, DAILY, MONTHLY, YEARLY)
                - ALL is recommended for demographics to reduce noise

        Returns:
            List of demographic insight dictionaries

        Raises:
            APIError: If API request fails

        Note:
            - Only ONE demographic pivot can be used per request
            - Cannot combine demographic pivot with campaign/creative pivot
            - Minimum threshold: 3 events (impressions/clicks/conversions)
            - Data approximated ±3 units for privacy
            - Latency: 12-24 hours for demographic data
            - Retention: 2 years (vs 10 years for performance data)
        """
        logger.info(f"Fetching demographic insights with pivot: {pivot}")

        try:
            url = "https://api.linkedin.com/rest/adAnalytics"

            # Build date range parameter
            if not start_date:
                # Default to last 90 days
                from datetime import timedelta
                end_date = end_date or datetime.now()
                start_date = end_date - timedelta(days=90)

            if not end_date:
                end_date = datetime.now()

            date_param = self.http_client.format_date_range(
                start_date.year,
                start_date.month,
                start_date.day,
                end_date.year,
                end_date.month,
                end_date.day
            )

            # Build accounts parameter
            if account_ids:
                # Format: List(urn:li:sponsoredAccount:123,urn:li:sponsoredAccount:456)
                account_urns = [f"urn:li:sponsoredAccount:{acc_id}" for acc_id in account_ids]
                # URL-encode each URN
                encoded_urns = [urllib.parse.quote(urn) for urn in account_urns]
                accounts_param = f"List({','.join(encoded_urns)})"
            else:
                accounts_param = None

            # Build campaigns parameter (optional filter)
            campaigns_param = None
            if campaign_ids:
                # Format: List(urn:li:sponsoredCampaign:123,urn:li:sponsoredCampaign:456)
                campaign_urns = [f"urn:li:sponsoredCampaign:{cid}" for cid in campaign_ids]
                encoded_urns = [urllib.parse.quote(urn) for urn in campaign_urns]
                campaigns_param = f"List({','.join(encoded_urns)})"

            # Fields for demographic insights
            fields = [
                "impressions",
                "clicks",
                "costInLocalCurrency",
                "approximateUniqueImpressions",
                "landingPageClicks",
                "likes",
                "shares",
                "comments",
                "externalWebsiteConversions",
                "conversionValueInLocalCurrency",
                "pivotValues",
                "dateRange"  # Include if using DAILY granularity
            ]
            fields_str = self.http_client.format_fields(fields)

            # Simple parameters (will be URL-encoded)
            params = {
                "q": "analytics",
                "pivot": pivot,
                "timeGranularity": time_granularity,
            }

            # Complex parameters (must NOT be URL-encoded)
            no_encoded_params = {
                "dateRange": date_param,
                "fields": fields_str,
            }

            if accounts_param:
                no_encoded_params["accounts"] = accounts_param

            if campaigns_param:
                no_encoded_params["campaigns"] = campaigns_param

            response = self.http_client.get(
                url=url,
                params=params,
                no_encoded_params=no_encoded_params
            )

            elements = response.get("elements", [])

            # Log paging information
            paging = response.get("paging", {})
            total = paging.get("total", len(elements))

            logger.success(f"Retrieved {len(elements)} demographic records (total: {total}) for pivot {pivot}")

            # Handle pagination if needed
            if len(elements) < total:
                logger.warning(f"Pagination detected: Retrieved {len(elements)}/{total} records. Consider implementing pagination.")

            return elements

        except Exception as e:
            logger.error(f"Failed to fetch demographic insights for pivot {pivot}: {e}")
            raise APIError(
                f"Failed to fetch demographic insights for pivot {pivot}",
                details={"pivot": pivot, "error": str(e)}
            )

    def lookup_organizations(self, organization_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Lookup organization details by IDs using LinkedIn organizationsLookup API.

        Args:
            organization_ids: List of organization IDs (numeric strings)

        Returns:
            Dictionary mapping organization ID to organization data
            {
                "1035": {
                    "name": "Microsoft",
                    ...
                }
            }

        Note:
            Uses endpoint: /rest/organizationsLookup?ids=List(id1,id2,...)
            Requires headers: Linkedin-Version, X-Restli-Protocol-Version
        """
        logger.info(f"Looking up {len(organization_ids)} organizations via organizationsLookup API")

        results = {}

        # Batch lookup in groups of 50 (API limit)
        batch_size = 50
        for i in range(0, len(organization_ids), batch_size):
            batch = organization_ids[i:i + batch_size]

            try:
                # Build ids parameter: ids=List(123,456,789)
                ids_str = ",".join(str(org_id) for org_id in batch)
                ids_param = f"List({ids_str})"

                url = "https://api.linkedin.com/rest/organizationsLookup"

                # Use no_encoded_params to avoid encoding List() syntax
                no_encoded_params = {"ids": ids_param}

                response = self.http_client.get(url=url, no_encoded_params=no_encoded_params)

                # Response format: {"results": {"123": {...}, "456": {...}}}
                batch_results = response.get("results", {})

                # Extract names
                for org_id, org_data in batch_results.items():
                    name = org_data.get("localizedName") or org_data.get("name", f"Organization {org_id}")
                    results[str(org_id)] = {
                        "name": name,
                        "id": org_id
                    }

                logger.debug(f"Batch {i//batch_size + 1}: Retrieved {len(batch_results)} organizations")

            except Exception as e:
                logger.warning(f"Batch lookup failed for {len(batch)} organizations: {e}")
                # Fallback to placeholders for failed batch
                for org_id in batch:
                    if str(org_id) not in results:
                        results[str(org_id)] = {
                            "name": f"Organization {org_id}",
                            "id": org_id
                        }

        logger.success(f"Retrieved {len(results)} organization details")
        return results

    def lookup_titles(self, title_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Lookup job title details by IDs using LinkedIn v2 titles API.

        Strategy: LinkedIn API doesn't support batch lookup with ids parameter.
        Instead, we fetch ALL titles with pagination and filter by the IDs we need.
        Results are cached for performance.

        Args:
            title_ids: List of title IDs (numeric strings)

        Returns:
            Dictionary mapping title ID to title data
            {
                "9": {
                    "name": "Software Engineer"
                }
            }
        """
        logger.info(f"Looking up {len(title_ids)} job titles via v2 titles API")

        results = {}

        # Convert title IDs to set for faster lookup
        needed_ids = set(str(tid) for tid in title_ids)

        try:
            # Fetch titles with pagination (limit iterations to avoid timeout)
            url = "https://api.linkedin.com/v2/titles"
            start = 0
            count = 100  # Fetch 100 at a time
            total_fetched = 0
            max_iterations = 30  # Limit to 30 iterations (3000 titles) to avoid timeout

            iteration = 0
            while len(results) < len(needed_ids) and iteration < max_iterations:
                params = {"start": start, "count": count}

                try:
                    response = self.http_client.get(url=url, params=params)
                except Exception as e:
                    logger.warning(f"Pagination request failed at start={start}: {e}")
                    break

                elements = response.get("elements", [])
                if not elements:
                    # No more data
                    break

                # Filter elements to only those we need
                for elem in elements:
                    title_id = str(elem.get("id"))
                    if title_id in needed_ids:
                        # Extract name
                        name_data = elem.get("name", {})
                        localized = name_data.get("localized", {})
                        name = localized.get("en_US")
                        if not name and localized:
                            name = list(localized.values())[0]
                        if not name:
                            name = f"Title {title_id}"

                        results[title_id] = {
                            "name": name,
                            "id": title_id
                        }

                total_fetched += len(elements)
                iteration += 1

                if iteration % 10 == 0:
                    logger.info(f"Progress: fetched {total_fetched} titles, found {len(results)}/{len(needed_ids)} needed")

                # Stop if we found all needed titles
                if len(results) >= len(needed_ids):
                    break

                # Move to next page
                start += count

                # Safety limit: don't fetch more than 10,000 titles
                if start >= 10000:
                    logger.warning("Reached safety limit of 10,000 titles")
                    break

            # Second pass: lookup missing titles one-by-one
            missing_ids = [tid for tid in needed_ids if tid not in results]
            if missing_ids:
                logger.info(f"First pass complete: {len(results)}/{len(needed_ids)} found. Attempting individual lookup for {len(missing_ids)} missing titles")

                for title_id in missing_ids:
                    try:
                        # Try individual lookup
                        title_url = f"https://api.linkedin.com/v2/titles/{title_id}"
                        title_response = self.http_client.get(url=title_url)

                        # Extract name
                        name_data = title_response.get("name", {})
                        localized = name_data.get("localized", {})
                        name = localized.get("en_US")
                        if not name and localized:
                            name = list(localized.values())[0]
                        if not name:
                            name = f"Title {title_id}"

                        results[title_id] = {
                            "name": name,
                            "id": title_id
                        }
                        logger.debug(f"Individual lookup success for title {title_id}: {name}")

                    except Exception as e:
                        logger.debug(f"Individual lookup failed for title {title_id}: {e}")
                        # Use placeholder
                        results[title_id] = {
                            "name": f"Title {title_id}",
                            "id": title_id
                        }

            # Final placeholders for any still missing
            for title_id in needed_ids:
                if title_id not in results:
                    results[title_id] = {
                        "name": f"Title {title_id}",
                        "id": title_id
                    }

            logger.success(f"Retrieved {len(results)} title details (fetched {total_fetched} via pagination, {len(missing_ids)} individual lookups)")

        except Exception as e:
            logger.error(f"Title lookup failed: {e}")
            # Fallback to placeholders for all
            for title_id in needed_ids:
                if title_id not in results:
                    results[title_id] = {
                        "name": f"Title {title_id}",
                        "id": title_id
                    }

        return results

    def close(self) -> None:
        """Close the HTTP client session."""
        if self.http_client:
            self.http_client.close()
            logger.debug("LinkedInAdapter closed")
