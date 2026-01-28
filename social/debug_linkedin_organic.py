#!/usr/bin/env python
"""Debug script to test LinkedIn Community Management API for organic posts.

This script tests the API endpoints to understand the response structure
before implementing the full module.
"""

import sys
import json
import urllib.parse
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from social.core.config import ConfigurationManager
from social.infrastructure.database import VerticaDataSink
from social.infrastructure.database_token_provider import DatabaseTokenProvider
from social.platforms.linkedin.http_client import LinkedInHTTPClient


# Organizations to test
ORGANIZATIONS = {
    "17857": "Esprinet",
    "1788340": "V-Valley the value of Esprinet",
}


def get_http_client() -> LinkedInHTTPClient:
    """Initialize HTTP client with token from database."""
    config_manager = ConfigurationManager()
    config = config_manager.load_config(test_mode=True)
    data_sink = VerticaDataSink(config=config.database, test_mode=True)
    db_provider = DatabaseTokenProvider("linkedin", data_sink)
    access_token = db_provider.get_access_token()
    return LinkedInHTTPClient(access_token=access_token)


def test_posts_api(http_client: LinkedInHTTPClient, org_id: str, org_name: str):
    """Test Posts API - Get posts by organization."""
    print(f"\n{'='*60}")
    print(f"TEST: Posts API for {org_name} (org_id: {org_id})")
    print(f"{'='*60}")

    # URL encode the organization URN
    org_urn = f"urn:li:organization:{org_id}"
    encoded_urn = urllib.parse.quote(org_urn, safe='')

    url = "https://api.linkedin.com/rest/posts"
    params = {
        "q": "author",
        "count": "10",
        "sortBy": "LAST_MODIFIED",
    }
    no_encoded_params = {
        "author": org_urn,
    }

    try:
        response = http_client.get(
            url=url,
            params=params,
            no_encoded_params=no_encoded_params,
            headers={"X-RestLi-Method": "FINDER"}
        )

        elements = response.get("elements", [])
        print(f"Found {len(elements)} posts")

        if elements:
            print(f"\nFirst post structure:")
            print(json.dumps(elements[0], indent=2, default=str)[:2000])

            # Extract key fields
            post = elements[0]
            print(f"\n--- Key Fields ---")
            print(f"ID: {post.get('id')}")
            print(f"Author: {post.get('author')}")
            print(f"Visibility: {post.get('visibility')}")
            print(f"Lifecycle State: {post.get('lifecycleState')}")
            print(f"Created At: {post.get('createdAt')}")
            print(f"Published At: {post.get('publishedAt')}")
            print(f"Commentary: {post.get('commentary', '')[:100]}...")

            # Check content structure
            content = post.get('content', {})
            print(f"Content keys: {content.keys() if content else 'None'}")

        return elements

    except Exception as e:
        print(f"ERROR: {e}")
        # Try to get response body for more details
        if hasattr(e, 'response') and e.response is not None:
            try:
                print(f"Response body: {e.response.text}")
            except:
                pass
        return []


def test_share_statistics_api(http_client: LinkedInHTTPClient, org_id: str, org_name: str, post_urns: list = None):
    """Test Organization Share Statistics API."""
    print(f"\n{'='*60}")
    print(f"TEST: Share Statistics API for {org_name}")
    print(f"{'='*60}")

    org_urn = f"urn:li:organization:{org_id}"

    url = "https://api.linkedin.com/rest/organizationalEntityShareStatistics"
    params = {
        "q": "organizationalEntity",
    }
    no_encoded_params = {
        "organizationalEntity": org_urn,
    }

    # If we have specific posts, get stats for them
    if post_urns:
        # Format: ugcPosts=List(urn:li:ugcPost:123,urn:li:ugcPost:456)
        posts_list = ",".join(post_urns[:5])  # Limit to 5
        no_encoded_params["ugcPosts"] = f"List({posts_list})"
        print(f"Requesting stats for posts: {posts_list}")

    try:
        response = http_client.get(
            url=url,
            params=params,
            no_encoded_params=no_encoded_params
        )

        elements = response.get("elements", [])
        print(f"Found {len(elements)} statistics entries")

        if elements:
            print(f"\nStatistics structure:")
            print(json.dumps(elements[0], indent=2, default=str))

            # Extract total stats
            for elem in elements:
                stats = elem.get("totalShareStatistics", {})
                post_id = elem.get("share") or elem.get("ugcPost") or "aggregate"
                print(f"\n--- Stats for {post_id} ---")
                print(f"Impressions: {stats.get('impressionCount', 0)}")
                print(f"Unique Impressions: {stats.get('uniqueImpressionsCount', 0)}")
                print(f"Clicks: {stats.get('clickCount', 0)}")
                print(f"Likes: {stats.get('likeCount', 0)}")
                print(f"Comments: {stats.get('commentCount', 0)}")
                print(f"Shares: {stats.get('shareCount', 0)}")
                print(f"Engagement: {stats.get('engagement', 0)}")

        return elements

    except Exception as e:
        print(f"ERROR: {e}")
        return []


def test_page_statistics_api(http_client: LinkedInHTTPClient, org_id: str, org_name: str):
    """Test Organization Page Statistics API."""
    print(f"\n{'='*60}")
    print(f"TEST: Page Statistics API for {org_name}")
    print(f"{'='*60}")

    org_urn = f"urn:li:organization:{org_id}"

    url = "https://api.linkedin.com/rest/organizationPageStatistics"
    params = {
        "q": "organization",
    }
    no_encoded_params = {
        "organization": org_urn,
    }

    try:
        response = http_client.get(
            url=url,
            params=params,
            no_encoded_params=no_encoded_params
        )

        elements = response.get("elements", [])
        print(f"Found {len(elements)} page statistics entries")

        if elements:
            print(f"\nPage Statistics structure:")
            print(json.dumps(elements[0], indent=2, default=str)[:2000])

        return elements

    except Exception as e:
        print(f"ERROR: {e}")
        return []


def test_follower_statistics_api(http_client: LinkedInHTTPClient, org_id: str, org_name: str):
    """Test Organization Follower Statistics API."""
    print(f"\n{'='*60}")
    print(f"TEST: Follower Statistics API for {org_name}")
    print(f"{'='*60}")

    org_urn = f"urn:li:organization:{org_id}"

    url = "https://api.linkedin.com/rest/organizationalEntityFollowerStatistics"
    params = {
        "q": "organizationalEntity",
    }
    no_encoded_params = {
        "organizationalEntity": org_urn,
    }

    try:
        response = http_client.get(
            url=url,
            params=params,
            no_encoded_params=no_encoded_params
        )

        elements = response.get("elements", [])
        print(f"Found {len(elements)} follower statistics entries")

        if elements:
            print(f"\nFollower Statistics structure:")
            # Truncate output as it can be large
            print(json.dumps(elements[0], indent=2, default=str)[:3000])

        return elements

    except Exception as e:
        print(f"ERROR: {e}")
        return []


def test_follower_statistics_timebound(http_client: LinkedInHTTPClient, org_id: str, org_name: str):
    """Test Organization Follower Statistics API with time intervals."""
    print(f"\n{'='*60}")
    print(f"TEST: Follower Statistics (Time-bound) for {org_name}")
    print(f"{'='*60}")

    org_urn = f"urn:li:organization:{org_id}"

    # Get last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    start_ts = int(start_date.timestamp() * 1000)
    end_ts = int(end_date.timestamp() * 1000)

    url = "https://api.linkedin.com/rest/organizationalEntityFollowerStatistics"
    params = {
        "q": "organizationalEntity",
    }
    no_encoded_params = {
        "organizationalEntity": org_urn,
        "timeIntervals": f"(timeRange:(start:{start_ts},end:{end_ts}),timeGranularityType:DAY)",
    }

    try:
        response = http_client.get(
            url=url,
            params=params,
            no_encoded_params=no_encoded_params
        )

        elements = response.get("elements", [])
        print(f"Found {len(elements)} time-bound follower entries")

        if elements:
            print(f"\nFirst few entries:")
            for elem in elements[:3]:
                time_range = elem.get("timeRange", {})
                gains = elem.get("followerGains", {})
                print(f"  Period: {time_range}")
                print(f"  Organic Gain: {gains.get('organicFollowerGain', 0)}")
                print(f"  Paid Gain: {gains.get('paidFollowerGain', 0)}")
                print()

        return elements

    except Exception as e:
        print(f"ERROR: {e}")
        return []


def main():
    print("="*60)
    print("LinkedIn Community Management API - Debug Test")
    print("="*60)

    # Initialize client
    print("\nInitializing HTTP client...")
    http_client = get_http_client()
    print("Client initialized successfully!")

    # Test each organization
    for org_id, org_name in ORGANIZATIONS.items():
        print(f"\n\n{'#'*60}")
        print(f"# TESTING ORGANIZATION: {org_name}")
        print(f"{'#'*60}")

        # 1. Test Posts API
        posts = test_posts_api(http_client, org_id, org_name)

        # 2. Test Share Statistics (aggregate)
        test_share_statistics_api(http_client, org_id, org_name)

        # 3. If we have posts, get specific post stats
        if posts:
            post_urns = [p.get("id") for p in posts[:5] if p.get("id")]
            if post_urns:
                print(f"\n--- Getting stats for specific posts ---")
                test_share_statistics_api(http_client, org_id, org_name, post_urns)

        # 4. Test Page Statistics
        test_page_statistics_api(http_client, org_id, org_name)

        # 5. Test Follower Statistics (lifetime)
        test_follower_statistics_api(http_client, org_id, org_name)

        # 6. Test Follower Statistics (time-bound)
        test_follower_statistics_timebound(http_client, org_id, org_name)

    print("\n" + "="*60)
    print("DEBUG TEST COMPLETED")
    print("="*60)


if __name__ == "__main__":
    main()
