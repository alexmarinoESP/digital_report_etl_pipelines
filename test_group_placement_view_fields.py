"""
Test script to explore Google Ads API group_placement_view fields.

This script queries the Google Ads API to see what fields are actually returned
from group_placement_view and helps debug field availability issues.
"""
import sys
import json
from datetime import datetime, timedelta
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict
import pandas as pd

def test_group_placement_view():
    """Test what fields are returned from group_placement_view."""

    print("=" * 80)
    print("GOOGLE ADS API - GROUP_PLACEMENT_VIEW FIELD TEST")
    print("=" * 80)
    print()

    # Load Google Ads client
    config_path = "social/platforms/google/google-ads-9474097201.yml"
    print(f"Loading Google Ads client from: {config_path}")

    try:
        client = GoogleAdsClient.load_from_storage(config_path)
        print("✓ Client loaded successfully")
    except Exception as e:
        print(f"✗ Failed to load client: {e}")
        return 1

    # Use one customer account for testing (Microsoft - has lots of data)
    customer_id = "6094466021"
    print(f"\nTesting with customer ID: {customer_id}")

    # Date range - last 7 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    date_str_start = start_date.strftime("%Y-%m-%d")
    date_str_end = end_date.strftime("%Y-%m-%d")
    print(f"Date range: {date_str_start} to {date_str_end}")

    print("\n" + "=" * 80)
    print("TEST 1: Query with ad_group.id (to see if it's supported)")
    print("=" * 80)

    query_with_adgroup_id = f"""
    SELECT
        ad_group.id,
        ad_group.name,
        ad_group.resource_name,
        group_placement_view.resource_name,
        group_placement_view.placement,
        group_placement_view.placement_type,
        group_placement_view.display_name,
        group_placement_view.target_url,
        metrics.impressions,
        metrics.active_view_ctr,
        customer.id
    FROM group_placement_view
    WHERE segments.date BETWEEN '{date_str_start}' AND '{date_str_end}'
    AND campaign.status = 'ENABLED'
    LIMIT 5
    """

    print("\nQuery:")
    print(query_with_adgroup_id)
    print()

    try:
        google_ads_service = client.get_service("GoogleAdsService")
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id
        search_request.query = query_with_adgroup_id

        response = google_ads_service.search(request=search_request)

        results = []
        for row in response:
            row_dict = MessageToDict(row._pb)
            results.append(row_dict)

        if results:
            print(f"✓ Query succeeded! Retrieved {len(results)} rows")
            print("\n--- First row structure ---")
            print(json.dumps(results[0], indent=2))

            df = pd.json_normalize(results)
            print("\n--- DataFrame columns ---")
            print(df.columns.tolist())
            print("\n--- DataFrame preview ---")
            print(df.head())
        else:
            print("✗ Query returned no results")

    except Exception as e:
        print(f"✗ Query failed: {e}")
        print(f"Error type: {type(e).__name__}")

    print("\n" + "=" * 80)
    print("TEST 2: Query WITHOUT ad_group.id (current working query)")
    print("=" * 80)

    query_without_adgroup_id = f"""
    SELECT
        group_placement_view.resource_name,
        group_placement_view.placement,
        group_placement_view.placement_type,
        group_placement_view.display_name,
        group_placement_view.target_url,
        metrics.impressions,
        metrics.active_view_ctr,
        customer.id
    FROM group_placement_view
    WHERE segments.date BETWEEN '{date_str_start}' AND '{date_str_end}'
    AND campaign.status = 'ENABLED'
    LIMIT 5
    """

    print("\nQuery:")
    print(query_without_adgroup_id)
    print()

    try:
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id
        search_request.query = query_without_adgroup_id

        response = google_ads_service.search(request=search_request)

        results = []
        for row in response:
            row_dict = MessageToDict(row._pb)
            results.append(row_dict)

        if results:
            print(f"✓ Query succeeded! Retrieved {len(results)} rows")
            print("\n--- First row structure ---")
            print(json.dumps(results[0], indent=2))

            # Analyze resource_name format
            if 'groupPlacementView' in results[0] and 'resourceName' in results[0]['groupPlacementView']:
                resource_name = results[0]['groupPlacementView']['resourceName']
                print(f"\n--- Resource name analysis ---")
                print(f"Full resource_name: {resource_name}")

                # Try to extract ad_group_id
                import re
                match = re.search(r'/groupPlacementViews/(\d+)', resource_name)
                if match:
                    ad_group_id = match.group(1)
                    print(f"✓ Extracted ad_group_id: {ad_group_id}")
                else:
                    print("✗ Could not extract ad_group_id from resource_name")

            df = pd.json_normalize(results)
            print("\n--- DataFrame columns ---")
            print(df.columns.tolist())
            print("\n--- DataFrame preview ---")
            print(df.head())
        else:
            print("✗ Query returned no results")

    except Exception as e:
        print(f"✗ Query failed: {e}")
        print(f"Error type: {type(e).__name__}")

    print("\n" + "=" * 80)
    print("TEST 3: Minimal query to check basic fields")
    print("=" * 80)

    minimal_query = f"""
    SELECT
        group_placement_view.placement,
        metrics.impressions
    FROM group_placement_view
    WHERE segments.date DURING LAST_7_DAYS
    AND campaign.status = 'ENABLED'
    LIMIT 3
    """

    print("\nQuery:")
    print(minimal_query)
    print()

    try:
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id
        search_request.query = minimal_query

        response = google_ads_service.search(request=search_request)

        results = []
        for row in response:
            row_dict = MessageToDict(row._pb)
            results.append(row_dict)

        if results:
            print(f"✓ Query succeeded! Retrieved {len(results)} rows")
            print("\n--- All rows ---")
            for i, row in enumerate(results, 1):
                print(f"\nRow {i}:")
                print(json.dumps(row, indent=2))
        else:
            print("✗ Query returned no results")

    except Exception as e:
        print(f"✗ Query failed: {e}")
        print(f"Error type: {type(e).__name__}")

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print()
    print("This test helps identify:")
    print("1. Whether ad_group.id can be selected with group_placement_view")
    print("2. What fields are actually returned by the API")
    print("3. The format of group_placement_view.resource_name")
    print("4. How to extract ad_group_id from resource_name")
    print()
    print("Run this script to see the actual API response structure.")
    print()

    return 0

if __name__ == "__main__":
    sys.exit(test_group_placement_view())
