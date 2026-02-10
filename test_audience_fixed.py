"""Simple test for audience API response."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from datetime import datetime, timedelta
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict
import pandas as pd

# Initialize client
client = GoogleAdsClient.load_from_storage("social/platforms/google/google-ads-9474097201.yml")

# Get Google Ads service
googleads_service = client.get_service("GoogleAdsService")
manager_id = "9474097201"

print("Getting customer hierarchy...")

# Get hierarchy to find non-manager accounts
test_account = None
try:
    query = """
        SELECT
            customer_client.id,
            customer_client.manager,
            customer_client.status,
            customer_client.descriptive_name
        FROM customer_client
        WHERE customer_client.status = 'ENABLED'
    """

    search_request = client.get_type("SearchGoogleAdsRequest")
    search_request.customer_id = manager_id
    search_request.query = query

    response = googleads_service.search(request=search_request)

    for row in response:
        # Find first non-manager enabled account
        if not row.customer_client.manager and row.customer_client.status.name == 'ENABLED':
            test_account = str(row.customer_client.id)
            print(f"[OK] Found non-manager account: {test_account} - {row.customer_client.descriptive_name}")
            break

except Exception as e:
    print(f"[ERROR] Error getting hierarchy: {e}")

if not test_account:
    print("[ERROR] No enabled non-manager account found")
    sys.exit(1)

print(f"\n{'='*80}")
print(f"TESTING AUDIENCE API - Account: {test_account}")
print(f"{'='*80}\n")

# Query - match production query exactly (no segments.date)
query = """
SELECT
    ad_group_criterion.criterion_id,
    ad_group.id,
    ad_group_criterion.display_name,
    customer.id
FROM ad_group_criterion
WHERE campaign.status = 'ENABLED'
AND ad_group_criterion.type IN ('USER_LIST', 'USER_INTEREST', 'AUDIENCE')
LIMIT 10
"""

print("Query:")
print(query)
print()

try:
    search_request = client.get_type("SearchGoogleAdsRequest")
    search_request.customer_id = test_account
    search_request.query = query

    response = googleads_service.search(request=search_request)

    # Convert to list of dicts
    results = []
    for row in response:
        row_dict = MessageToDict(row._pb)
        results.append(row_dict)

    if not results:
        print("[ERROR] NO DATA RETURNED")
    else:
        df = pd.json_normalize(results)

        print(f"[OK] Retrieved {len(df)} rows")
        print(f"\n{'='*60}")
        print("RAW API COLUMNS (after json_normalize):")
        print(f"{'='*60}")
        for i, col in enumerate(df.columns):
            print(f"  {i+1}. '{col}'")

        print(f"\n{'='*60}")
        print("FIRST ROW VALUES:")
        print(f"{'='*60}")
        for col in df.columns:
            val = df[col].iloc[0] if len(df) > 0 else None
            print(f"  '{col}' = {val}")

        print(f"\n{'='*60}")
        print("ALL COLUMNS WITH 3 SAMPLE VALUES:")
        print(f"{'='*60}")
        for col in df.columns:
            samples = df[col].head(3).tolist()
            print(f"  '{col}': {samples}")

except Exception as e:
    print(f"[ERROR] ERROR: {e}")
    import traceback
    traceback.print_exc()
