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

# Get one test account
manager_id = "9474097201"

# List accessible customers
customer_service = client.get_service("CustomerService")
accessible_customers = customer_service.list_accessible_customers()
customer_ids = accessible_customers.resource_names

print(f"Found {len(customer_ids)} accessible customers")

# Find first non-manager enabled account
test_account = None
for customer_resource_name in customer_ids[:10]:  # Check first 10
    customer_id = customer_resource_name.split('/')[-1]

    # Skip manager account
    if customer_id == manager_id:
        continue

    try:
        # Try a simple query to see if account is accessible
        query = "SELECT customer.id, customer.status FROM customer LIMIT 1"
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id
        search_request.query = query

        response = googleads_service.search(request=search_request)
        for row in response:
            if row.customer.status.name == 'ENABLED':
                test_account = customer_id
                break

        if test_account:
            break
    except:
        continue

if not test_account:
    print("❌ No enabled test account found")
    sys.exit(1)

print(f"\n{'='*80}")
print(f"TESTING AUDIENCE API - Account: {test_account}")
print(f"{'='*80}\n")

# Query
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

query = f"""
SELECT
    ad_group_criterion.criterion_id,
    ad_group.id,
    ad_group_criterion.display_name,
    customer.id
FROM ad_group_criterion
WHERE segments.date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
AND campaign.status = 'ENABLED'
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
        print("❌ NO DATA RETURNED")
    else:
        df = pd.json_normalize(results)

        print(f"✅ Retrieved {len(df)} rows")
        print(f"\nRAW API COLUMNS (after json_normalize):")
        for i, col in enumerate(df.columns):
            print(f"  {i+1}. '{col}'")

        print(f"\nFIRST ROW VALUES:")
        for col in df.columns:
            val = df[col].iloc[0] if len(df) > 0 else None
            print(f"  '{col}': {val}")

        print(f"\nALL COLUMNS WITH 3 SAMPLE VALUES:")
        for col in df.columns:
            samples = df[col].head(3).tolist()
            print(f"  '{col}': {samples}")

except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
