import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict
import pandas as pd

# Initialize client
client = GoogleAdsClient.load_from_storage("social/platforms/google/google-ads-9474097201.yml")
google_ads_service = client.get_service("GoogleAdsService")

# Test query on a single account
customer_id = "1884325396"  # Try another account
query = """
    SELECT
      ad_group.id,
      ad_group_criterion.display_name,
      customer.id
    FROM ad_group_audience_view
    WHERE campaign.serving_status IN ('ENDED','SERVING')
    LIMIT 5
"""

print(f"Querying customer {customer_id}...")
response = google_ads_service.search(customer_id=customer_id, query=query)

all_results = []
for row in response:
    row_dict = MessageToDict(row._pb)
    all_results.append(row_dict)
    print(f"\nRaw row dict keys: {row_dict.keys()}")
    print(f"Full row: {row_dict}")

if all_results:
    df = pd.json_normalize(all_results)
    print(f"\n{'='*80}")
    print(f"DataFrame columns: {list(df.columns)}")
    print(f"\n{'='*80}")
    print("First row:")
    print(df.iloc[0].to_dict())
