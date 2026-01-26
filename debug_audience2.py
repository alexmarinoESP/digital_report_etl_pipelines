import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict
import pandas as pd
from social.platforms.google.processor import GoogleProcessor

# Initialize client
client = GoogleAdsClient.load_from_storage("social/platforms/google/google-ads-9474097201.yml")
google_ads_service = client.get_service("GoogleAdsService")

# Test query
customer_id = "1884325396"
query = """
    SELECT
      ad_group.id,
      ad_group_criterion.display_name,
      customer.id
    FROM ad_group_audience_view
    WHERE campaign.serving_status IN ('ENDED','SERVING')
    LIMIT 3
"""

print(f"Querying customer {customer_id}...")
response = google_ads_service.search(customer_id=customer_id, query=query)

all_results = []
for row in response:
    row_dict = MessageToDict(row._pb)
    all_results.append(row_dict)

df = pd.json_normalize(all_results)

print(f"\n{'='*80}")
print("STEP 1: Raw data from API")
print(f"Columns: {list(df.columns)}")
print(f"First row display_name: {df.iloc[0]['adGroupCriterion.displayName']}")

print(f"\n{'='*80}")
print("STEP 2: After handle_columns()")
processor = GoogleProcessor(df)
df2 = processor.handle_columns().get_df()
print(f"Columns: {list(df2.columns)}")
if 'displayname' in df2.columns:
    print(f"First row displayname: {df2.iloc[0]['displayname']}")
else:
    print(f"NO displayname column! Available: {list(df2.columns)}")

print(f"\n{'='*80}")
print("STEP 3: After google_rename_columns()")
processor2 = GoogleProcessor(df)
df3 = processor2.handle_columns().google_rename_columns().get_df()
print(f"Columns: {list(df3.columns)}")
if 'display_name' in df3.columns:
    print(f"SUCCESS! First row display_name: {df3.iloc[0]['display_name']}")
else:
    print(f"FAIL! NO display_name column! Available: {list(df3.columns)}")
