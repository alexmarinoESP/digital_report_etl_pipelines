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

# Test query on a single account
customer_id = "1884325396"
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

df = pd.json_normalize(all_results)

print(f"\n{'='*80}")
print("BEFORE PROCESSING:")
print(f"Columns: {list(df.columns)}")
print(f"\nFirst row:")
for col in df.columns:
    print(f"  {col}: {df.iloc[0][col]}")

print(f"\n{'='*80}")
print("APPLYING google_rename_columns()...")

processor = GoogleProcessor(df)
processed_df = processor.google_rename_columns().get_df()

print(f"\nAFTER google_rename_columns():")
print(f"Columns: {list(processed_df.columns)}")
print(f"\nFirst row:")
for col in processed_df.columns:
    print(f"  {col}: {processed_df.iloc[0][col]}")

print(f"\n{'='*80}")
print("Checking if display_name column exists:")
if 'display_name' in processed_df.columns:
    print(f"✓ YES! display_name values: {processed_df['display_name'].tolist()[:3]}")
else:
    print(f"✗ NO! Available columns: {list(processed_df.columns)}")
