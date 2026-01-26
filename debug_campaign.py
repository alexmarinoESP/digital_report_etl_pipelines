import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict
import pandas as pd
from social.platforms.google.processor import GoogleProcessor
from datetime import datetime, timedelta

# Initialize client
client = GoogleAdsClient.load_from_storage("social/platforms/google/google-ads-9474097201.yml")
google_ads_service = client.get_service("GoogleAdsService")

# Test query
customer_id = "1884325396"
end_date = datetime.now()
start_date = end_date - timedelta(days=150)

query = f"""
    SELECT campaign.start_date,
    campaign.end_date, campaign.name,
    campaign.id, campaign.serving_status,
    customer.id, campaign.status
    FROM campaign
    WHERE segments.date BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND '{end_date.strftime("%Y-%m-%d")}'
    LIMIT 5
"""

print(f"Querying customer {customer_id}...")
print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

response = google_ads_service.search_stream(customer_id=customer_id, query=query)

# Get first batch
all_results = []
for batch in response:
    dictobj = MessageToDict(batch._pb)
    for result in dictobj.get("results", []):
        all_results.append(result)
    break  # Only first batch

if not all_results:
    print("No results!")
    sys.exit(0)

df = pd.json_normalize(all_results)

print(f"\n{'='*80}")
print("STEP 1: Raw data from API")
print(f"Columns: {list(df.columns)}")
print(f"\nFirst row:")
for col in df.columns:
    print(f"  {col}: {df.iloc[0][col]}")

print(f"\n{'='*80}")
print("STEP 2: After handle_columns()")
processor = GoogleProcessor(df)
df2 = processor.handle_columns().get_df()
print(f"Columns: {list(df2.columns)}")
print(f"\nFirst row:")
for col in df2.columns:
    print(f"  {col}: {df2.iloc[0][col]}")

print(f"\n{'='*80}")
print("STEP 3: After deal_with_date(['startdate','enddate'])")
processor2 = GoogleProcessor(df)
df3 = processor2.handle_columns().deal_with_date(['startdate','enddate']).get_df()
print(f"Columns: {list(df3.columns)}")
print(f"\nFirst row start_date: {df3.iloc[0].get('start_date', 'NOT FOUND')}")
print(f"First row end_date: {df3.iloc[0].get('end_date', 'NOT FOUND')}")

print(f"\n{'='*80}")
print("STEP 4: After full processing pipeline")
processor3 = GoogleProcessor(df)
df4 = (processor3
    .handle_columns()
    .deal_with_date(['startdate','enddate'])
    .google_rename_columns()
    .get_df())
print(f"Columns: {list(df4.columns)}")
print(f"\nChecking key fields:")
print(f"  id: {df4.iloc[0].get('id', 'NOT FOUND')}")
print(f"  name: {df4.iloc[0].get('name', 'NOT FOUND')}")
print(f"  start_date: {df4.iloc[0].get('start_date', 'NOT FOUND')}")
print(f"  end_date: {df4.iloc[0].get('end_date', 'NOT FOUND')}")
print(f"  serving_status: {df4.iloc[0].get('serving_status', 'NOT FOUND')}")
print(f"  status: {df4.iloc[0].get('status', 'NOT FOUND')}")
print(f"  customer_id_google: {df4.iloc[0].get('customer_id_google', 'NOT FOUND')}")
