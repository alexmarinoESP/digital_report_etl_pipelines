"""Debug script to check Google Ads API response for placement"""
import sys
from social.platforms.google.adapter import GoogleAdapter
from social.infrastructure.file_token_provider import FileBasedTokenProvider
import pandas as pd

# Initialize adapter
token_provider = FileBasedTokenProvider("google")
adapter = GoogleAdapter(
    config={
        "api_base_url": "https://googleads.googleapis.com",
        "api_version": "v19",
        "manager_id": "9474097201"
    },
    token_provider=token_provider
)

# Get placements for one account only (to debug faster)
print("Fetching placements...")
results = adapter.get_all_placements(day=10)  # Last 10 days

if isinstance(results, pd.DataFrame):
    print(f"\nReceived DataFrame with {len(results)} rows")
    print(f"\nColumns: {results.columns.tolist()}")
    print(f"\nFirst few rows:")
    print(results.head(10))

    # Check for 'id' column
    if 'id' in results.columns:
        print(f"\n✓ Column 'id' is present")
        print(f"Sample IDs: {results['id'].head().tolist()}")
    else:
        print(f"\n✗ Column 'id' is NOT present")

    # Check for ad_group related columns
    adgroup_cols = [col for col in results.columns if 'group' in col.lower() or 'adgroup' in col.lower()]
    if adgroup_cols:
        print(f"\nAd group related columns: {adgroup_cols}")
else:
    print(f"Received {type(results)}: {results}")
