"""Test audience API response."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from datetime import datetime, timedelta
from social.platforms.google.http_client import GoogleAdsClient
from social.infrastructure.file_token_provider import FileBasedTokenProvider
import pandas as pd

# Initialize
token_provider = FileBasedTokenProvider(platform="google", credentials_file=None)
client = GoogleAdsClient(
    token_provider=token_provider,
    config_file_path="social/platforms/google/google-ads-9474097201.yml",
    api_version="v19",
    manager_customer_id="9474097201"
)

# Get accounts
accounts = client.get_all_accounts()
test_account = None
for acc in accounts:
    if acc.get('status') == 'ENABLED' and not acc.get('manager'):
        test_account = acc['id']
        break

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
"""

print("Query:")
print(query)
print()

try:
    df = client.execute_query(
        customer_id=str(test_account),
        query=query,
        use_streaming=False
    )

    if df.empty:
        print("❌ NO DATA RETURNED")
    else:
        print(f"✅ Retrieved {len(df)} rows")
        print(f"\nRAW API COLUMNS:")
        print(df.columns.tolist())
        print(f"\nFIRST ROW (raw from API):")
        for col in df.columns:
            val = df[col].iloc[0] if len(df) > 0 else None
            print(f"  {col}: {val} (type: {type(val).__name__})")

        print(f"\nALL COLUMNS WITH SAMPLE VALUES:")
        for col in df.columns:
            samples = df[col].head(3).tolist()
            print(f"  '{col}': {samples}")

except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
