"""Quick test to verify Facebook token works.

Run with:
    python test_facebook_token.py

Requirements:
    pip install facebook-business pyyaml
"""

import yaml
from pathlib import Path

# Try importing Facebook SDK
try:
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from facebook_business.adobjects.campaign import Campaign
except ImportError:
    print("=" * 80)
    print("ERROR: facebook_business not installed")
    print("=" * 80)
    print("Install with: pip install facebook-business pyyaml")
    print()
    exit(1)

def test_facebook_token():
    """Test Facebook access token and account access."""

    # Load credentials
    creds_path = Path("social/config/credentials.yml")

    if not creds_path.exists():
        print(f"ERROR: Credentials file not found at {creds_path}")
        exit(1)

    with open(creds_path, 'r', encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    fb_creds = creds['facebook']
    access_token = fb_creds['access_token']
    app_id = fb_creds['app_id']
    app_secret = fb_creds['app_secret']
    account_ids = fb_creds['id_account']

    print()
    print("=" * 80)
    print("FACEBOOK TOKEN TEST")
    print("=" * 80)
    print(f"App ID: {app_id}")
    print(f"Token (first 30 chars): {access_token[:30]}...")
    print(f"Token (last 10 chars): ...{access_token[-10:]}")
    print(f"Number of accounts to test: {len(account_ids)}")
    print()

    # Initialize API with v19.0 (current version)
    try:
        FacebookAdsApi.init(app_id, app_secret, access_token, api_version='v19.0')
        print("✓ API initialized successfully (v19.0)")
        print()
    except Exception as e:
        print(f"✗ API initialization failed: {e}")
        exit(1)

    # Test each account
    print("Testing Account Access:")
    print("-" * 80)

    successful = 0
    failed = 0

    for idx, account_id in enumerate(account_ids, 1):
        print(f"\n[{idx}/{len(account_ids)}] Testing account: {account_id}")
        print("-" * 40)

        try:
            # Format account ID correctly (act_XXXXX)
            if not account_id.startswith('act_'):
                formatted_id = f'act_{account_id}'
            else:
                formatted_id = account_id

            account = AdAccount(formatted_id)

            # Try to fetch account info
            account_data = account.api_get(fields=[
                'name',
                'account_status',
                'currency',
                'timezone_name',
                'business_name'
            ])

            print(f"✓ SUCCESS")
            print(f"  Name: {account_data.get('name', 'N/A')}")
            print(f"  Business: {account_data.get('business_name', 'N/A')}")
            print(f"  Status: {account_data.get('account_status', 'N/A')}")
            print(f"  Currency: {account_data.get('currency', 'N/A')}")
            print(f"  Timezone: {account_data.get('timezone_name', 'N/A')}")

            # Try to get campaigns count
            try:
                campaigns = account.get_campaigns(fields=['name'], params={'limit': 10})
                campaign_list = list(campaigns)
                print(f"  Campaigns: {len(campaign_list)} found")
            except Exception as e:
                print(f"  Campaigns: Unable to fetch ({str(e)[:50]}...)")

            successful += 1

        except Exception as e:
            print(f"✗ FAILED")
            print(f"  Error Type: {type(e).__name__}")
            print(f"  Error Message: {str(e)}")
            failed += 1

    # Summary
    print()
    print("=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Total accounts tested: {len(account_ids)}")
    print(f"✓ Successful: {successful}")
    print(f"✗ Failed: {failed}")
    print()

    if failed == 0:
        print("🎉 ALL TESTS PASSED! Token is working correctly.")
        return 0
    else:
        print("⚠️  Some accounts failed. Check errors above.")
        return 1

if __name__ == "__main__":
    exit_code = test_facebook_token()
    exit(exit_code)
