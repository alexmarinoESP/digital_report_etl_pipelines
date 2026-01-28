#!/usr/bin/env python
"""Debug script to inspect targetingCriteria structure from LinkedIn API."""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from social.core.config import ConfigurationManager
from social.infrastructure.database import VerticaDataSink
from social.infrastructure.database_token_provider import DatabaseTokenProvider
from social.platforms.linkedin.http_client import LinkedInHTTPClient

def main():
    # Load config
    config_manager = ConfigurationManager()
    config = config_manager.load_config(test_mode=True)

    # Create data sink for token provider
    data_sink = VerticaDataSink(config=config.database, test_mode=True)

    # Get token
    db_provider = DatabaseTokenProvider("linkedin", data_sink)
    access_token = db_provider.get_access_token()

    # Create HTTP client
    http_client = LinkedInHTTPClient(access_token=access_token)

    # Fetch campaigns for all accounts (from COMPANY_ACCOUNT_MAP)
    account_ids = ["503427986", "510686676", "512866551", "512065861", "506509802", "506522380", "511420282", "511422249"]

    target_campaign_id = "260964364"  # The campaign with known audience_id

    for account_id in account_ids:
        print(f"\n{'='*60}")
        print(f"Fetching campaigns from account {account_id}...")

        url = f"https://api.linkedin.com/rest/adAccounts/{account_id}/adCampaigns"
        params = {"q": "search"}
        no_encoded_params = {"search": "(status:(values:List(ACTIVE,PAUSED,COMPLETED,ARCHIVED)))"}

        try:
            response = http_client.get(url=url, params=params, no_encoded_params=no_encoded_params)
            campaigns = response.get("elements", [])
            print(f"Found {len(campaigns)} campaigns")

            # Find target campaign
            for campaign in campaigns:
                campaign_id = str(campaign.get("id", ""))
                if campaign_id == target_campaign_id:
                    print(f"\n{'='*60}")
                    print(f"FOUND TARGET CAMPAIGN {target_campaign_id}")
                    print(f"{'='*60}")

                    # Print full targetingCriteria
                    targeting = campaign.get("targetingCriteria", {})
                    print(f"\ntargetingCriteria:")
                    print(json.dumps(targeting, indent=2, default=str))

                    # Try to extract audience
                    include = targeting.get("include", {})
                    and_list = include.get("and", [])
                    print(f"\ninclude.and has {len(and_list)} items")

                    for i, item in enumerate(and_list):
                        print(f"\nItem {i}: {json.dumps(item, indent=2, default=str)[:500]}")

                    return

            # If not found, print first campaign's targeting structure
            if campaigns:
                first = campaigns[0]
                print(f"\nFirst campaign ID: {first.get('id')}")
                print(f"targetingCriteria keys: {first.get('targetingCriteria', {}).keys()}")

        except Exception as e:
            print(f"Error fetching from account {account_id}: {e}")

    print(f"\nCampaign {target_campaign_id} not found in any account!")

if __name__ == "__main__":
    main()
