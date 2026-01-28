#!/usr/bin/env python
"""Debug script to inspect raw API response from LinkedIn."""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from social.core.config import ConfigurationManager
from social.infrastructure.database import VerticaDataSink
from social.infrastructure.database_token_provider import DatabaseTokenProvider
from social.infrastructure.file_token_provider import FileBasedTokenProvider
from social.platforms.linkedin.http_client import LinkedInHTTPClient

def main():
    # Load config
    config_manager = ConfigurationManager()
    config = config_manager.load_config(test_mode=True)

    # Create data sink for token provider
    data_sink = VerticaDataSink(config=config.database, test_mode=True)

    # Get token
    file_provider = FileBasedTokenProvider("linkedin")
    db_provider = DatabaseTokenProvider("linkedin", data_sink)
    access_token = db_provider.get_access_token()

    # Create HTTP client
    http_client = LinkedInHTTPClient(access_token=access_token)

    # Fetch campaigns for account 511420282 (where campaign 467847723 is)
    account_id = "511420282"
    url = f"https://api.linkedin.com/rest/adAccounts/{account_id}/adCampaigns"

    params = {"q": "search"}
    no_encoded_params = {"search": "(status:(values:List(ACTIVE,PAUSED,COMPLETED,ARCHIVED)))"}

    print(f"Fetching campaigns from {url}...")
    response = http_client.get(url=url, params=params, no_encoded_params=no_encoded_params)

    campaigns = response.get("elements", [])
    print(f"Found {len(campaigns)} campaigns")

    # Find campaign 467847723
    target_id = 467847723
    for campaign in campaigns:
        campaign_id = campaign.get("id", "")
        if str(target_id) in str(campaign_id):
            print(f"\n{'='*60}")
            print(f"FOUND CAMPAIGN {target_id}")
            print(f"{'='*60}")
            print(json.dumps(campaign, indent=2, default=str))
            print(f"\n{'='*60}")
            print("runSchedule field:")
            print(json.dumps(campaign.get("runSchedule", "NOT FOUND"), indent=2, default=str))
            break
    else:
        print(f"Campaign {target_id} not found!")
        # Print first campaign to see structure
        if campaigns:
            print("\nFirst campaign structure:")
            print(json.dumps(campaigns[0], indent=2, default=str))

if __name__ == "__main__":
    main()
