"""LinkedIn Ads Platform Constants.

This module defines all constants specific to the LinkedIn Ads platform,
including API configuration, company mappings, and lookback periods.

Constants:
- API_BASE_URL: Base URL for LinkedIn Marketing API
- INSIGHTS_LOOKBACK_DAYS: Number of days to look back for insights data
- COMPANY_ACCOUNT_MAP: Mapping of LinkedIn account IDs to company IDs
"""

from typing import Dict, Final


# API Configuration
API_BASE_URL: Final[str] = "https://api.linkedin.com/rest"

# Data extraction configuration
INSIGHTS_LOOKBACK_DAYS: Final[int] = 730  # 2 years of historical data

# Company-to-Account Mapping
# Maps LinkedIn account IDs (string) to internal company IDs (integer)
# Default company ID is 1 for unmapped accounts
COMPANY_ACCOUNT_MAP: Final[Dict[str, int]] = {
    "503427986": 1,
    "510686676": 1,
    "512866551": 30,  # Zeliatech
    "512065861": 23,  # V-valley PT
    "506509802": 32,  # V-Valley IT
    "506522380": 19,  # DACOM
    "511420282": 2,
    "511422249": 20,  # V-valley ES
}
