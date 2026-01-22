#!/bin/bash
# Test LinkedIn Ads Pipeline - Writes to TEST tables
# This script tests the complete LinkedIn implementation

echo "================================================================================"
echo "SOCIAL PIPELINE - LINKEDIN TEST"
echo "================================================================================"
echo ""

# Activate virtual environment if exists
if [ -f .venv/bin/activate ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

echo "Running LinkedIn pipeline in TEST mode..."
echo ""
echo "Tables will be written to:"
echo "  - linkedin_ads_account_TEST"
echo "  - linkedin_ads_campaign_TEST"
echo "  - linkedin_ads_audience_TEST"
echo "  - linkedin_ads_campaign_audience_TEST"
echo "  - linkedin_ads_insights_TEST"
echo "  - linkedin_ads_creative_TEST"
echo ""

# Run the test
python -m social.test_pipeline --platform linkedin --verbose

echo ""
echo "================================================================================"
echo "Test completed! Check the output above for results."
echo "================================================================================"
