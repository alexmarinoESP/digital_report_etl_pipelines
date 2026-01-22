@echo off
REM Test ALL Platforms - Writes to TEST tables
REM ALL THREE platforms now fully implemented with REAL API data!

echo ================================================================================
echo SOCIAL PIPELINE - ALL PLATFORMS TEST
echo ================================================================================
echo.
echo This will test ALL THREE platforms with REAL API DATA:
echo   - LinkedIn Ads (REAL API DATA)
echo   - Google Ads (REAL API DATA)
echo   - Facebook Ads (REAL API DATA)
echo.

REM Activate virtual environment if exists
if exist .venv\Scripts\activate.bat (
    echo Activating virtual environment...
    call .venv\Scripts\activate.bat
)

echo Running ALL platforms in TEST mode...
echo.
echo LinkedIn tables (REAL DATA):
echo   - linkedin_ads_account_TEST
echo   - linkedin_ads_campaign_TEST
echo   - linkedin_ads_audience_TEST
echo   - linkedin_ads_campaign_audience_TEST
echo   - linkedin_ads_insights_TEST
echo   - linkedin_ads_creative_TEST
echo.
echo Google tables (REAL DATA):
echo   - google_ads_account_TEST
echo   - google_ads_campaign_TEST
echo   - google_ads_ad_creatives_TEST
echo   - google_ads_cost_by_device_TEST
echo   - google_ads_placement_TEST
echo   - google_ads_audience_TEST
echo   - google_ads_report_TEST
echo   - google_ads_violation_TEST
echo.
echo Facebook tables (REAL DATA):
echo   - fb_ads_campaign_TEST
echo   - fb_ads_ad_set_TEST
echo   - fb_ads_audience_adset_TEST
echo   - fb_ads_custom_conversion_TEST
echo   - fb_ads_insight_TEST
echo   - fb_ads_insight_actions_TEST
echo.
echo TOTAL: 20 TEST TABLES WITH REAL API DATA
echo.

REM Run the test
python -m social.test_pipeline --platform all --verbose

echo.
echo ================================================================================
echo Test completed! Check the output above for results.
echo All 20 TEST tables should now contain REAL data from their respective APIs.
echo ================================================================================
pause
