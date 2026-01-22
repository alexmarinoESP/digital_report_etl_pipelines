@echo off
REM Test Google Ads Platform - Writes to TEST tables
REM Google Ads adapter NOW FULLY IMPLEMENTED with real API data

echo ================================================================================
echo SOCIAL PIPELINE - GOOGLE ADS TEST
echo ================================================================================
echo.
echo Google Ads adapter is now FULLY IMPLEMENTED!
echo This test will write REAL DATA from Google Ads API to test tables.
echo.
echo Tables that will be written to (with REAL data from Google Ads API):
echo   - google_ads_account_TEST
echo   - google_ads_campaign_TEST
echo   - google_ads_ad_creatives_TEST
echo   - google_ads_cost_by_device_TEST
echo   - google_ads_placement_TEST
echo   - google_ads_audience_TEST
echo   - google_ads_report_TEST
echo   - google_ads_violation_TEST
echo.
echo Total: 8 tables with REAL Google Ads data
echo.

REM Activate virtual environment if exists
if exist .venv\Scripts\activate.bat (
    echo Activating virtual environment...
    call .venv\Scripts\activate.bat
)

echo Running Google Ads pipeline in TEST mode...
echo.

REM Run the test
python -m social.test_pipeline --platform google --verbose

echo.
echo ================================================================================
echo Test completed! Check the output above for results.
echo All Google Ads TEST tables should now contain REAL data from the API.
echo ================================================================================
pause
