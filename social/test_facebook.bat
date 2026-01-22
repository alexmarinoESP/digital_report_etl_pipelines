@echo off
REM Test Facebook Ads Platform - Writes to TEST tables
REM Facebook Ads adapter NOW FULLY IMPLEMENTED with real API data

echo ================================================================================
echo SOCIAL PIPELINE - FACEBOOK ADS TEST
echo ================================================================================
echo.
echo Facebook Ads adapter is now FULLY IMPLEMENTED!
echo This test will write REAL DATA from Facebook Marketing API to test tables.
echo.
echo Tables that will be written to (with REAL data from Facebook API):
echo   - fb_ads_campaign_TEST
echo   - fb_ads_ad_set_TEST
echo   - fb_ads_audience_adset_TEST
echo   - fb_ads_custom_conversion_TEST
echo   - fb_ads_insight_TEST
echo   - fb_ads_insight_actions_TEST
echo.
echo Total: 6 tables with REAL Facebook Ads data
echo.

REM Activate virtual environment if exists
if exist .venv\Scripts\activate.bat (
    echo Activating virtual environment...
    call .venv\Scripts\activate.bat
)

echo Running Facebook Ads pipeline in TEST mode...
echo.

REM Run the test
python -m social.test_pipeline --platform facebook --verbose

echo.
echo ================================================================================
echo Test completed! Check the output above for results.
echo All Facebook Ads TEST tables should now contain REAL data from the API.
echo ================================================================================
pause
