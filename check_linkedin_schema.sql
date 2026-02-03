-- Check LinkedIn insights table structure in PROD
SELECT column_name, data_type
FROM v_catalog.columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'linkedin_ads_insights'
ORDER BY ordinal_position;
