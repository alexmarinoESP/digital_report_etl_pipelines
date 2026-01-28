-- Check if source table exists and has correct schema
SELECT column_name, data_type, is_nullable
FROM v_catalog.columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_report_TEST_source'
ORDER BY ordinal_position;
