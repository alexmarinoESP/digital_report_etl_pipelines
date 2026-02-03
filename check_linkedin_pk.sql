-- Check PRIMARY KEY for linkedin_ads_insights
SELECT constraint_name, column_name, ordinal_position
FROM v_catalog.primary_keys
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'linkedin_ads_insights'
ORDER BY ordinal_position;

-- Check if date column exists
SELECT column_name, data_type
FROM v_catalog.columns  
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'linkedin_ads_insights'
  AND column_name LIKE '%date%';
