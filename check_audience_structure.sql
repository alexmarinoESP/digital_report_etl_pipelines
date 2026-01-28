SELECT column_name, data_type, is_nullable 
FROM columns 
WHERE table_schema = 'GoogleAnalytics' 
AND table_name = 'fb_ads_audience_adset'
ORDER BY ordinal_position;

SELECT * FROM GoogleAnalytics.fb_ads_audience_adset LIMIT 5;
