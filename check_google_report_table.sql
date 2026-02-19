-- Check google_ads_report_TEST table structure
SELECT column_name, data_type, is_nullable
FROM columns
WHERE table_schema = 'GoogleAnalytics'
AND table_name = 'google_ads_report_TEST'
ORDER BY ordinal_position;

-- Check if table exists
SELECT table_name
FROM tables
WHERE table_schema = 'GoogleAnalytics'
AND table_name LIKE 'google_ads_report%';

-- Check sample data from production
SELECT TOP 5
    date,
    campaign_id,
    adgroup_id,
    ad_id,
    clicks,
    impressions,
    conversions,
    cost_micros,
    average_cpm,
    average_cpc,
    average_cost,
    ctr
FROM GoogleAnalytics.google_ads_report
ORDER BY date DESC;
