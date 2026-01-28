-- Check if data is aggregated for ad_id 791614485563
SELECT 
    ad_id,
    adgroup_id,
    campaign_id,
    date,
    clicks,
    impressions,
    cost_micros,
    load_date
FROM GoogleAnalytics.google_ads_report_TEST
WHERE ad_id = '791614485563'
ORDER BY date DESC
LIMIT 50;

-- Count how many rows for this ad_id
SELECT 
    COUNT(*) as row_count,
    COUNT(DISTINCT date) as distinct_dates,
    SUM(clicks) as total_clicks,
    SUM(impressions) as total_impressions
FROM GoogleAnalytics.google_ads_report_TEST
WHERE ad_id = '791614485563';
