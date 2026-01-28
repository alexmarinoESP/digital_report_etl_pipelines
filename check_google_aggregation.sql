-- Controlla i dati caricati
SELECT 
    ad_id,
    campaign_id,
    clicks,
    impressions,
    conversions,
    costmicros,
    load_date
FROM GoogleAnalytics.google_ads_report_TEST
WHERE ad_id = '443808797429'
LIMIT 10;
