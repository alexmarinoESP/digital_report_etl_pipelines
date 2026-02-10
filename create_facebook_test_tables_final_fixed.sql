-- Copy fb_ads_ad_set (fix end_time type conversion)
INSERT INTO GoogleAnalytics.fb_ads_ad_set_TEST
SELECT
    id,
    start_time,
    CASE WHEN end_time::VARCHAR IN ('', 'null') THEN NULL ELSE end_time::TIMESTAMP END,
    destination_type,
    campaign_id,
    COALESCE(last_updated_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_ad_set;

-- Copy fb_ads_insight_actions (fix value type conversion)
INSERT INTO GoogleAnalytics.fb_ads_insight_actions_TEST
SELECT
    ad_id,
    action_type,
    conversion_id,
    action_target_id,
    CASE WHEN value::VARCHAR IN ('', 'null') THEN NULL ELSE value::INTEGER END,
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_insight_actions;
