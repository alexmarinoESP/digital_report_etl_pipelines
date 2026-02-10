-- ============================================================================
-- Copy and CLEAN data from production Facebook tables to TEST tables
-- Handles type mismatches and corrupt data in production tables
-- ============================================================================

-- Copy fb_ads_campaign
INSERT INTO GoogleAnalytics.fb_ads_campaign_TEST
SELECT
    campaign_id,
    status,
    configured_status,
    effective_status,
    created_time,
    objective,
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE)
FROM GoogleAnalytics.fb_ads_campaign;

-- Copy fb_ads_ad_set (cast end_time explicitly, skip if invalid)
INSERT INTO GoogleAnalytics.fb_ads_ad_set_TEST
SELECT
    id,
    start_time,
    TRY_CAST(end_time AS TIMESTAMP),
    destination_type,
    campaign_id,
    COALESCE(last_updated_date::DATE, CURRENT_DATE)
FROM GoogleAnalytics.fb_ads_ad_set;

-- Copy fb_ads_insight (skip rows with NaN values)
INSERT INTO GoogleAnalytics.fb_ads_insight_TEST
SELECT
    campaign_id,
    adset_id,
    ad_id,
    account_id,
    ad_name,
    spend,
    impressions,
    reach,
    inline_link_clicks,
    inline_link_click_ctr,
    clicks,
    ctr,
    cpc,
    cpm,
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE)
FROM GoogleAnalytics.fb_ads_insight
WHERE (inline_link_click_ctr IS NULL OR inline_link_click_ctr::VARCHAR != 'NaN')
  AND (ctr IS NULL OR ctr::VARCHAR != 'NaN')
  AND (cpc IS NULL OR cpc::VARCHAR != 'NaN')
  AND (cpm IS NULL OR cpm::VARCHAR != 'NaN')
  AND (spend IS NULL OR spend::VARCHAR != 'NaN');

-- Copy fb_ads_insight_actions (cast value explicitly, skip if invalid)
INSERT INTO GoogleAnalytics.fb_ads_insight_actions_TEST
SELECT
    ad_id,
    action_type,
    conversion_id,
    action_target_id,
    TRY_CAST(value AS INTEGER),
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE)
FROM GoogleAnalytics.fb_ads_insight_actions;

-- Copy fb_ads_audience_adset
INSERT INTO GoogleAnalytics.fb_ads_audience_adset_TEST
SELECT
    campaign_id,
    adset_id,
    audience_id,
    name,
    COALESCE(row_loaded_date::DATE, CURRENT_DATE)
FROM GoogleAnalytics.fb_ads_audience_adset;

-- Copy fb_ads_custom_conversion
INSERT INTO GoogleAnalytics.fb_ads_custom_conversion_TEST
SELECT
    conversion_id,
    custom_event_type,
    rule,
    pixel_rule,
    CURRENT_DATE
FROM GoogleAnalytics.fb_ads_custom_conversion;

-- Verify row counts
SELECT 'fb_ads_campaign_TEST' AS table_name, COUNT(*) AS row_count FROM GoogleAnalytics.fb_ads_campaign_TEST
UNION ALL
SELECT 'fb_ads_ad_set_TEST', COUNT(*) FROM GoogleAnalytics.fb_ads_ad_set_TEST
UNION ALL
SELECT 'fb_ads_insight_TEST', COUNT(*) FROM GoogleAnalytics.fb_ads_insight_TEST
UNION ALL
SELECT 'fb_ads_insight_actions_TEST', COUNT(*) FROM GoogleAnalytics.fb_ads_insight_actions_TEST
UNION ALL
SELECT 'fb_ads_audience_adset_TEST', COUNT(*) FROM GoogleAnalytics.fb_ads_audience_adset_TEST
UNION ALL
SELECT 'fb_ads_custom_conversion_TEST', COUNT(*) FROM GoogleAnalytics.fb_ads_custom_conversion_TEST;

COMMIT;
