-- ============================================================================
-- Copy data from production Facebook tables to TEST tables
-- Run this AFTER creating the TEST tables
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

-- Copy fb_ads_ad_set (keep types as-is from source)
INSERT INTO GoogleAnalytics.fb_ads_ad_set_TEST
SELECT
    id,
    start_time,
    end_time,
    destination_type,
    campaign_id,
    COALESCE(last_updated_date::DATE, CURRENT_DATE)
FROM GoogleAnalytics.fb_ads_ad_set;

-- Copy fb_ads_insight (handle NaN in float columns)
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
FROM GoogleAnalytics.fb_ads_insight;

-- Copy fb_ads_insight_actions (keep types as-is from source)
INSERT INTO GoogleAnalytics.fb_ads_insight_actions_TEST
SELECT
    ad_id,
    action_type,
    conversion_id,
    action_target_id,
    value,
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

COMMIT;
