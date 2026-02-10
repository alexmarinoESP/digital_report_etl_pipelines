-- ============================================================================
-- Recreate Facebook TEST tables with EXACT types from production
-- Then copy data without any conversion
-- ============================================================================

-- Step 1: Drop existing TEST tables
-- ============================================================================
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_campaign_TEST CASCADE;
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_ad_set_TEST CASCADE;
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_insight_TEST CASCADE;
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_insight_actions_TEST CASCADE;
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_audience_adset_TEST CASCADE;
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_custom_conversion_TEST CASCADE;

-- Step 2: Create TEST tables with EXACT types from production
-- ============================================================================

-- fb_ads_campaign_TEST (exact copy of production schema + load_date)
CREATE TABLE GoogleAnalytics.fb_ads_campaign_TEST (
    campaign_id VARCHAR(400) NOT NULL,
    status VARCHAR(400),
    configured_status VARCHAR(400),
    effective_status VARCHAR(400),
    created_time TIMESTAMP,
    objective VARCHAR(400),
    load_date DATE,
    PRIMARY KEY (campaign_id)
);

-- fb_ads_ad_set_TEST (exact copy of production schema + load_date)
CREATE TABLE GoogleAnalytics.fb_ads_ad_set_TEST (
    id VARCHAR(400) NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    destination_type VARCHAR(300),
    campaign_id VARCHAR(400),
    load_date DATE,
    PRIMARY KEY (id)
);

-- fb_ads_insight_TEST (exact copy of production schema + load_date)
CREATE TABLE GoogleAnalytics.fb_ads_insight_TEST (
    campaign_id VARCHAR(400),
    adset_id VARCHAR(400),
    ad_id VARCHAR(400) NOT NULL,
    account_id VARCHAR(400),
    ad_name VARCHAR(400),
    spend FLOAT,
    impressions INT,
    reach INT,
    inline_link_clicks INT,
    inline_link_click_ctr FLOAT,
    clicks INT,
    ctr FLOAT,
    cpc FLOAT,
    cpm FLOAT,
    load_date DATE,
    PRIMARY KEY (ad_id)
);

-- fb_ads_insight_actions_TEST (exact copy of production schema + load_date)
CREATE TABLE GoogleAnalytics.fb_ads_insight_actions_TEST (
    ad_id VARCHAR(400) NOT NULL,
    action_type VARCHAR(600) NOT NULL,
    conversion_id VARCHAR(750),
    action_target_id VARCHAR(150),
    value INT,
    load_date DATE,
    PRIMARY KEY (ad_id, action_type)
);

-- fb_ads_audience_adset_TEST (exact copy of production schema + load_date)
CREATE TABLE GoogleAnalytics.fb_ads_audience_adset_TEST (
    campaign_id VARCHAR(300),
    adset_id VARCHAR(300) NOT NULL,
    audience_id VARCHAR(300) NOT NULL,
    name VARCHAR(800),
    load_date DATE,
    PRIMARY KEY (audience_id, adset_id)
);

-- fb_ads_custom_conversion_TEST (exact copy of production schema + load_date)
CREATE TABLE GoogleAnalytics.fb_ads_custom_conversion_TEST (
    conversion_id VARCHAR(750) NOT NULL,
    custom_event_type VARCHAR(400),
    rule VARCHAR(1000),
    pixel_rule VARCHAR(1000),
    load_date DATE,
    PRIMARY KEY (conversion_id)
);

-- Step 3: Copy data WITHOUT any conversion (types match exactly)
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

-- Copy fb_ads_ad_set
INSERT INTO GoogleAnalytics.fb_ads_ad_set_TEST
SELECT
    id,
    start_time,
    end_time,
    destination_type,
    campaign_id,
    COALESCE(last_updated_date::DATE, CURRENT_DATE)
FROM GoogleAnalytics.fb_ads_ad_set;

-- Copy fb_ads_insight
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

-- Copy fb_ads_insight_actions
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

-- Step 4: Verify row counts
-- ============================================================================
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
