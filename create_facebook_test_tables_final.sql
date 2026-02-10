-- ============================================================================
-- Script to recreate Facebook _TEST tables with new schema
--
-- Connection: vertica13.esprinet.com:5433 - Database: Esprinet
--
-- Changes:
-- - Removes: row_loaded_date (TIMESTAMP), last_updated_date (TIMESTAMP)
-- - Adds: load_date (DATE)
-- - Copies all data from production tables to TEST tables
-- ============================================================================

-- Step 1: Drop old TEST tables
-- ============================================================================
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_campaign_TEST CASCADE;
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_ad_set_TEST CASCADE;
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_insight_TEST CASCADE;
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_insight_actions_TEST CASCADE;
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_audience_adset_TEST CASCADE;
DROP TABLE IF EXISTS GoogleAnalytics.fb_ads_custom_conversion_TEST CASCADE;

-- Step 2: Create new TEST tables with updated schema (matching production)
-- ============================================================================

-- fb_ads_campaign_TEST
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

-- fb_ads_ad_set_TEST
CREATE TABLE GoogleAnalytics.fb_ads_ad_set_TEST (
    id VARCHAR(400) NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    destination_type VARCHAR(300),
    campaign_id VARCHAR(400),
    load_date DATE,
    PRIMARY KEY (id)
);

-- fb_ads_insight_TEST
CREATE TABLE GoogleAnalytics.fb_ads_insight_TEST (
    campaign_id VARCHAR(400),
    adset_id VARCHAR(400),
    ad_id VARCHAR(400) NOT NULL,
    account_id VARCHAR(400),
    ad_name VARCHAR(400),
    spend FLOAT,
    impressions INTEGER,
    reach INTEGER,
    inline_link_clicks INTEGER,
    inline_link_click_ctr FLOAT,
    clicks INTEGER,
    ctr FLOAT,
    cpc FLOAT,
    cpm FLOAT,
    load_date DATE,
    PRIMARY KEY (ad_id)
);

-- fb_ads_insight_actions_TEST (added action_target_id column)
CREATE TABLE GoogleAnalytics.fb_ads_insight_actions_TEST (
    ad_id VARCHAR(400) NOT NULL,
    action_type VARCHAR(600) NOT NULL,
    conversion_id VARCHAR(750),
    action_target_id VARCHAR(150),
    value INTEGER,
    load_date DATE,
    PRIMARY KEY (ad_id, action_type)
);

-- fb_ads_audience_adset_TEST
CREATE TABLE GoogleAnalytics.fb_ads_audience_adset_TEST (
    campaign_id VARCHAR(300),
    adset_id VARCHAR(300) NOT NULL,
    audience_id VARCHAR(300) NOT NULL,
    name VARCHAR(800),
    load_date DATE,
    PRIMARY KEY (audience_id, adset_id)
);

-- fb_ads_custom_conversion_TEST
CREATE TABLE GoogleAnalytics.fb_ads_custom_conversion_TEST (
    conversion_id VARCHAR(750) NOT NULL,
    custom_event_type VARCHAR(400),
    rule VARCHAR(1000),
    pixel_rule VARCHAR(1000),
    load_date DATE,
    PRIMARY KEY (conversion_id)
);

-- Step 3: Copy data from production tables to TEST tables
-- Priority: last_updated_date -> row_loaded_date -> CURRENT_DATE (where columns exist)
-- ============================================================================

-- Copy fb_ads_campaign (has both date columns)
INSERT INTO GoogleAnalytics.fb_ads_campaign_TEST
SELECT
    campaign_id,
    status,
    configured_status,
    effective_status,
    created_time,
    objective,
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_campaign;

-- Copy fb_ads_ad_set (has only last_updated_date - column order matches source)
INSERT INTO GoogleAnalytics.fb_ads_ad_set_TEST
SELECT
    id,
    start_time,
    end_time,
    destination_type,
    campaign_id,
    COALESCE(last_updated_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_ad_set;

-- Copy fb_ads_insight (has both date columns, filter NaN values)
INSERT INTO GoogleAnalytics.fb_ads_insight_TEST
SELECT
    campaign_id,
    adset_id,
    ad_id,
    account_id,
    ad_name,
    CASE WHEN spend::VARCHAR = 'NaN' THEN NULL ELSE spend END,
    impressions,
    reach,
    inline_link_clicks,
    CASE WHEN inline_link_click_ctr::VARCHAR = 'NaN' THEN NULL ELSE inline_link_click_ctr END,
    clicks,
    CASE WHEN ctr::VARCHAR = 'NaN' THEN NULL ELSE ctr END,
    CASE WHEN cpc::VARCHAR = 'NaN' THEN NULL ELSE cpc END,
    CASE WHEN cpm::VARCHAR = 'NaN' THEN NULL ELSE cpm END,
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_insight;

-- Copy fb_ads_insight_actions (has both date columns, includes action_target_id)
INSERT INTO GoogleAnalytics.fb_ads_insight_actions_TEST
SELECT
    ad_id,
    action_type,
    conversion_id,
    action_target_id,
    value,
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_insight_actions;

-- Copy fb_ads_audience_adset (has only row_loaded_date)
INSERT INTO GoogleAnalytics.fb_ads_audience_adset_TEST
SELECT
    campaign_id,
    adset_id,
    audience_id,
    name,
    COALESCE(row_loaded_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_audience_adset;

-- Copy fb_ads_custom_conversion (has no date columns)
INSERT INTO GoogleAnalytics.fb_ads_custom_conversion_TEST
SELECT
    conversion_id,
    custom_event_type,
    rule,
    pixel_rule,
    CURRENT_DATE AS load_date
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

-- Done!
COMMIT;
