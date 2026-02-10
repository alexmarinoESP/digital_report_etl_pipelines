-- ============================================================================
-- Script to recreate Facebook _TEST tables with new schema
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

-- Step 2: Create new TEST tables with updated schema
-- ============================================================================

-- fb_ads_campaign_TEST
CREATE TABLE GoogleAnalytics.fb_ads_campaign_TEST (
    campaign_id VARCHAR(255) NOT NULL,
    status VARCHAR(50),
    configured_status VARCHAR(50),
    effective_status VARCHAR(50),
    created_time TIMESTAMP,
    objective VARCHAR(100),
    load_date DATE,
    PRIMARY KEY (campaign_id)
);

-- fb_ads_ad_set_TEST
CREATE TABLE GoogleAnalytics.fb_ads_ad_set_TEST (
    id VARCHAR(255) NOT NULL,
    campaign_id VARCHAR(255),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    destination_type VARCHAR(100),
    load_date DATE,
    PRIMARY KEY (id)
);

-- fb_ads_insight_TEST
CREATE TABLE GoogleAnalytics.fb_ads_insight_TEST (
    ad_id VARCHAR(255) NOT NULL,
    account_id VARCHAR(255),
    campaign_id VARCHAR(255),
    adset_id VARCHAR(255),
    ad_name VARCHAR(500),
    spend DECIMAL(18,2),
    impressions INTEGER,
    reach INTEGER,
    inline_link_clicks INTEGER,
    inline_link_click_ctr DECIMAL(10,6),
    clicks INTEGER,
    ctr DECIMAL(10,6),
    cpc DECIMAL(18,6),
    cpm DECIMAL(18,6),
    load_date DATE,
    PRIMARY KEY (ad_id)
);

-- fb_ads_insight_actions_TEST
CREATE TABLE GoogleAnalytics.fb_ads_insight_actions_TEST (
    ad_id VARCHAR(255) NOT NULL,
    action_type VARCHAR(255) NOT NULL,
    value DECIMAL(18,2),
    conversion_id VARCHAR(255),
    load_date DATE,
    PRIMARY KEY (ad_id, action_type)
);

-- fb_ads_audience_adset_TEST
CREATE TABLE GoogleAnalytics.fb_ads_audience_adset_TEST (
    audience_id VARCHAR(255) NOT NULL,
    adset_id VARCHAR(255) NOT NULL,
    campaign_id VARCHAR(255),
    name VARCHAR(500),
    load_date DATE,
    PRIMARY KEY (audience_id, adset_id)
);

-- fb_ads_custom_conversion_TEST
CREATE TABLE GoogleAnalytics.fb_ads_custom_conversion_TEST (
    conversion_id VARCHAR(255) NOT NULL,
    custom_event_type VARCHAR(100),
    rule LONG VARCHAR,
    pixel_id VARCHAR(255),
    event_type VARCHAR(255),
    load_date DATE,
    PRIMARY KEY (conversion_id)
);

-- Step 3: Copy data from production tables to TEST tables
-- Priority: last_updated_date -> row_loaded_date -> CURRENT_DATE
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
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_campaign;

-- Copy fb_ads_ad_set
INSERT INTO GoogleAnalytics.fb_ads_ad_set_TEST
SELECT
    id,
    campaign_id,
    start_time,
    end_time,
    destination_type,
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_ad_set;

-- Copy fb_ads_insight
INSERT INTO GoogleAnalytics.fb_ads_insight_TEST
SELECT
    ad_id,
    account_id,
    campaign_id,
    adset_id,
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
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_insight;

-- Copy fb_ads_insight_actions
INSERT INTO GoogleAnalytics.fb_ads_insight_actions_TEST
SELECT
    ad_id,
    action_type,
    value,
    conversion_id,
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_insight_actions;

-- Copy fb_ads_audience_adset
INSERT INTO GoogleAnalytics.fb_ads_audience_adset_TEST
SELECT
    audience_id,
    adset_id,
    campaign_id,
    name,
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE) AS load_date
FROM GoogleAnalytics.fb_ads_audience_adset;

-- Copy fb_ads_custom_conversion
INSERT INTO GoogleAnalytics.fb_ads_custom_conversion_TEST
SELECT
    conversion_id,
    custom_event_type,
    rule,
    pixel_id,
    event_type,
    COALESCE(last_updated_date::DATE, row_loaded_date::DATE, CURRENT_DATE) AS load_date
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
