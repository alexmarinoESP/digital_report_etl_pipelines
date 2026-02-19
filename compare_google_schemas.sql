-- =============================================================================
-- Script per confrontare gli schemi delle tabelle Google Ads PROD vs TEST
-- Eseguire questo script su Vertica tramite DBeaver o altro client SQL
-- =============================================================================

-- 1. Lista di tutte le tabelle google_ads disponibili
SELECT '===== TABELLE DISPONIBILI =====' AS info;
SELECT table_name
FROM tables
WHERE table_schema = 'GoogleAnalytics'
  AND table_name LIKE 'google_ads_%'
ORDER BY table_name;

-- =============================================================================
-- 2. GOOGLE_ADS_REPORT: PROD vs TEST
-- =============================================================================
SELECT '===== GOOGLE_ADS_REPORT (PROD) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_report'
ORDER BY ordinal_position;

SELECT '===== GOOGLE_ADS_REPORT_TEST (TEST) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_report_TEST'
ORDER BY ordinal_position;

-- =============================================================================
-- 3. GOOGLE_ADS_CAMPAIGN: PROD vs TEST
-- =============================================================================
SELECT '===== GOOGLE_ADS_CAMPAIGN (PROD) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_campaign'
ORDER BY ordinal_position;

SELECT '===== GOOGLE_ADS_CAMPAIGN_TEST (TEST) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_campaign_TEST'
ORDER BY ordinal_position;

-- =============================================================================
-- 4. GOOGLE_ADS_PLACEMENT: PROD vs TEST
-- =============================================================================
SELECT '===== GOOGLE_ADS_PLACEMENT (PROD) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_placement'
ORDER BY ordinal_position;

SELECT '===== GOOGLE_ADS_PLACEMENT_TEST (TEST) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_placement_TEST'
ORDER BY ordinal_position;

-- =============================================================================
-- 5. GOOGLE_ADS_AD_CREATIVES: PROD vs TEST
-- =============================================================================
SELECT '===== GOOGLE_ADS_AD_CREATIVES (PROD) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_ad_creatives'
ORDER BY ordinal_position;

SELECT '===== GOOGLE_ADS_AD_CREATIVES_TEST (TEST) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_ad_creatives_TEST'
ORDER BY ordinal_position;

-- =============================================================================
-- 6. GOOGLE_ADS_AUDIENCE: PROD vs TEST
-- =============================================================================
SELECT '===== GOOGLE_ADS_AUDIENCE (PROD) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_audience'
ORDER BY ordinal_position;

SELECT '===== GOOGLE_ADS_AUDIENCE_TEST (TEST) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_audience_TEST'
ORDER BY ordinal_position;

-- =============================================================================
-- 7. GOOGLE_ADS_COST_BY_DEVICE: PROD vs TEST
-- =============================================================================
SELECT '===== GOOGLE_ADS_COST_BY_DEVICE (PROD) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_cost_by_device'
ORDER BY ordinal_position;

SELECT '===== GOOGLE_ADS_COST_BY_DEVICE_TEST (TEST) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_cost_by_device_TEST'
ORDER BY ordinal_position;

-- =============================================================================
-- 8. GOOGLE_ADS_ACCOUNT: PROD vs TEST
-- =============================================================================
SELECT '===== GOOGLE_ADS_ACCOUNT (PROD) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_account'
ORDER BY ordinal_position;

SELECT '===== GOOGLE_ADS_ACCOUNT_TEST (TEST) =====' AS info;
SELECT column_name, data_type, is_nullable, column_default
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name = 'google_ads_account_TEST'
ORDER BY ordinal_position;

-- =============================================================================
-- 9. Query di riepilogo: conta colonne per tabella
-- =============================================================================
SELECT '===== RIEPILOGO CONTEGGIO COLONNE =====' AS info;
SELECT
    table_name,
    COUNT(*) AS num_columns
FROM columns
WHERE table_schema = 'GoogleAnalytics'
  AND table_name LIKE 'google_ads_%'
GROUP BY table_name
ORDER BY table_name;

-- =============================================================================
-- 10. Query per trovare differenze automaticamente
-- =============================================================================
SELECT '===== COLONNE DIVERSE TRA PROD E TEST =====' AS info;

WITH prod_cols AS (
    SELECT
        REPLACE(table_name, '_TEST', '') AS base_table,
        column_name,
        data_type,
        is_nullable,
        ordinal_position
    FROM columns
    WHERE table_schema = 'GoogleAnalytics'
      AND table_name LIKE 'google_ads_%'
      AND table_name NOT LIKE '%_TEST'
),
test_cols AS (
    SELECT
        REPLACE(table_name, '_TEST', '') AS base_table,
        column_name,
        data_type,
        is_nullable,
        ordinal_position
    FROM columns
    WHERE table_schema = 'GoogleAnalytics'
      AND table_name LIKE 'google_ads_%_TEST'
)
SELECT
    COALESCE(p.base_table, t.base_table) AS table_name,
    COALESCE(p.column_name, t.column_name) AS column_name,
    CASE
        WHEN p.column_name IS NULL THEN 'Solo in TEST'
        WHEN t.column_name IS NULL THEN 'Solo in PROD'
        WHEN p.data_type != t.data_type THEN 'Tipo diverso'
        ELSE 'OK'
    END AS status,
    p.data_type AS prod_type,
    t.data_type AS test_type
FROM prod_cols p
FULL OUTER JOIN test_cols t
    ON p.base_table = t.base_table
    AND p.column_name = t.column_name
WHERE p.column_name IS NULL
   OR t.column_name IS NULL
   OR p.data_type != t.data_type
ORDER BY table_name, column_name;
