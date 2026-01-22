# Multi-Platform Testing Guide

## 🎯 Overview

Il Social Pipeline supporta **3 platform**:
- **LinkedIn Ads** ✅ Completamente implementato (dati REALI)
- **Google Ads** ⏳ Test mode con dati MOCK
- **Facebook Ads** ⏳ Test mode con dati MOCK

Tutti i test scrivono su tabelle con suffisso `_TEST`.

---

## 🚀 Quick Start - Test Tutte le Platform

### Windows

```batch
# Test TUTTE E TRE le platform insieme (20 tabelle TEST)
test_all_platforms.bat

# Oppure singolarmente
test_linkedin.bat    # LinkedIn - Dati REALI
test_google.bat      # Google - Dati MOCK
test_facebook.bat    # Facebook - Dati MOCK
```

### Python Direct

```bash
# Test tutte le platform
python -m social.test_pipeline --platform all --verbose

# Test singola platform
python -m social.test_pipeline --platform linkedin
python -m social.test_pipeline --platform google
python -m social.test_pipeline --platform facebook
```

---

## 📊 Tabelle di Destinazione

### LinkedIn Ads (6 Tabelle) - ✅ DATI REALI

```
linkedin_ads_account_TEST          (Account information)
linkedin_ads_campaign_TEST         (Campaigns)
linkedin_ads_audience_TEST         (Audiences/segments)
linkedin_ads_campaign_audience_TEST (Campaign-audience relationships)
linkedin_ads_insights_TEST         (Performance metrics)
linkedin_ads_creative_TEST         (Ad creatives)
```

### Google Ads (8 Tabelle) - ⏳ DATI MOCK

```
google_ads_account_TEST            (Account information - MOCK)
google_ads_campaign_TEST           (Campaigns - MOCK)
google_ads_ad_creatives_TEST       (Ad creatives - MOCK)
google_ads_cost_by_device_TEST     (Device costs - MOCK)
google_ads_placement_TEST          (Placements - MOCK)
google_ads_audience_TEST           (Audiences - MOCK)
google_ads_report_TEST             (Reports - MOCK)
google_ads_violation_TEST          (Policy violations - MOCK)
```

### Facebook Ads (6 Tabelle) - ⏳ DATI MOCK

```
fb_ads_campaign_TEST               (Campaigns - MOCK)
fb_ads_ad_set_TEST                 (Ad sets - MOCK)
fb_ads_audience_adset_TEST         (Audience-adset relationships - MOCK)
fb_ads_custom_conversion_TEST      (Custom conversions - MOCK)
fb_ads_insight_TEST                (Performance insights - MOCK)
fb_ads_insight_actions_TEST        (User actions - MOCK)
```

**TOTALE: 20 TABELLE TEST**

---

## 🧪 Comandi di Test

### Test Tutte le Platform

```bash
# Scrive su TUTTE le 20 tabelle TEST
python -m social.test_pipeline --platform all --verbose

# Expected output:
# LinkedIn: 6 tables with REAL data
# Google: 8 tables with MOCK data
# Facebook: 6 tables with MOCK data
```

### Test Singola Platform

```bash
# LinkedIn (dati REALI dall'API)
python -m social.test_pipeline --platform linkedin --verbose

# Google (dati MOCK)
python -m social.test_pipeline --platform google --verbose

# Facebook (dati MOCK)
python -m social.test_pipeline --platform facebook --verbose
```

### Test Tabelle Specifiche

```bash
# Solo alcune tabelle LinkedIn
python -m social.test_pipeline --platform linkedin \
    --tables linkedin_ads_campaign,linkedin_ads_insights

# Solo alcune tabelle Google
python -m social.test_pipeline --platform google \
    --tables google_ads_campaign,google_ads_report
```

### Dry Run (No Database Writes)

```bash
# Test senza scrivere nel database
python -m social.test_pipeline --platform all --dry-run --verbose
```

---

## 📋 Verificare i Risultati

### Contare Tutte le Tabelle TEST

```sql
-- Contare TUTTE le 20 tabelle TEST
SELECT
    table_name,
    COUNT(*) as row_count
FROM (
    -- LinkedIn (6 tabelle)
    SELECT 'linkedin_ads_account_TEST' as table_name FROM esp_digital_report.linkedin_ads_account_TEST
    UNION ALL SELECT 'linkedin_ads_campaign_TEST' FROM esp_digital_report.linkedin_ads_campaign_TEST
    UNION ALL SELECT 'linkedin_ads_audience_TEST' FROM esp_digital_report.linkedin_ads_audience_TEST
    UNION ALL SELECT 'linkedin_ads_campaign_audience_TEST' FROM esp_digital_report.linkedin_ads_campaign_audience_TEST
    UNION ALL SELECT 'linkedin_ads_insights_TEST' FROM esp_digital_report.linkedin_ads_insights_TEST
    UNION ALL SELECT 'linkedin_ads_creative_TEST' FROM esp_digital_report.linkedin_ads_creative_TEST

    -- Google (8 tabelle)
    UNION ALL SELECT 'google_ads_account_TEST' FROM esp_digital_report.google_ads_account_TEST
    UNION ALL SELECT 'google_ads_campaign_TEST' FROM esp_digital_report.google_ads_campaign_TEST
    UNION ALL SELECT 'google_ads_ad_creatives_TEST' FROM esp_digital_report.google_ads_ad_creatives_TEST
    UNION ALL SELECT 'google_ads_cost_by_device_TEST' FROM esp_digital_report.google_ads_cost_by_device_TEST
    UNION ALL SELECT 'google_ads_placement_TEST' FROM esp_digital_report.google_ads_placement_TEST
    UNION ALL SELECT 'google_ads_audience_TEST' FROM esp_digital_report.google_ads_audience_TEST
    UNION ALL SELECT 'google_ads_report_TEST' FROM esp_digital_report.google_ads_report_TEST
    UNION ALL SELECT 'google_ads_violation_TEST' FROM esp_digital_report.google_ads_violation_TEST

    -- Facebook (6 tabelle)
    UNION ALL SELECT 'fb_ads_campaign_TEST' FROM esp_digital_report.fb_ads_campaign_TEST
    UNION ALL SELECT 'fb_ads_ad_set_TEST' FROM esp_digital_report.fb_ads_ad_set_TEST
    UNION ALL SELECT 'fb_ads_audience_adset_TEST' FROM esp_digital_report.fb_ads_audience_adset_TEST
    UNION ALL SELECT 'fb_ads_custom_conversion_TEST' FROM esp_digital_report.fb_ads_custom_conversion_TEST
    UNION ALL SELECT 'fb_ads_insight_TEST' FROM esp_digital_report.fb_ads_insight_TEST
    UNION ALL SELECT 'fb_ads_insight_actions_TEST' FROM esp_digital_report.fb_ads_insight_actions_TEST
) t
GROUP BY table_name
ORDER BY table_name;
```

### Lista Rapida Tabelle TEST

```sql
-- Verifica quali tabelle TEST esistono
SELECT table_name
FROM v_catalog.tables
WHERE table_schema = 'esp_digital_report'
  AND table_name LIKE '%_TEST'
ORDER BY table_name;

-- Dovrebbe mostrare 20 tabelle dopo il test completo
```

### Visualizzare Sample Data

```sql
-- LinkedIn (REAL data)
SELECT TOP 5 * FROM esp_digital_report.linkedin_ads_campaign_TEST;

-- Google (MOCK data)
SELECT TOP 5 * FROM esp_digital_report.google_ads_campaign_TEST;

-- Facebook (MOCK data)
SELECT TOP 5 * FROM esp_digital_report.fb_ads_campaign_TEST;
```

---

## ⚙️ Mock Data vs Real Data

### LinkedIn - Dati REALI ✅

LinkedIn adapter è **completamente implementato**:
- Fa chiamate API reali a LinkedIn Marketing API v202509
- Recupera campaign URNs dal database per insights
- Gestisce tutte le dipendenze tra tabelle
- Applica processing strategies completo
- **Output**: Dati reali di produzione

### Google - Dati MOCK ⏳

Google adapter è in **test mode**:
- **NON** fa chiamate API reali
- Genera 3 righe di dati mock per ogni tabella
- Mock data include: id, name, status, row_loaded_date
- **Output**: Dati fittizi per test di struttura

**Mock data example**:
```python
{
    'id': ['1', '2', '3'],
    'name': ['google_ads_campaign_test_1', 'google_ads_campaign_test_2', 'google_ads_campaign_test_3'],
    'status': ['ENABLED', 'PAUSED', 'ENABLED'],
    'row_loaded_date': [datetime.now()] * 3
}
```

### Facebook - Dati MOCK ⏳

Facebook adapter è in **test mode**:
- **NON** fa chiamate API reali
- Genera 3 righe di dati mock per ogni tabella
- Mock data include: id, name, status, row_loaded_date
- **Output**: Dati fittizi per test di struttura

**Mock data example**:
```python
{
    'id': ['fb_1', 'fb_2', 'fb_3'],
    'name': ['fb_ads_campaign_test_1', 'fb_ads_campaign_test_2', 'fb_ads_campaign_test_3'],
    'status': ['ACTIVE', 'PAUSED', 'ACTIVE'],
    'row_loaded_date': [datetime.now()] * 3
}
```

---

## 🎯 Output Atteso

### Test Completo (All Platforms)

```bash
$ python -m social.test_pipeline --platform all --verbose

================================================================================
  SOCIAL PIPELINE - TEST SUITE
  SOLID Architecture Validation
================================================================================

Test Configuration:
  Platform: all
  Tables: ALL
  Dry Run: False
  Verbose: True

--------------------------------------------------------------------------------
TEST 1: Configuration Loading
--------------------------------------------------------------------------------
✓ Configuration loaded successfully
  Test Mode: True
  Platforms: linkedin, google, facebook
  Database: your-host:5433

--------------------------------------------------------------------------------
TEST 2: Adapter Initialization
--------------------------------------------------------------------------------
✓ Adapters initialized successfully
  Initialized platforms: linkedin, google, facebook
  linkedin: 6 tables configured
  google: 8 tables configured
  facebook: 6 tables configured

--------------------------------------------------------------------------------
TEST 3: Pipeline Execution
--------------------------------------------------------------------------------

LINKEDIN (REAL DATA):
Extracting LinkedIn table: linkedin_ads_account
✓ Loaded 5 rows to linkedin_ads_account_TEST
Extracting LinkedIn table: linkedin_ads_campaign
✓ Loaded 125 rows to linkedin_ads_campaign_TEST
... (6 tabelle totali)

GOOGLE (MOCK DATA):
[TEST MODE] Extracting Google Ads table: google_ads_account (using mock data)
[TEST MODE] Generated 3 mock rows for google_ads_account
✓ Loaded 3 rows to google_ads_account_TEST
... (8 tabelle totali)

FACEBOOK (MOCK DATA):
[TEST MODE] Extracting Facebook Ads table: fb_ads_campaign (using mock data)
[TEST MODE] Generated 3 mock rows for fb_ads_campaign
✓ Loaded 3 rows to fb_ads_campaign_TEST
... (6 tabelle totali)

================================================================================
VALIDATION RESULTS
================================================================================

LINKEDIN:
  ✓ SUCCESS: ~4000 rows (REAL DATA)

GOOGLE:
  ✓ SUCCESS: 24 rows (MOCK DATA - 8 tables × 3 rows)

FACEBOOK:
  ✓ SUCCESS: 18 rows (MOCK DATA - 6 tables × 3 rows)

================================================================================
TOTAL ROWS LOADED: ~4042 rows across 20 TEST tables
STATUS: ✓ ALL TESTS PASSED
================================================================================
```

---

## 🗑️ Cleanup Tabelle TEST

### Truncate Tutte le Tabelle TEST

```sql
-- LinkedIn
TRUNCATE TABLE esp_digital_report.linkedin_ads_account_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_campaign_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_audience_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_campaign_audience_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_insights_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_creative_TEST;

-- Google
TRUNCATE TABLE esp_digital_report.google_ads_account_TEST;
TRUNCATE TABLE esp_digital_report.google_ads_campaign_TEST;
TRUNCATE TABLE esp_digital_report.google_ads_ad_creatives_TEST;
TRUNCATE TABLE esp_digital_report.google_ads_cost_by_device_TEST;
TRUNCATE TABLE esp_digital_report.google_ads_placement_TEST;
TRUNCATE TABLE esp_digital_report.google_ads_audience_TEST;
TRUNCATE TABLE esp_digital_report.google_ads_report_TEST;
TRUNCATE TABLE esp_digital_report.google_ads_violation_TEST;

-- Facebook
TRUNCATE TABLE esp_digital_report.fb_ads_campaign_TEST;
TRUNCATE TABLE esp_digital_report.fb_ads_ad_set_TEST;
TRUNCATE TABLE esp_digital_report.fb_ads_audience_adset_TEST;
TRUNCATE TABLE esp_digital_report.fb_ads_custom_conversion_TEST;
TRUNCATE TABLE esp_digital_report.fb_ads_insight_TEST;
TRUNCATE TABLE esp_digital_report.fb_ads_insight_actions_TEST;
```

---

## 📝 Note Importanti

### Implementazione Platform

| Platform | Status | Data Type | Note |
|----------|--------|-----------|------|
| **LinkedIn** | ✅ Complete | REAL API data | Production-ready |
| **Google** | ⏳ Test mode | MOCK data | Adapter stub with mock data generator |
| **Facebook** | ⏳ Test mode | MOCK data | Adapter stub with mock data generator |

### Quando Usare Ogni Test

**`test_linkedin.bat`**:
- Testing LinkedIn adapter con dati reali
- Validazione end-to-end LinkedIn
- Before production deployment

**`test_google.bat`** / **`test_facebook.bat`**:
- Testing struttura tabelle
- Validazione configurazione
- Verifica database schema

**`test_all_platforms.bat`**:
- Testing completo di tutte e 3 le platform
- Validazione configurazione multi-platform
- Verifica che tutte le 20 tabelle TEST vengano create
- Integration testing

---

## 🚀 Roadmap

### Fase Attuale
- ✅ LinkedIn: Production-ready con dati reali
- ✅ Google: Test mode con mock data
- ✅ Facebook: Test mode con mock data
- ✅ Tutti e 3 scrivono su tabelle TEST

### Prossimi Step
1. Completare Google Ads adapter (sostituire mock con API reale)
2. Completare Facebook Ads adapter (sostituire mock con API reale)
3. Add unit tests per tutti gli adapter
4. Add integration tests

---

## 🆘 Support

Per domande:
1. Check logs in `logs/social_pipeline_*.log`
2. Review TABLES_REFERENCE.md per schema tabelle
3. Review TESTING.md per troubleshooting
4. Contact: alex.marino@esprinet.com
