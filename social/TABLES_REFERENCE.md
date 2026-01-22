# Social Pipeline - Tabelle di Destinazione

## Overview

Questo documento elenca tutte le tabelle su cui scrive il pipeline durante i test e in produzione.

**Importante**: In modalità TEST, tutte le tabelle hanno il suffisso `_TEST`.

---

## 🔵 LinkedIn Ads (6 Tabelle)

### Tabelle Production

| # | Tabella | Descrizione | Dipendenze |
|---|---------|-------------|------------|
| 1 | `linkedin_ads_account` | Account LinkedIn Ads | - |
| 2 | `linkedin_ads_campaign` | Campagne pubblicitarie | Account |
| 3 | `linkedin_ads_audience` | Segmenti di audience/targeting | Account |
| 4 | `linkedin_ads_campaign_audience` | Relazione campagne-audience | Campaign, Audience |
| 5 | `linkedin_ads_insights` | Metriche performance giornaliere | Campaign |
| 6 | `linkedin_ads_creative` | Creatività/annunci | Campaign, Insights |

### Tabelle TEST

Durante i test, il pipeline scrive su:

```
linkedin_ads_account_TEST
linkedin_ads_campaign_TEST
linkedin_ads_audience_TEST
linkedin_ads_campaign_audience_TEST
linkedin_ads_insights_TEST
linkedin_ads_creative_TEST
```

### Ordine di Esecuzione

Il pipeline rispetta automaticamente le dipendenze:

1. `linkedin_ads_account`
2. `linkedin_ads_campaign` (requires account)
3. `linkedin_ads_audience` (requires account)
4. `linkedin_ads_campaign_audience` (requires campaign + audience)
5. `linkedin_ads_insights` (requires campaign URNs from database)
6. `linkedin_ads_creative` (requires creative URNs from insights)

### Colonne Principali

#### linkedin_ads_account
```sql
- id (VARCHAR)
- name (VARCHAR)
- currency (VARCHAR)
- status (VARCHAR)
- companyid (INT)
- row_loaded_date (TIMESTAMP)
```

#### linkedin_ads_campaign
```sql
- id (VARCHAR)
- account (VARCHAR)
- name (VARCHAR)
- status (VARCHAR)
- objectiveType (VARCHAR)
- costType (VARCHAR)
- start_date (DATE)
- end_date (DATE)
- totalBudget (NUMERIC)
- dailyBudget (NUMERIC)
- unitCost (NUMERIC)
- row_loaded_date (TIMESTAMP)
```

#### linkedin_ads_audience
```sql
- id (VARCHAR)
- account (VARCHAR)
- name (VARCHAR)
- description (VARCHAR)
- row_loaded_date (TIMESTAMP)
```

#### linkedin_ads_campaign_audience
```sql
- id (VARCHAR) -- campaign_id
- audience_id (VARCHAR)
- row_loaded_date (TIMESTAMP)
```

#### linkedin_ads_insights
```sql
- creative_id (VARCHAR)
- date (DATE)
- actionClicks (INT)
- adUnitClicks (INT)
- clicks (INT)
- comments (INT)
- costInLocalCurrency (NUMERIC)
- landingPageClicks (INT)
- likes (INT)
- reactions (INT)
- shares (INT)
- totalEngagements (INT)
- impressions (INT)
- externalWebsiteConversions (INT)
- conversionValueInLocalCurrency (NUMERIC)
- row_loaded_date (TIMESTAMP)
```

#### linkedin_ads_creative
```sql
- id (VARCHAR)
- campaign_id (VARCHAR)
- account (VARCHAR)
- name (VARCHAR)
- status (VARCHAR)
- status_0 (VARCHAR)
- type (VARCHAR)
- lastModifiedAt (TIMESTAMP)
- createdAt (TIMESTAMP)
- row_loaded_date (TIMESTAMP)
```

---

## 🔴 Google Ads (8 Tabelle)

### Tabelle Production

| # | Tabella | Descrizione |
|---|---------|-------------|
| 1 | `google_ads_account` | Account Google Ads |
| 2 | `google_ads_campaign` | Campagne |
| 3 | `google_ads_ad_creatives` | Creatività annunci |
| 4 | `google_ads_cost_by_device` | Costi per dispositivo |
| 5 | `google_ads_placement` | Placement annunci |
| 6 | `google_ads_audience` | Audience/segmenti |
| 7 | `google_ads_report` | Report aggregati |
| 8 | `google_ads_violation` | Violazioni policy |

### Tabelle TEST

Durante i test:

```
google_ads_account_TEST
google_ads_campaign_TEST
google_ads_ad_creatives_TEST
google_ads_cost_by_device_TEST
google_ads_placement_TEST
google_ads_audience_TEST
google_ads_report_TEST
google_ads_violation_TEST
```

**Note**: L'adapter Google non è ancora implementato (stub), quindi queste tabelle non vengono ancora popolate.

---

## 🟢 Facebook Ads (6 Tabelle)

### Tabelle Production

| # | Tabella | Descrizione |
|---|---------|-------------|
| 1 | `fb_ads_campaign` | Campagne Facebook |
| 2 | `fb_ads_ad_set` | Ad Set (gruppi di annunci) |
| 3 | `fb_ads_audience_adset` | Relazione audience-adset |
| 4 | `fb_ads_custom_conversion` | Conversioni custom |
| 5 | `fb_ads_insight` | Metriche performance |
| 6 | `fb_ads_insight_actions` | Azioni utente dettagliate |

### Tabelle TEST

Durante i test:

```
fb_ads_campaign_TEST
fb_ads_ad_set_TEST
fb_ads_audience_adset_TEST
fb_ads_custom_conversion_TEST
fb_ads_insight_TEST
fb_ads_insight_actions_TEST
```

**Note**: L'adapter Facebook non è ancora implementato (stub), quindi queste tabelle non vengono ancora popolate.

---

## 📊 Riepilogo Totale

### Tutte le Platform

| Platform | Tabelle Production | Tabelle TEST | Status |
|----------|-------------------|--------------|--------|
| **LinkedIn** | 6 | 6 | ✅ Implementato |
| **Google** | 8 | 8 | ⏳ Stub (da implementare) |
| **Facebook** | 6 | 6 | ⏳ Stub (da implementare) |
| **TOTALE** | **20** | **20** | - |

---

## 🧪 Comandi per Testare

### Test LinkedIn (Scrive su tabelle _TEST)

```bash
# Windows
test_linkedin.bat

# Linux/Mac
./test_linkedin.sh

# Python
python -m social.test_pipeline --platform linkedin --verbose
```

### Test Tutte le Platform

```bash
# Quando Google e Facebook saranno implementati
python -m social.test_pipeline --platform all
```

### Test Tabelle Specifiche

```bash
# Solo campaign e insights
python -m social.test_pipeline --platform linkedin \
    --tables linkedin_ads_campaign,linkedin_ads_insights
```

---

## 🔍 Verificare i Dati nel Database

### Contare Rows nelle Tabelle TEST

```sql
-- LinkedIn
SELECT
    'linkedin_ads_account_TEST' as table_name,
    COUNT(*) as row_count
FROM esp_digital_report.linkedin_ads_account_TEST
UNION ALL
SELECT 'linkedin_ads_campaign_TEST', COUNT(*)
FROM esp_digital_report.linkedin_ads_campaign_TEST
UNION ALL
SELECT 'linkedin_ads_audience_TEST', COUNT(*)
FROM esp_digital_report.linkedin_ads_audience_TEST
UNION ALL
SELECT 'linkedin_ads_campaign_audience_TEST', COUNT(*)
FROM esp_digital_report.linkedin_ads_campaign_audience_TEST
UNION ALL
SELECT 'linkedin_ads_insights_TEST', COUNT(*)
FROM esp_digital_report.linkedin_ads_insights_TEST
UNION ALL
SELECT 'linkedin_ads_creative_TEST', COUNT(*)
FROM esp_digital_report.linkedin_ads_creative_TEST
ORDER BY table_name;

-- Google (quando implementato)
SELECT
    'google_ads_campaign_TEST' as table_name,
    COUNT(*) as row_count
FROM esp_digital_report.google_ads_campaign_TEST
UNION ALL
SELECT 'google_ads_ad_creatives_TEST', COUNT(*)
FROM esp_digital_report.google_ads_ad_creatives_TEST
-- ... altre tabelle
ORDER BY table_name;

-- Facebook (quando implementato)
SELECT
    'fb_ads_campaign_TEST' as table_name,
    COUNT(*) as row_count
FROM esp_digital_report.fb_ads_campaign_TEST
UNION ALL
SELECT 'fb_ads_insight_TEST', COUNT(*)
FROM esp_digital_report.fb_ads_insight_TEST
-- ... altre tabelle
ORDER BY table_name;
```

### Visualizzare Sample Data

```sql
-- LinkedIn Campaign
SELECT TOP 10 *
FROM esp_digital_report.linkedin_ads_campaign_TEST
ORDER BY row_loaded_date DESC;

-- LinkedIn Insights
SELECT TOP 10 *
FROM esp_digital_report.linkedin_ads_insights_TEST
ORDER BY date DESC, row_loaded_date DESC;

-- Verificare dipendenze (insights deve avere creative_id che esistono)
SELECT DISTINCT i.creative_id
FROM esp_digital_report.linkedin_ads_insights_TEST i
LEFT JOIN esp_digital_report.linkedin_ads_creative_TEST c
    ON i.creative_id = c.id
WHERE c.id IS NULL;
-- Dovrebbe tornare 0 rows se tutto OK
```

### Verificare Tutte le Tabelle TEST Esistenti

```sql
-- Lista tutte le tabelle TEST
SELECT table_name
FROM v_catalog.tables
WHERE table_schema = 'esp_digital_report'
  AND table_name LIKE '%_TEST'
ORDER BY table_name;
```

---

## 🗑️ Cleanup Tabelle TEST

### Truncate Dati TEST

```sql
-- LinkedIn
TRUNCATE TABLE esp_digital_report.linkedin_ads_account_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_campaign_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_audience_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_campaign_audience_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_insights_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_creative_TEST;

-- Google
TRUNCATE TABLE esp_digital_report.google_ads_campaign_TEST;
TRUNCATE TABLE esp_digital_report.google_ads_ad_creatives_TEST;
-- ... altre tabelle

-- Facebook
TRUNCATE TABLE esp_digital_report.fb_ads_campaign_TEST;
TRUNCATE TABLE esp_digital_report.fb_ads_insight_TEST;
-- ... altre tabelle
```

### Drop Tabelle TEST

```sql
-- LinkedIn
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_account_TEST;
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_campaign_TEST;
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_audience_TEST;
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_campaign_audience_TEST;
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_insights_TEST;
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_creative_TEST;

-- Google
DROP TABLE IF EXISTS esp_digital_report.google_ads_campaign_TEST;
DROP TABLE IF EXISTS esp_digital_report.google_ads_ad_creatives_TEST;
-- ... altre tabelle

-- Facebook
DROP TABLE IF EXISTS esp_digital_report.fb_ads_campaign_TEST;
DROP TABLE IF EXISTS esp_digital_report.fb_ads_insight_TEST;
-- ... altre tabelle
```

---

## 📋 Note Importanti

### Test Mode

- Il pipeline in test mode aggiunge automaticamente il suffisso `_TEST` a tutte le tabelle
- Le tabelle TEST devono avere la stessa struttura delle tabelle production
- Il test mode è attivato di default quando si usa `test_pipeline.py`

### Deduplication

Il pipeline usa la strategia **left anti-join** per evitare duplicati:
- Legge i dati esistenti dalla tabella usando le colonne chiave
- Fa un merge con `how='left'` e `indicator=True`
- Mantiene solo le righe con `indicator='left_only'`

### Dependencies

Alcune tabelle hanno dipendenze:
- **linkedin_ads_insights**: Richiede URN di campaign dal database
- **linkedin_ads_creative**: Richiede URN di creative dagli insights
- **linkedin_ads_campaign_audience**: Richiede campaign e audience

Il pipeline gestisce automaticamente l'ordine di esecuzione usando **topological sort**.

---

## 🎯 Schema Completo

```
Database: esp_digital_report

LinkedIn Tables (Production):
├── linkedin_ads_account
├── linkedin_ads_campaign
├── linkedin_ads_audience
├── linkedin_ads_campaign_audience
├── linkedin_ads_insights
└── linkedin_ads_creative

LinkedIn Tables (TEST):
├── linkedin_ads_account_TEST
├── linkedin_ads_campaign_TEST
├── linkedin_ads_audience_TEST
├── linkedin_ads_campaign_audience_TEST
├── linkedin_ads_insights_TEST
└── linkedin_ads_creative_TEST

Google Tables (Production):
├── google_ads_account
├── google_ads_campaign
├── google_ads_ad_creatives
├── google_ads_cost_by_device
├── google_ads_placement
├── google_ads_audience
├── google_ads_report
└── google_ads_violation

Google Tables (TEST):
├── google_ads_account_TEST
├── google_ads_campaign_TEST
├── google_ads_ad_creatives_TEST
├── google_ads_cost_by_device_TEST
├── google_ads_placement_TEST
├── google_ads_audience_TEST
├── google_ads_report_TEST
└── google_ads_violation_TEST

Facebook Tables (Production):
├── fb_ads_campaign
├── fb_ads_ad_set
├── fb_ads_audience_adset
├── fb_ads_custom_conversion
├── fb_ads_insight
└── fb_ads_insight_actions

Facebook Tables (TEST):
├── fb_ads_campaign_TEST
├── fb_ads_ad_set_TEST
├── fb_ads_audience_adset_TEST
├── fb_ads_custom_conversion_TEST
├── fb_ads_insight_TEST
└── fb_ads_insight_actions_TEST
```

---

## 🆘 Support

Per domande sulle tabelle:
1. Verifica config YAML in `social/platforms/{platform}/`
2. Controlla logs in `logs/social_pipeline_*.log`
3. Review TESTING.md per troubleshooting
4. Contact: alex.marino@esprinet.com
