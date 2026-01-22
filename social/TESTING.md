# Social Pipeline - Testing Guide

## Overview

Questa guida spiega come testare il nuovo Social Pipeline basato su SOLID principles.

---

## Test Scripts Disponibili

### 1. Script Python Completo (`test_pipeline.py`)

Script Python completo con validation e reporting dettagliato.

**Features**:
- ✅ Test configuration loading
- ✅ Test adapter initialization
- ✅ Test pipeline execution
- ✅ Results validation
- ✅ Detailed reporting
- ✅ Support per tutte le platform
- ✅ Support per dry-run mode

### 2. Script Batch per Windows (`test_linkedin.bat`)

Script Windows semplice per quick testing di LinkedIn.

### 3. Script Bash per Linux/Mac (`test_linkedin.sh`)

Script Unix/Linux/Mac per quick testing di LinkedIn.

---

## Quick Start

### Windows

```batch
# Test LinkedIn (scrive su tabelle _TEST)
test_linkedin.bat

# Oppure manuale
python -m social.test_pipeline --platform linkedin --verbose
```

### Linux/Mac

```bash
# Rendi eseguibile
chmod +x test_linkedin.sh

# Run
./test_linkedin.sh

# Oppure manuale
python -m social.test_pipeline --platform linkedin --verbose
```

---

## Comandi Test

### Test Singola Platform

```bash
# LinkedIn
python -m social.test_pipeline --platform linkedin

# Google (quando implementato)
python -m social.test_pipeline --platform google

# Facebook (quando implementato)
python -m social.test_pipeline --platform facebook
```

### Test Tutte le Platform

```bash
python -m social.test_pipeline --platform all
```

### Test Tabelle Specifiche

```bash
# Solo campaign e insights
python -m social.test_pipeline --platform linkedin \
    --tables linkedin_ads_campaign,linkedin_ads_insights

# Solo account
python -m social.test_pipeline --platform linkedin \
    --tables linkedin_ads_account
```

### Dry Run (No Database Writes)

```bash
# Test senza scrivere nel database
python -m social.test_pipeline --platform linkedin --dry-run
```

### Verbose Mode

```bash
# Debug-level logging
python -m social.test_pipeline --platform linkedin --verbose
```

---

## Cosa Fa il Test

Il test script esegue i seguenti step:

### 1. Configuration Loading Test
- Carica configurazione da YAML
- Valida environment variables
- Testa database connection

### 2. Adapter Initialization Test
- Inizializza adapters per le platform specificate
- Verifica token provider setup
- Valida table configurations

### 3. Pipeline Execution Test
- Esegue extraction per ogni tabella
- Applica processing strategies
- Scrive dati nel database (tabelle `_TEST`)
- Verifica dependency resolution

### 4. Results Validation
- Conta rows per ogni tabella
- Verifica che non ci siano errori
- Mostra summary dettagliato

---

## Output Atteso

### Success Case

```
================================================================================
  SOCIAL PIPELINE - TEST SUITE
  SOLID Architecture Validation
================================================================================

Test Configuration:
  Platform: linkedin
  Tables: ALL
  Dry Run: False
  Verbose: True

--------------------------------------------------------------------------------
TEST 1: Configuration Loading
--------------------------------------------------------------------------------
✓ Configuration loaded successfully
  Test Mode: True
  Platforms: linkedin
  Database: your-host:5433

--------------------------------------------------------------------------------
TEST 2: Adapter Initialization
--------------------------------------------------------------------------------
✓ Adapters initialized successfully
  Initialized platforms: linkedin
  linkedin: 6 tables configured

--------------------------------------------------------------------------------
TEST 3: Pipeline Execution
--------------------------------------------------------------------------------
Extracting LinkedIn table: linkedin_ads_account
✓ Loaded 5 rows to linkedin_ads_account_TEST
Extracting LinkedIn table: linkedin_ads_campaign
✓ Loaded 125 rows to linkedin_ads_campaign_TEST
Extracting LinkedIn table: linkedin_ads_audience
✓ Loaded 45 rows to linkedin_ads_audience_TEST
Extracting LinkedIn table: linkedin_ads_campaign_audience
✓ Loaded 180 rows to linkedin_ads_campaign_audience_TEST
Extracting LinkedIn table: linkedin_ads_insights
✓ Loaded 3500 rows to linkedin_ads_insights_TEST
Extracting LinkedIn table: linkedin_ads_creative
✓ Loaded 220 rows to linkedin_ads_creative_TEST

✓ Pipeline execution completed

================================================================================
VALIDATION RESULTS
================================================================================

LINKEDIN:
  ✓ SUCCESS: 4075 total rows
    ✓ linkedin_ads_account_TEST: 5 rows
    ✓ linkedin_ads_campaign_TEST: 125 rows
    ✓ linkedin_ads_audience_TEST: 45 rows
    ✓ linkedin_ads_campaign_audience_TEST: 180 rows
    ✓ linkedin_ads_insights_TEST: 3500 rows
    ✓ linkedin_ads_creative_TEST: 220 rows

================================================================================
TOTAL ROWS LOADED: 4075
STATUS: ✓ ALL TESTS PASSED
================================================================================

================================================================================
TEST SUITE SUMMARY
================================================================================
Tests Passed: 3/3
Success Rate: 100%
✓ ALL TESTS PASSED - Pipeline is working correctly!
```

---

## Verificare i Dati nel Database

Dopo il test, puoi verificare i dati direttamente nel database:

```sql
-- Verifica tabelle TEST create
SELECT table_name
FROM v_catalog.tables
WHERE table_schema = 'esp_digital_report'
AND table_name LIKE '%_TEST';

-- Conta rows per tabella
SELECT
    'linkedin_ads_account_TEST' as table_name,
    COUNT(*) as row_count
FROM esp_digital_report.linkedin_ads_account_TEST
UNION ALL
SELECT
    'linkedin_ads_campaign_TEST',
    COUNT(*)
FROM esp_digital_report.linkedin_ads_campaign_TEST
UNION ALL
SELECT
    'linkedin_ads_audience_TEST',
    COUNT(*)
FROM esp_digital_report.linkedin_ads_audience_TEST
UNION ALL
SELECT
    'linkedin_ads_campaign_audience_TEST',
    COUNT(*)
FROM esp_digital_report.linkedin_ads_campaign_audience_TEST
UNION ALL
SELECT
    'linkedin_ads_insights_TEST',
    COUNT(*)
FROM esp_digital_report.linkedin_ads_insights_TEST
UNION ALL
SELECT
    'linkedin_ads_creative_TEST',
    COUNT(*)
FROM esp_digital_report.linkedin_ads_creative_TEST;

-- Visualizza sample data
SELECT TOP 10 * FROM esp_digital_report.linkedin_ads_campaign_TEST;
SELECT TOP 10 * FROM esp_digital_report.linkedin_ads_insights_TEST;
```

---

## Troubleshooting

### Error: "No module named 'social'"

```bash
# Assicurati di essere nella directory corretta
cd /path/to/digital_report_etl_pipelines

# Run con python -m
python -m social.test_pipeline --platform linkedin
```

### Error: "Configuration not loaded"

```bash
# Verifica environment variables
echo $VERTICA_HOST
echo $VERTICA_USER
echo $VERTICA_PASSWORD

# Oppure crea .env file
cp .env.example .env
# Edit .env con i tuoi valori
```

### Error: "No tokens found in database"

```bash
# Verifica che i token siano nel database
# La tabella social_tokens deve esistere con tokens validi

# Query per verificare:
# SELECT platform, access_token, refresh_token, expires_at
# FROM esp_digital_report.social_tokens
# WHERE platform = 'linkedin' AND active = true;
```

### Error: "Table does not exist"

```bash
# Le tabelle TEST vengono create automaticamente
# Se non esistono, verifica DDL scripts in:
# shared/ddl/ o simili

# Oppure crea manualmente copiando struttura da tabelle production:
# CREATE TABLE linkedin_ads_campaign_TEST LIKE linkedin_ads_campaign;
```

### Logs per Debugging

```bash
# I log sono salvati in:
# logs/social_pipeline_YYYY-MM-DD.log

# Visualizza log in real-time
tail -f logs/social_pipeline_$(date +%Y-%m-%d).log

# Cerca errori
grep "ERROR" logs/social_pipeline_*.log
```

---

## Cleanup Test Data

Per pulire i dati di test dopo aver verificato:

```sql
-- Truncate tabelle TEST
TRUNCATE TABLE esp_digital_report.linkedin_ads_account_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_campaign_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_audience_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_campaign_audience_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_insights_TEST;
TRUNCATE TABLE esp_digital_report.linkedin_ads_creative_TEST;

-- Oppure drop tabelle TEST
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_account_TEST;
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_campaign_TEST;
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_audience_TEST;
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_campaign_audience_TEST;
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_insights_TEST;
DROP TABLE IF EXISTS esp_digital_report.linkedin_ads_creative_TEST;
```

---

## Production Deployment

Dopo che i test passano con successo:

### 1. Review Results

```bash
# Confronta risultati TEST vs legacy script
# Verifica che i dati siano consistenti
```

### 2. Run in Production Mode

```bash
# Run senza --test-mode (scrive su tabelle production)
python -m social.run_pipeline --platform linkedin
```

### 3. Monitor

```bash
# Monitor logs durante first production run
tail -f logs/social_pipeline_*.log

# Verifica row counts match expectations
```

### 4. Schedule

```bash
# Add to cron/Azure Container App Job schedule
# Esempio: daily at 2 AM
# 0 2 * * * cd /app && python -m social.run_pipeline --platform linkedin
```

---

## Advanced Testing

### Test con Mock Data

```python
# TODO: Create mock data fixtures for unit tests
# File: tests/fixtures/linkedin_mock_data.py
```

### Integration Tests

```python
# TODO: pytest integration tests
# File: tests/integration/test_linkedin_pipeline.py
```

### Performance Testing

```bash
# Measure execution time
time python -m social.test_pipeline --platform linkedin
```

---

## Support

Per domande o issues:
1. Check logs in `logs/` directory
2. Review REFACTORING_GUIDE.md
3. Check GitHub issues
4. Contact: alex.marino@esprinet.com
