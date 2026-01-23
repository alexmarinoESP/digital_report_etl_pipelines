# Quick Test Guide - Social Ads Platform

Guida rapida per testare il flusso completo delle pipeline senza impattare i dati di produzione.

---

## 🎯 Obiettivo

Testare tutte le 4 piattaforme (Microsoft, LinkedIn, Facebook, Google) recuperando dati recenti e scrivendoli su **tabelle di test** (suffisso `_test`).

---

## ⚙️ Prerequisiti

```bash
# 1. Assicurati di avere le credenziali configurate
cat .env | grep VERTICA
cat tokens.json

# 2. Controlla che le pipeline siano disponibili
ls -la social/platforms/*/pipeline.py
```

---

## 🚀 Esecuzione Test Rapido

### Opzione 1: Test Completo (Tutte le Piattaforme)

**Recupera ultimi 7 giorni di dati per tutte le piattaforme**:

```bash
# Esegui test completo
uv run python -m social.test_quick_run

# Output atteso:
# 🚀 SOCIAL ADS PLATFORM - QUICK TEST
# Date range: Last 7 days
# Target: TEST tables (suffix: _test)
#
# 🔵 Testing LinkedIn Ads...
# ✅ LinkedIn completed: 150 rows in 12.34s
#
# 🔵 Testing Facebook Ads...
# ✅ Facebook completed: 230 rows in 18.56s
#
# 🔵 Testing Google Ads...
# ✅ Google completed: 180 rows in 15.23s
#
# 🔵 Testing Microsoft Ads...
# ✅ Microsoft completed: 120 rows in 10.12s
#
# 📊 TEST SUMMARY
# Total Platforms: 4
# ✅ Successful: 4
# ❌ Failed: 0
# 📊 Total Rows: 680
# ⏱️  Total Duration: 56.25s
#
# 💾 Data written to TEST tables (suffix: _test)
```

### Opzione 2: Test Singola Piattaforma

**Testa solo LinkedIn**:
```bash
uv run python -m social.test_quick_run --platform linkedin
```

**Testa solo Facebook**:
```bash
uv run python -m social.test_quick_run --platform facebook
```

**Testa solo Google**:
```bash
uv run python -m social.test_quick_run --platform google
```

**Testa solo Microsoft**:
```bash
uv run python -m social.test_quick_run --platform microsoft
```

### Opzione 3: Personalizza Range Date

**Recupera ultimi 5 giorni**:
```bash
uv run python -m social.test_quick_run --days 5
```

**Recupera ultimi 14 giorni (solo LinkedIn)**:
```bash
uv run python -m social.test_quick_run --platform linkedin --days 14
```

---

## 🔍 Verifica Dati Scritti

### Controlla Tutte le Tabelle di Test

```bash
# Verifica dati in tutte le tabelle _test
uv run python -m social.verify_test_data

# Output atteso:
# 🔍 VERIFYING TEST TABLES
# Schema: social_ads
# Tables to check: 8
#
# 📊 Table: social_ads.linkedin_campaigns_test
# 📈 Total Rows: 150
# 📅 Date Range: 2026-01-15 to 2026-01-22
#
# 📊 Records by Date (last 10 days):
#    2026-01-22:     25 records
#    2026-01-21:     22 records
#    2026-01-20:     21 records
#    ...
#
# 📋 Sample Data (3 most recent records):
#    campaign_id  campaign_name          date        impressions  clicks  spend
#    123456789    Summer Campaign 2026   2026-01-22  1500         45      125.50
#    ...
```

### Controlla Singola Tabella

```bash
# Verifica solo LinkedIn
uv run python -m social.verify_test_data --table linkedin_campaigns_test

# Verifica solo Facebook
uv run python -m social.verify_test_data --table facebook_campaigns_test
```

---

## 📊 Tabelle di Test Create

Il test crea le seguenti tabelle (se non esistono già):

| Piattaforma | Tabelle Test |
|-------------|--------------|
| LinkedIn | `linkedin_campaigns_test`, `linkedin_ad_groups_test` |
| Facebook | `facebook_campaigns_test`, `facebook_ad_sets_test` |
| Google | `google_campaigns_test`, `google_ad_groups_test` |
| Microsoft | `microsoft_campaigns_test`, `microsoft_ad_groups_test` |

**Nota**: Tutte le tabelle hanno il suffisso `_test` per evitare conflitti con i dati di produzione.

---

## 🧹 Pulizia Tabelle di Test

**ATTENZIONE**: Questo comando elimina TUTTE le tabelle di test!

```bash
# Elimina tutte le tabelle _test
uv run python -m social.verify_test_data --cleanup

# Output:
# ⚠️  CLEANUP TEST TABLES
# This will DELETE all data in test tables!
# Are you sure? Type 'yes' to confirm:
# yes
#
# Found 8 test tables:
#   - linkedin_campaigns_test
#   - facebook_campaigns_test
#   - ...
#
# Deleting tables...
# ✅ Deleted: linkedin_campaigns_test
# ✅ Deleted: facebook_campaigns_test
# ...
```

---

## 🎯 Test Scenarios

### Scenario 1: Quick Sanity Check (5 minuti)

**Obiettivo**: Verificare che tutte le piattaforme funzionino

```bash
# 1. Esegui test con ultimi 5 giorni
uv run python -m social.test_quick_run --days 5

# 2. Verifica che ci siano dati
uv run python -m social.verify_test_data

# 3. Aspettati: ✅ per tutte le piattaforme, >0 righe
```

### Scenario 2: Deep Test Single Platform (10 minuti)

**Obiettivo**: Test approfondito di una piattaforma specifica

```bash
# 1. Test LinkedIn con 14 giorni
uv run python -m social.test_quick_run --platform linkedin --days 14

# 2. Verifica dettagli LinkedIn
uv run python -m social.verify_test_data --table linkedin_campaigns_test

# 3. Controlla:
#    - Row count > 0
#    - Date range corretto (ultimi 14 giorni)
#    - NULL values minimi
#    - Sample data ha senso
```

### Scenario 3: Full Integration Test (15 minuti)

**Obiettivo**: Test completo di tutte le piattaforme

```bash
# 1. Pulisci tabelle esistenti
uv run python -m social.verify_test_data --cleanup

# 2. Esegui test completo
uv run python -m social.test_quick_run --days 7

# 3. Verifica tutte le tabelle
uv run python -m social.verify_test_data

# 4. Aspettati:
#    - 4/4 piattaforme successful
#    - Tutte le tabelle con dati
#    - Durata totale < 5 minuti
```

---

## 🐛 Troubleshooting

### Problema: "ModuleNotFoundError: No module named 'social'"

**Soluzione**:
```bash
# Assicurati di essere nella root del progetto
cd /path/to/digital_report_etl_pipelines

# Installa dipendenze
uv sync --extra social

# Riprova
uv run python -m social.test_quick_run
```

### Problema: "TokenError: Token not found for platform 'linkedin'"

**Soluzione**:
```bash
# Verifica che tokens.json esista
ls -la tokens.json

# Verifica formato JSON
cat tokens.json
# Deve essere:
# {
#   "microsoft": "Bearer xxx...",
#   "linkedin": "Bearer yyy...",
#   "facebook": "Bearer zzz...",
#   "google": "Bearer www..."
# }

# Se mancante, crea tokens.json con i tuoi token
```

### Problema: "Connection refused to Vertica"

**Soluzione**:
```bash
# Verifica variabili ambiente
echo $VERTICA_HOST
echo $VERTICA_PORT
echo $VERTICA_USER

# Testa connessione
python -c "
import vertica_python
conn = vertica_python.connect(
    host='$VERTICA_HOST',
    port=$VERTICA_PORT,
    user='$VERTICA_USER',
    password='$VERTICA_PASSWORD',
    database='$VERTICA_DATABASE'
)
print('✅ Connected!')
conn.close()
"
```

### Problema: "Empty DataFrame / No data returned"

**Possibili cause**:

1. **Date range troppo vecchio**:
   ```bash
   # Usa range più recente
   uv run python -m social.test_quick_run --days 5
   ```

2. **Nessuna campagna attiva**:
   ```bash
   # Verifica che ci siano campagne attive nell'account
   # Controlla config files per account IDs corretti
   cat social/platforms/linkedin/config_linkedin_ads.yml
   ```

3. **Token scaduto**:
   ```bash
   # Rigenera token e aggiorna tokens.json
   ```

---

## 📈 Metriche di Successo

### Test Successful ✅

Un test è considerato successful se:
- ✅ Exit code = 0
- ✅ Tutte le piattaforme ritornano `success: true`
- ✅ Total rows > 0
- ✅ Durata < 5 minuti (per 7 giorni, tutte le piattaforme)
- ✅ Nessun errore nei log
- ✅ Dati scritti correttamente nelle tabelle di test

### Performance Baseline

| Platform | 7 giorni | Rows attesi | Durata attesa |
|----------|----------|-------------|---------------|
| LinkedIn | ~100-200 | 150 | ~15s |
| Facebook | ~150-300 | 230 | ~20s |
| Google | ~120-250 | 180 | ~18s |
| Microsoft | ~80-150 | 120 | ~12s |
| **TOTAL** | ~450-900 | ~680 | **~65s** |

**Nota**: I valori dipendono dal numero di campagne attive e dal volume di dati.

---

## 🎓 Best Practices

1. **Esegui test prima di deploy**:
   ```bash
   # Prima di fare deploy in produzione
   uv run python -m social.test_quick_run
   ```

2. **Usa tabelle di test per sviluppo**:
   ```bash
   # Durante sviluppo, scrivi sempre su _test
   # Solo in produzione usa tabelle normali
   ```

3. **Pulisci regolarmente**:
   ```bash
   # Pulisci tabelle test ogni settimana
   uv run python -m social.verify_test_data --cleanup
   ```

4. **Monitora performance**:
   ```bash
   # Se durata > 5 minuti, investiga:
   # - Connessione lenta a Vertica?
   # - API rate limiting?
   # - Troppi dati?
   ```

---

## 📚 Documentazione Correlata

- [README.md](README.md) - Panoramica progetto
- [docs/USAGE_GUIDE.md](docs/USAGE_GUIDE.md) - Guida utilizzo piattaforme
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) - Risoluzione problemi
- [docs/API_REFERENCE.md](docs/API_REFERENCE.md) - Riferimento API

---

## 🤝 Supporto

Se incontri problemi:
1. Controlla [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)
2. Verifica log di esecuzione
3. Contatta: Alessandro Benelli (alessandro.benelli@esprinet.com)

---

**Buon Testing! 🚀**
