# Social Pipeline Monitoring System - Documentazione Completa

Sistema di monitoring e alerting per le pipeline ETL social (Facebook, Google, Microsoft, LinkedIn).

---

## **📋 OVERVIEW**

### Cosa fa questo sistema

Ogni notte, dopo che i 4 Container App Jobs completano le loro esecuzioni:

1. ✅ Ogni pipeline **scrive un execution summary** (JSON) su Blob Storage
2. ✅ Una **Logic App** legge tutti i summary e li aggrega
3. ✅ Viene inviata una **email di report** con status di tutte le pipeline
4. ✅ Email ha **status visivo** (✅ success / ⚠️ partial / ❌ failed)

### Benefici

- **Visibility**: stato di tutte le pipeline in una sola email
- **Proattività**: alert immediato se qualcosa va storto
- **Audit**: storico esecuzioni conservato per 90 giorni
- **Scalabilità**: aggiungi nuove pipeline senza modificare Logic App
- **Economico**: ~€0.06/mese di costi Azure

---

## **🏗️ ARCHITETTURA**

```
┌─────────────────────────────────────────────────────────┐
│          Container App Jobs (Schedulati 02:00)          │
├─────────────────────────────────────────────────────────┤
│  Facebook Job    │  Google Job   │  Microsoft  │ LinkedIn│
│   ↓ finisce      │   ↓ finisce   │  ↓ finisce  │ ↓ finisce│
│   02:15          │   02:25       │  02:18      │  02:22   │
└──────┬───────────┴──────┬────────┴──────┬──────┴────┬────┘
       │                  │               │           │
       ▼                  ▼               ▼           ▼
┌─────────────────────────────────────────────────────────┐
│           Azure Blob Storage: stdigitalreportsdc        │
│              Container: social-pipeline-logs/           │
├─────────────────────────────────────────────────────────┤
│  facebook/2026-02-23/exec-020245-abc.json               │
│  google/2026-02-23/exec-022512-def.json                 │
│  microsoft/2026-02-23/exec-021845-ghi.json              │
│  linkedin/2026-02-23/exec-022203-jkl.json               │
└──────────────────────┬──────────────────────────────────┘
                       │
                       │ 02:45 - Logic App Trigger
                       ▼
┌─────────────────────────────────────────────────────────┐
│         Logic App: logic-social-pipeline-report         │
├─────────────────────────────────────────────────────────┤
│  1. List blobs per ogni platform                        │
│  2. Read & parse JSON summary                           │
│  3. Aggrega risultati                                   │
│  4. Build HTML report                                   │
│  5. Send email                                          │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
                  📧 Email Report
         (team@azienda.com - 02:46)
```

---

## **📁 FILE STRUTTURA**

### Codice Python

```
digital_report_etl_pipelines/
├── shared/
│   └── monitoring/
│       ├── __init__.py
│       └── execution_summary.py          ← Core monitoring class
│
└── social/
    └── platforms/
        ├── facebook/
        │   └── run_facebook.py            ← Modificato (summary writer)
        ├── google/
        │   └── run_google.py              ← Modificato (summary writer)
        ├── microsoft/
        │   └── run_microsoft.py           ← Modificato (summary writer)
        └── linkedin/
            └── run_linkedin_all.py        ← Modificato (summary writer)
```

### Documentazione Azure

```
azure/
├── README_MONITORING_SYSTEM.md            ← Questo file
├── LOGIC_APP_QUICK_SETUP.md               ← Guida rapida (15 min)
├── LOGIC_APP_SETUP_GUIDE.md               ← Guida dettagliata
├── TESTING_GUIDE.md                       ← Guida testing completo
├── email-template-simple.html             ← Template HTML email
└── logic-app-social-pipeline-report.json  ← ARM template (opzionale)
```

---

## **🚀 SETUP RAPIDO (30 minuti)**

### 1. Blob Storage (5 min)

✅ **GIÀ FATTO** - usi storage esistente `stdigitalreportsdc`

```bash
# Verifica container esiste
az storage container show \
  --name social-pipeline-logs \
  --account-name stdigitalreportsdc
```

### 2. Env Variables Container Jobs (5 min)

Aggiungi a tutti e 4 i Container App Jobs:

```bash
# Get connection string
CONN_STR=$(az storage account show-connection-string \
  --name stdigitalreportsdc \
  --resource-group <TUO_RG> \
  --output tsv)

# Aggiorna jobs (uno alla volta o loop)
az containerapp job update \
  --name facebook-ads-job \
  --resource-group <TUO_RG> \
  --set-env-vars "SUMMARY_STORAGE_CONNECTION_STRING=$CONN_STR"

# Ripeti per: google-ads-job, microsoft-ads-job, linkedin-ads-job
```

### 3. Logic App (15 min)

Segui: **`LOGIC_APP_QUICK_SETUP.md`**

Versione veloce:
1. Crea Logic App (Consumption)
2. Trigger: Recurrence (ogni giorno 02:45)
3. Variabili: AllSummaries, TodayDate
4. For each platform → List blobs → Parse JSON → Append
5. Compose email HTML
6. Send email (Office 365)

### 4. Test (5 min)

```bash
# Trigger manuale un job
az containerapp job start \
  --name facebook-ads-job \
  --resource-group <TUO_RG>

# Aspetta completamento → verifica blob creato
# Poi trigger manuale Logic App → verifica email
```

---

## **📊 EXECUTION SUMMARY FORMAT**

### Esempio JSON (Success)

```json
{
  "execution_id": "550e8400-e29b-41d4-a716-446655440000",
  "platform": "facebook",
  "status": "success",
  "start_time": "2026-02-23T02:00:15.123456",
  "end_time": "2026-02-23T02:15:42.789012",
  "duration_seconds": 927.67,
  "tables": [
    {
      "name": "fb_ads_insight",
      "success": true,
      "rows": 1500,
      "columns": 25,
      "column_names": ["date", "campaign_id", "impressions", "..."]
    },
    {
      "name": "fb_ads_campaign",
      "success": true,
      "rows": 150,
      "columns": 20,
      "column_names": ["campaign_id", "name", "status", "..."]
    }
  ],
  "tables_count": 2,
  "total_rows": 1650,
  "exit_code": 0,
  "errors": [],
  "metadata": {
    "account_ids": ["act_123456789"],
    "tables_count": 2
  }
}
```

### Esempio JSON (Failed)

```json
{
  "execution_id": "660f9511-f30c-52e5-b827-557766551111",
  "platform": "google",
  "status": "failed",
  "start_time": "2026-02-23T02:00:20.456789",
  "end_time": "2026-02-23T02:01:35.123456",
  "duration_seconds": 74.67,
  "tables": [],
  "tables_count": 0,
  "total_rows": 0,
  "exit_code": 2,
  "errors": [
    {
      "type": "AuthenticationError",
      "message": "Invalid refresh token",
      "timestamp": "2026-02-23T02:01:35.000000"
    }
  ],
  "metadata": {}
}
```

### Esempio JSON (Partial Success)

```json
{
  "execution_id": "770e8622-g41d-63f6-c938-668877662222",
  "platform": "microsoft",
  "status": "partial_success",
  "start_time": "2026-02-23T02:00:18.789012",
  "end_time": "2026-02-23T02:12:45.456789",
  "duration_seconds": 747.67,
  "tables": [
    {
      "name": "ms_ads_campaign",
      "success": true,
      "rows": 500,
      "columns": 18
    },
    {
      "name": "ms_ads_adgroup",
      "success": false,
      "rows": 0,
      "error": "Timeout"
    }
  ],
  "tables_count": 2,
  "tables_succeeded": 1,
  "tables_failed": 1,
  "total_rows": 500,
  "exit_code": 3,
  "errors": [
    {
      "table": "ms_ads_adgroup",
      "message": "Request timeout after 300s"
    }
  ],
  "metadata": {
    "customer_id": "123456",
    "account_id": "789012"
  }
}
```

---

## **📧 EMAIL REPORT**

### Subject Line

```
✅ All Success - Social Pipelines Report - 2026-02-23
⚠️ Partial Success - Social Pipelines Report - 2026-02-23
❌ Failures Detected - Social Pipelines Report - 2026-02-23
```

### Body (HTML)

Vedi: **`email-template-simple.html`** per template completo.

**Contiene:**
- 📊 Summary stats (success/partial/failed count)
- 📋 Tabella dettaglio per platform (status, tables, rows, duration)
- ⚠️ Alert box per failures o missing platforms
- 🎨 Styling professionale (colori, badge, responsive)

---

## **⚙️ CONFIGURAZIONE**

### Environment Variables (Container Jobs)

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `SUMMARY_STORAGE_CONNECTION_STRING` | ✅ Yes | Connection string Blob Storage | `DefaultEndpointsProtocol=https;...` |

**Tutti gli altri env var** (FACEBOOK_APP_ID, VERTICA_HOST, ecc.) rimangono invariati.

### Logic App Settings

| Setting | Value | Description |
|---------|-------|-------------|
| Schedule | `02:45` UTC+1 | Trigger time (dopo job completion ~02:30) |
| Time zone | `W. Europe Standard Time` | Timezone italiano |
| Frequency | `Daily` | Run ogni notte |
| Email recipient | `team@azienda.com` | Indirizzo email destinatario |

---

## **🔧 MAINTENANCE**

### Lifecycle Policy Blob Storage

**Retention: 90 giorni** (già configurato)

Vecchi blob vengono eliminati automaticamente per evitare accumulo:

```json
{
  "rules": [
    {
      "name": "delete-old-execution-logs",
      "enabled": true,
      "type": "Lifecycle",
      "definition": {
        "filters": {
          "blobTypes": ["blockBlob"],
          "prefixMatch": ["social-pipeline-logs/"]
        },
        "actions": {
          "baseBlob": {
            "delete": {
              "daysAfterModificationGreaterThan": 90
            }
          }
        }
      }
    }
  ]
}
```

### Monitoring

**Metriche da monitorare:**

1. **Container Jobs:**
   - Success rate (target: 100%)
   - Duration trend
   - Error rate per exit code

2. **Logic App:**
   - Run success rate (target: 100%)
   - Duration (target: < 1 min)
   - Email delivery success

3. **Blob Storage:**
   - File count per day (target: 4 files/day)
   - Missing platforms alert

**Alerts consigliati:**

```bash
# Alert se Logic App fallisce
az monitor metrics alert create \
  --name "Logic App Failed" \
  --resource-group <RG> \
  --scopes <LOGIC_APP_ID> \
  --condition "avg runs.failed > 0" \
  --action email-action-group
```

---

## **💰 COSTI**

### Breakdown Mensile

| Risorsa | Uso | Costo |
|---------|-----|-------|
| **Blob Storage** | | |
| - Storage | ~5 GB (90 giorni × 4 files × 2 KB) | €0.02 |
| - Transactions | 150 write + 30 read | €0.01 |
| **Logic App** | | |
| - Executions | 30 runs/month | €0.001 |
| - Actions | ~40 actions/run × 30 | €0.03 |
| **Office 365** | Email send | Incluso |
| **TOTALE** | | **~€0.07/mese** |

**Praticamente gratis!** ✅

---

## **🐛 TROUBLESHOOTING**

Vedi: **`TESTING_GUIDE.md`** sezione Troubleshooting per dettagli.

### Quick Fixes

| Problema | Soluzione Rapida |
|----------|------------------|
| Blob non viene scritto | Verifica env var `SUMMARY_STORAGE_CONNECTION_STRING` |
| Logic App non trova blob | Verifica date corretta, path blob corretto |
| Parse JSON fallisce | Aggiorna schema con blob effettivo |
| Email non arriva | Re-autorizza connessione Office 365 |
| Email con dati sbagliati | Verifica step "Append to AllSummaries" popolato |

---

## **📚 DOCUMENTAZIONE AGGIUNTIVA**

- **Quick Setup**: `LOGIC_APP_QUICK_SETUP.md` (15 min)
- **Setup Dettagliato**: `LOGIC_APP_SETUP_GUIDE.md` (guida completa)
- **Testing**: `TESTING_GUIDE.md` (test end-to-end)
- **Email Template**: `email-template-simple.html` (HTML template)

---

## **🎯 NEXT STEPS**

### Immediate (Produzione)

1. ✅ Verifica tutte le env var configurate
2. ✅ Test manuale di un job
3. ✅ Test manuale Logic App
4. ✅ Aspetta prima nightly run automatica (domani notte)
5. ✅ Verifica email mattina dopo

### Future Enhancements

- **Teams notification** invece/oltre email
- **Power BI dashboard** con storico esecuzioni
- **Slack integration** per alert real-time
- **Metrics export** a Prometheus/Grafana
- **Auto-retry** job falliti
- **Anomaly detection** (es. row count anomalo)

---

## **👥 TEAM & SUPPORT**

**Maintainer**: Data Engineering Team

**Contatti**:
- Per issues: GitHub Issues o Teams channel
- Per modifiche: PR con review del team
- Documentazione: Questa cartella `azure/`

---

**Sistema completamente operativo!** 🎉

**Ultima modifica**: 2026-02-23
**Versione**: 1.0.0
