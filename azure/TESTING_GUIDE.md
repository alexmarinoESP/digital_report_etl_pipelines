# Testing Guide - Social Pipeline Monitoring

Guida completa per testare il sistema di monitoring end-to-end.

---

## **FASE 1: Test Locale (Python)**

### 1.1 Test ExecutionSummaryWriter

Crea un test file: `test_summary_writer.py`

```python
import os
from datetime import datetime
import pandas as pd
from shared.monitoring import ExecutionSummaryWriter

# Set connection string (usa quello vero)
os.environ["SUMMARY_STORAGE_CONNECTION_STRING"] = "DefaultEndpointsProtocol=https;AccountName=stdigitalreportsdc;..."

# Test 1: Success
print("Test 1: Success summary")
writer = ExecutionSummaryWriter(platform="test-facebook")

test_data = {
    "fb_ads_insight": pd.DataFrame({"col1": [1, 2, 3]}),
    "fb_ads_campaign": pd.DataFrame({"col1": [4, 5]}),
}

writer.write_success(
    start_time=datetime.now(),
    end_time=datetime.now(),
    tables_processed=test_data,
    exit_code=0,
    metadata={"test": True}
)

print("✅ Success summary written!")

# Test 2: Failure
print("\nTest 2: Failure summary")
writer2 = ExecutionSummaryWriter(platform="test-google")

writer2.write_failure(
    start_time=datetime.now(),
    end_time=datetime.now(),
    error=Exception("Test error"),
    exit_code=3
)

print("✅ Failure summary written!")

# Test 3: Partial Success
print("\nTest 3: Partial success summary")
writer3 = ExecutionSummaryWriter(platform="test-microsoft")

writer3.write_partial_success(
    start_time=datetime.now(),
    end_time=datetime.now(),
    tables_succeeded={"table1": pd.DataFrame({"col": [1]})},
    tables_failed=["table2", "table3"],
    errors=[
        {"table": "table2", "message": "Timeout"},
        {"table": "table3", "message": "Auth failed"}
    ],
    exit_code=3
)

print("✅ Partial success summary written!")
```

**Esegui:**
```bash
python test_summary_writer.py
```

**Verifica:**
1. Console output mostra JSON summary
2. Azure Portal → Storage `stdigitalreportsdc` → Container `social-pipeline-logs`
3. Dovresti vedere 3 file:
   - `test-facebook/2026-02-23/exec-....json`
   - `test-google/2026-02-23/exec-....json`
   - `test-microsoft/2026-02-23/exec-....json`

---

## **FASE 2: Test Container Jobs**

### 2.1 Test con uv run (locale)

```bash
# Test Facebook
cd social/platforms/facebook
set SUMMARY_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=...
set STORAGE_TYPE=none
set FACEBOOK_APP_ID=...
set FACEBOOK_APP_SECRET=...
set FACEBOOK_ACCESS_TOKEN=...
set FB_AD_ACCOUNT_IDS=act_123456

uv run python run_facebook.py
```

**Verifica:**
1. ✅ Pipeline esegue (anche se fallisce per mancanza dati)
2. ✅ Console mostra "EXECUTION SUMMARY" JSON
3. ✅ Blob creato in `facebook/2026-02-23/exec-....json`

### 2.2 Test Container App Job (Azure)

**Opzione A: Run Manuale**
```bash
# Trigger manuale del job
az containerapp job start \
  --name facebook-ads-job \
  --resource-group <TUO_RG>
```

**Opzione B: Portal**
1. Azure Portal → Container App Job `facebook-ads-job`
2. **Execution history** → **Create execution**
3. Aspetta completamento (~10-15 min)

**Verifica:**
1. Job completa (status: Succeeded/Failed)
2. **Logs** → controlla output JSON "EXECUTION SUMMARY"
3. Storage → `social-pipeline-logs/facebook/YYYY-MM-DD/exec-....json` creato

---

## **FASE 3: Test Logic App**

### 3.1 Crea Dati di Test (se non hai esecuzioni reali)

Se i Container Jobs non sono ancora andati, crea file di test manualmente:

**Portal:**
1. Storage `stdigitalreportsdc` → Container `social-pipeline-logs`
2. **Upload** → crea struttura:
   ```
   facebook/2026-02-23/exec-test.json
   google/2026-02-23/exec-test.json
   microsoft/2026-02-23/exec-test.json
   linkedin/2026-02-23/exec-test.json
   ```

**Contenuto esempio (`facebook/2026-02-23/exec-test.json`):**
```json
{
  "execution_id": "test-facebook-001",
  "platform": "facebook",
  "status": "success",
  "start_time": "2026-02-23T02:00:00",
  "end_time": "2026-02-23T02:15:00",
  "duration_seconds": 900,
  "tables": [
    {
      "name": "fb_ads_insight",
      "success": true,
      "rows": 1500,
      "columns": 20
    }
  ],
  "tables_count": 3,
  "total_rows": 4500,
  "exit_code": 0,
  "errors": [],
  "metadata": {
    "test": true
  }
}
```

Ripeti per google, microsoft, linkedin (cambia platform e execution_id).

### 3.2 Test Manuale Logic App

1. Portal → Logic App `logic-social-pipeline-report`
2. **Overview** → **Run Trigger** → **Run**
3. **Runs history** → apri ultima esecuzione
4. Verifica ogni step:
   - ✅ Recurrence trigger
   - ✅ Initialize variables (AllSummaries, TodayDate)
   - ✅ For each platform (4 iterazioni)
   - ✅ List blobs (trova file per ogni platform)
   - ✅ Get blob content (legge JSON)
   - ✅ Parse JSON (nessun errore)
   - ✅ Append to AllSummaries
   - ✅ Compose email body
   - ✅ Send email

### 3.3 Verifica Email Ricevuta

1. Check inbox (email configurata in Logic App)
2. Verifica email contiene:
   - ✅ Subject: "Social Pipelines Report - 2026-02-23"
   - ✅ Body HTML con summary
   - ✅ Dati corretti (4 platforms, status, ecc.)

---

## **FASE 4: Test End-to-End (Completo)**

### Scenario: Nightly Run Completo

**Timing:**
```
02:00 - Container Jobs partono (schedulati)
02:15 - Facebook finisce → scrive summary
02:18 - Microsoft finisce → scrive summary
02:22 - LinkedIn finisce → scrive summary
02:25 - Google finisce → scrive summary
02:45 - Logic App parte → aggrega + invia email
02:46 - Email arriva
```

**Setup:**
1. ✅ Tutti i 4 Container Jobs hanno env var `SUMMARY_STORAGE_CONNECTION_STRING`
2. ✅ Logic App schedulata alle 02:45
3. ✅ Blob Storage container `social-pipeline-logs` esiste

**Esecuzione:**
1. **Aspetta il giorno dopo** (i job partono automaticamente alle 02:00)
2. Oppure **triggera manualmente** tutti e 4 i jobs:
   ```bash
   az containerapp job start --name facebook-ads-job --resource-group <RG>
   az containerapp job start --name google-ads-job --resource-group <RG>
   az containerapp job start --name microsoft-ads-job --resource-group <RG>
   az containerapp job start --name linkedin-ads-job --resource-group <RG>
   ```

**Verifica (mattina dopo):**

1. **Storage Account** → `social-pipeline-logs/`:
   ```
   facebook/2026-02-23/exec-20260223-020245-abc123.json ✅
   google/2026-02-23/exec-20260223-022312-def456.json ✅
   microsoft/2026-02-23/exec-20260223-021845-ghi789.json ✅
   linkedin/2026-02-23/exec-20260223-022103-jkl012.json ✅
   ```

2. **Logic App** → Run history:
   - Status: **Succeeded** ✅
   - Duration: ~30-60 secondi
   - Actions: ~30-40 (tutti green)

3. **Email**:
   - Ricevuta alle ~02:46 ✅
   - Subject corretto ✅
   - HTML ben formattato ✅
   - Dati accurati (4/4 platforms, status, rows, ecc.) ✅

---

## **TROUBLESHOOTING**

### Problema: Blob non viene scritto

**Diagnosi:**
```bash
# Check logs del Container Job
az containerapp job execution logs show \
  --name facebook-ads-job \
  --resource-group <RG> \
  --execution-name <EXECUTION_NAME>
```

**Cerca in logs:**
- ❌ "Failed to upload summary to Blob Storage" → check connection string
- ❌ "No storage connection string" → env var mancante
- ✅ "Summary uploaded to blob: facebook/..." → OK!

**Soluzioni:**
1. Verifica `SUMMARY_STORAGE_CONNECTION_STRING` configurata
2. Verifica connection string valido (copia/incolla corretto)
3. Verifica container `social-pipeline-logs` esiste

### Problema: Logic App non trova blob

**Diagnosi:**
1. Logic App → Run history → step "List blobs"
2. Check output: `value: []` (array vuoto)

**Soluzioni:**
1. Verifica date corretta (Logic App usa `TodayDate`, blobs hanno data corretta?)
2. Verifica path: `facebook/2026-02-23/` esiste?
3. Test manuale upload blob

### Problema: Parse JSON fallisce

**Diagnosi:**
- Step "Parse JSON" → rosso
- Error: "Invalid JSON" o "Schema mismatch"

**Soluzioni:**
1. Download blob da Storage → verifica JSON valido
2. Test JSON su jsonlint.com
3. Aggiorna schema in Logic App (usa blob effettivo per generarlo)

### Problema: Email non arriva

**Diagnosi:**
1. Logic App → step "Send email" → verde?
2. Check spam folder
3. Verifica connessione Office 365 autenticata

**Soluzioni:**
1. Re-autorizza connessione Office 365
2. Verifica recipient email corretto
3. Test con email personale

### Problema: Dati email sbagliati

**Diagnosi:**
- Email arriva ma mostra dati strani (es. "0 platforms")

**Soluzioni:**
1. Logic App → Run history → step "Append to AllSummaries"
2. Verifica output: array popolato?
3. Check step "Compose email" → inputs corretti?

---

## **MONITORING CONTINUO**

### Dashboard Consigliata

**Azure Portal → Monitor → Workbooks** → crea:

1. **Container Jobs Success Rate**
   - Query: runs ultimi 7 giorni
   - Chart: success vs failed

2. **Logic App Execution Time**
   - Trend duration nel tempo
   - Alert se > 2 minuti

3. **Blob Storage Growth**
   - Numero file per platform/giorno
   - Alert se mancano file

### Alerts Consigliati

```bash
# Alert: Container Job failed
az monitor metrics alert create \
  --name "Facebook Job Failed Alert" \
  --resource-group <RG> \
  --scopes <JOB_RESOURCE_ID> \
  --condition "avg executions.failed > 0" \
  --window-size 1h

# Alert: Logic App failed
az monitor metrics alert create \
  --name "Logic App Failed Alert" \
  --resource-group <RG> \
  --scopes <LOGIC_APP_ID> \
  --condition "avg runs.failed > 0" \
  --window-size 1h
```

---

## **CHECKLIST FINALE**

Prima di andare in produzione:

- [ ] ✅ Test locale ExecutionSummaryWriter funziona
- [ ] ✅ Tutti e 4 i Container Jobs hanno `SUMMARY_STORAGE_CONNECTION_STRING`
- [ ] ✅ Blob Storage container `social-pipeline-logs` creato
- [ ] ✅ Lifecycle policy configurata (90 giorni retention)
- [ ] ✅ Logic App creata e testata manualmente
- [ ] ✅ Logic App schedule corretta (02:45 UTC+1)
- [ ] ✅ Email ricevuta con dati corretti
- [ ] ✅ Test end-to-end con tutti e 4 i platform
- [ ] ✅ Documentazione aggiornata (questo file!)
- [ ] ✅ Team informato su nuovo sistema monitoring

---

**Sistema pronto per produzione!** 🚀

**Next**: Aspetta la prima nightly run automatica e verifica tutto funzioni.
