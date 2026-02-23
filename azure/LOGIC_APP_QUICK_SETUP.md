# Logic App - Setup Veloce (15 minuti)

Guida semplificata per creare la Logic App che aggrega i report delle pipeline social.

---

## **STEP 1: Crea Logic App (3 min)**

1. **Azure Portal** → **Logic Apps** → **+ Create**
2. Compila:
   - Name: `logic-social-pipeline-report`
   - Region: *stessa dei Container Apps*
   - Plan: **Consumption**
3. **Create** → Aspetta deploy → **Go to resource**

---

## **STEP 2: Apri Designer (1 min)**

1. Logic App → **Logic app designer** (menu sx)
2. Template: **Blank Logic App**

---

## **STEP 3: Aggiungi Trigger (2 min)**

### Trigger: Recurrence

1. Cerca: `Recurrence`
2. Configura:
   - **Interval**: 1
   - **Frequency**: Day
   - **Time zone**: (UTC+01:00) Brussels, Copenhagen...
   - **At these hours**: 2
   - **At these minutes**: 45

💡 Esegue ogni notte alle **02:45** (dopo che i jobs finiscono ~02:30)

---

## **STEP 4: Variabili (3 min)**

### 4.1 Variabile: AllSummaries

1. **+ New step** → cerca `Initialize variable`
2. Configura:
   - Name: `AllSummaries`
   - Type: **Array**
   - Value: *lascia vuoto*

### 4.2 Variabile: TodayDate

1. **+ New step** → `Initialize variable`
2. Configura:
   - Name: `TodayDate`
   - Type: **String**
   - Value: *click casella → Expression →*
     ```
     formatDateTime(utcNow(), 'yyyy-MM-dd')
     ```

---

## **STEP 5: Loop Platform (5 min)**

### 5.1 For Each Platform

1. **+ New step** → cerca `For each`
2. In **Select output**: *Expression →*
   ```
   createArray('facebook', 'google', 'microsoft', 'linkedin')
   ```

### 5.2 List Blobs (dentro For each)

1. **Add action** → cerca `Azure Blob Storage`
2. Scegli: **List blobs (V2)**
3. **Prima volta**: crea connessione
   - Connection name: `azureblob`
   - Storage account: `stdigitalreportsdc`
   - Access key: *copia da Portal → Storage → Access keys → key1*
4. Configura:
   - Container: `/social-pipeline-logs`
   - Folder: *Expression →*
     ```
     concat(items('For_each'), '/', variables('TodayDate'))
     ```

### 5.3 For Each Blob (dentro primo For each)

1. **Add action** → `For each`
2. **Select output**: *Dynamic content →* `value` (da List blobs)

#### 5.3.1 Get Blob Content

1. **Add action** → `Azure Blob Storage` → `Get blob content (V2)`
2. **Blob**: *Dynamic →* `Path`

#### 5.3.2 Parse JSON

1. **Add action** → `Parse JSON`
2. **Content**: *Dynamic →* `File Content`
3. **Schema**: *click "Use sample..." →* incolla:

```json
{
  "execution_id": "uuid",
  "platform": "facebook",
  "status": "success",
  "start_time": "2026-02-23T02:00:00",
  "end_time": "2026-02-23T02:15:00",
  "duration_seconds": 900,
  "tables": [],
  "tables_count": 3,
  "total_rows": 4500,
  "exit_code": 0,
  "errors": [],
  "metadata": {}
}
```

#### 5.3.3 Append to AllSummaries

1. **Add action** → `Append to array variable`
2. **Name**: `AllSummaries`
3. **Value**: *Dynamic →* `Body` (da Parse JSON)

---

## **STEP 6: Invia Email (3 min)**

### 6.1 Compose Email Body

1. **+ New step** (FUORI da tutti i For each) → `Compose`
2. **Inputs**: *incolla questo HTML base:*

```html
<html>
<body style="font-family: Arial;">
  <h1>📊 Social Pipelines Report</h1>
  <p><strong>Date:</strong> {TodayDate}</p>
  <p><strong>Platforms processed:</strong> {count}</p>

  <h2>Summary</h2>
  <ul>
    <li>Total: {total}</li>
    <li>✅ Success: {success}</li>
    <li>⚠️ Partial: {partial}</li>
    <li>❌ Failed: {failed}</li>
  </ul>

  <p><em>See Azure Portal for details</em></p>
</body>
</html>
```

**Sostituisci i placeholder** con *Dynamic content*:
- `{TodayDate}` → variabile `TodayDate`
- `{total}` → *Expression:* `length(variables('AllSummaries'))`
- Ecc.

### 6.2 Send Email

1. **+ New step** → cerca `Office 365 Outlook`
2. Scegli: **Send an email (V2)**
3. **Sign in** con account aziendale
4. Configura:
   - **To**: `tua-email@azienda.com`
   - **Subject**: *Expression:*
     ```
     concat('Social Pipelines Report - ', variables('TodayDate'))
     ```
   - **Body**: *Dynamic →* `Outputs` (da Compose)

---

## **STEP 7: Salva e Testa (2 min)**

1. Click **Save** (toolbar)
2. **Run** → **Run Trigger** → **Run**
3. Vai su **Run history** → verifica esecuzione
4. Controlla email

---

## **VERSIONE MINIMA - Solo Count**

Se vuoi partire ancora più semplice:

### Email Ultra-Semplice

Subject: `Social Pipelines - OK`

Body:
```
Pipelines eseguite: 4/4
Tutte completate con successo.

Data: {TodayDate}
```

Basta 1 solo **Compose** + **Send Email**.

---

## **NEXT STEPS**

Dopo che funziona la versione base, puoi migliorare:

1. ✨ **Tabella HTML** con dettagli per platform
2. ✨ **Conditional subject** (✅/⚠️/❌ in base a status)
3. ✨ **Alert Teams** invece di email
4. ✨ **Power BI** per dashboard storico

---

## **COSTI**

- **~€0.05/mese** (30 run × 20 actions × €0.000025)
- Praticamente **gratis** ✅

---

**Fatto! Hai la Logic App funzionante in 15 minuti.** 🚀

Per domande: controlla `LOGIC_APP_SETUP_GUIDE.md` per versione dettagliata.
