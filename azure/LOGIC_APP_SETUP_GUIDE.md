# Logic App Setup Guide - Social Pipeline Report

Guida completa per creare la Logic App che aggrega i summary delle pipeline e invia email di report.

---

## **PARTE 1: Crea Logic App**

### 1.1 Crea risorsa Logic App

1. **Azure Portal** → **Logic Apps** → **+ Create**
2. **Basics**:
   - Subscription: *la tua*
   - Resource group: *stesso dei Container Apps*
   - **Logic App name**: `logic-social-pipeline-report`
   - **Region**: *stessa dei Container Apps*
   - **Plan type**: **Consumption** (pay-per-execution)
3. **Review + Create** → **Create**
4. Aspetta il deploy, poi **Go to resource**

---

## **PARTE 2: Configura Workflow**

### 2.1 Apri Logic App Designer

1. Nella Logic App → **Logic app designer** (menu a sinistra)
2. Scegli **Blank Logic App** (template vuoto)

---

### 2.2 STEP 1: Trigger - Recurrence (Schedulato ogni notte)

1. Cerca: **Recurrence**
2. Click su **Recurrence** trigger
3. Configura:
   - **Interval**: `1`
   - **Frequency**: `Day`
   - Click **Add new parameter** → spunta **Time zone** e **At these hours**
   - **Time zone**: `(UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna`
   - **At these hours**: `2` (alle 2:45 AM ogni notte, dopo che i job sono finiti)
   - **At these minutes**: `45`

**IMPORTANTE**: La schedule parte alle 02:45 per dare buffer ai Container Jobs (che finiscono ~02:30).

---

### 2.3 STEP 2: Inizializza Variabili

#### **2.3.1 Variabile AllSummaries (array)**

1. Click **+ New step**
2. Cerca: **Initialize variable**
3. Configura:
   - **Name**: `AllSummaries`
   - **Type**: `Array`
   - **Value**: lascia vuoto `[]`

#### **2.3.2 Variabile TodayDate (string)**

1. Click **+ New step**
2. Cerca: **Initialize variable**
3. Configura:
   - **Name**: `TodayDate`
   - **Type**: `String`
   - **Value**: Click nella casella → **Expression** → inserisci:
     ```
     formatDateTime(utcNow(), 'yyyy-MM-dd')
     ```
   - Click **OK**

#### **2.3.3 Variabile MissingPlatforms (array)**

1. Click **+ New step**
2. Cerca: **Initialize variable**
3. Configura:
   - **Name**: `MissingPlatforms`
   - **Type**: `Array`
   - **Value**: lascia vuoto `[]`

---

### 2.4 STEP 3: Loop per Ogni Platform

1. Click **+ New step**
2. Cerca: **For each**
3. In **Select an output from previous steps**: Click → **Expression** → inserisci:
   ```
   createArray('facebook', 'google', 'microsoft', 'linkedin')
   ```
4. Click **OK**

**Dentro il For each**, aggiungi questi step:

#### **2.4.1 List Blobs per Platform**

1. **Add an action** (dentro For each)
2. Cerca: **Azure Blob Storage**
3. Scegli: **List blobs (V2)**
4. **Connection**:
   - **Connection name**: `azureblob-pipeline-logs`
   - **Authentication type**: **Access key**
   - **Storage Account name**: `stdigitalreportsdc`
   - **Shared Storage Key**: *Vai su Storage Account → Access keys → Copia key1*
   - Click **Create**
5. Configura:
   - **Storage account name**: seleziona `stdigitalreportsdc`
   - **Container name**: `/social-pipeline-logs`
   - **Folder path**: Click → **Expression** → inserisci:
     ```
     concat(items('For_each'), '/', variables('TodayDate'))
     ```
   - Click **OK**

#### **2.4.2 Condition - Check if blobs found**

1. **Add an action**
2. Cerca: **Condition**
3. Configura la condizione:
   - Click nella prima casella → **Dynamic content** → cerca `value` da "List blobs"
   - Operatore: `is not equal to`
   - Click nella terza casella → **Expression** → inserisci: `null`
   - Click **OK**

**Nel ramo "True"** (blobs trovati):

##### **2.4.2.1 For each blob trovato**

1. **Add an action**
2. Cerca: **For each**
3. In **Select an output from previous steps**: seleziona **value** da "List blobs (V2)"

**Dentro questo For each interno**:

##### **2.4.2.1.1 Get blob content**

1. **Add an action**
2. Cerca: **Azure Blob Storage** → **Get blob content (V2)**
3. Configura:
   - **Storage account**: `stdigitalreportsdc`
   - **Blob**: Click → **Dynamic content** → seleziona **Path** da "List blobs (V2)"

##### **2.4.2.1.2 Parse JSON**

1. **Add an action**
2. Cerca: **Parse JSON**
3. Configura:
   - **Content**: seleziona **File Content** da "Get blob content (V2)"
   - **Schema**: Click **Use sample payload to generate schema** → incolla questo JSON:

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
      "column_names": ["date", "campaign_id", "impressions"]
    }
  ],
  "tables_count": 3,
  "total_rows": 4500,
  "exit_code": 0,
  "errors": [],
  "metadata": {
    "account_ids": ["act_123456"],
    "tables_count": 3
  }
}
```

4. Click **Done**

##### **2.4.2.1.3 Append to AllSummaries**

1. **Add an action**
2. Cerca: **Append to array variable**
3. Configura:
   - **Name**: `AllSummaries`
   - **Value**: seleziona **Body** da "Parse JSON"

**Nel ramo "False"** (nessun blob trovato):

##### **2.4.2.2 Append to MissingPlatforms**

1. **Add an action**
2. Cerca: **Append to array variable**
3. Configura:
   - **Name**: `MissingPlatforms`
   - **Value**: Click → **Expression** → inserisci:
     ```
     items('For_each')
     ```
   - Click **OK**

---

### 2.5 STEP 4: Build Email Report

1. Click **+ New step** (FUORI dal For each principale)
2. Cerca: **Compose**
3. Rinomina action in: `Build_Email_Report`
4. In **Inputs**: Click → **Expression** → inserisci:

```javascript
concat(
  '<html><head><style>',
  'body { font-family: Arial, sans-serif; margin: 20px; }',
  'h1 { color: #0078d4; }',
  'table { border-collapse: collapse; width: 100%; margin-top: 20px; }',
  'th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }',
  'th { background-color: #0078d4; color: white; }',
  '.success { background-color: #d4edda; }',
  '.warning { background-color: #fff3cd; }',
  '.error { background-color: #f8d7da; }',
  '</style></head><body>',
  '<h1>📊 Social Pipelines Daily Report</h1>',
  '<p><strong>Date:</strong> ', variables('TodayDate'), '</p>',
  '<p><strong>Execution Summary:</strong></p>',
  '<ul>',
  '<li>Total platforms: ', string(length(variables('AllSummaries'))), '</li>',
  '<li>✅ Successful: ', string(length(where(variables('AllSummaries'), item()['status'], 'success'))), '</li>',
  '<li>⚠️ Partial: ', string(length(where(variables('AllSummaries'), item()['status'], 'partial_success'))), '</li>',
  '<li>❌ Failed: ', string(length(where(variables('AllSummaries'), item()['status'], 'failed'))), '</li>',
  if(greater(length(variables('MissingPlatforms')), 0),
    concat('<li>🔍 Missing: ', string(length(variables('MissingPlatforms'))), ' (', join(variables('MissingPlatforms'), ', '), ')</li>'),
    ''
  ),
  '</ul>',
  '<table><thead><tr><th>Platform</th><th>Status</th><th>Tables</th><th>Rows</th><th>Duration</th><th>Exit Code</th></tr></thead><tbody>',
  '<!-- Platform rows will be added here -->',
  '</tbody></table>',
  '</body></html>'
)
```

**NOTA**: Questo è un esempio semplificato. Per generare righe dinamiche della tabella serve un altro For each. Vedi sotto per versione completa.

---

### 2.6 STEP 5: Send Email

1. Click **+ New step**
2. Cerca: **Office 365 Outlook**
3. Scegli: **Send an email (V2)**
4. **Sign in** con il tuo account aziendale
5. Configura:
   - **To**: `il-tuo-email@azienda.com`
   - **Subject**: Click → **Expression** → inserisci:
     ```
     concat(
       if(equals(length(where(variables('AllSummaries'), item()['status'], 'failed')), 0),
         if(equals(length(where(variables('AllSummaries'), item()['status'], 'partial_success')), 0),
           '✅ All Success',
           '⚠️ Partial Success'
         ),
         '❌ Failures Detected'
       ),
       ' - Social Pipelines Report - ',
       variables('TodayDate')
     )
     ```
   - **Body**: seleziona **Outputs** da "Build_Email_Report"
   - **Importance**: Click → **Expression** → inserisci:
     ```
     if(greater(length(where(variables('AllSummaries'), item()['status'], 'failed')), 0), 'High', 'Normal')
     ```

---

### 2.7 SALVA Logic App

1. Click **Save** (toolbar in alto)
2. Aspetta conferma "Your logic app has been saved"

---

## **PARTE 3: Test**

### 3.1 Test Manuale

1. Nella Logic App → **Overview**
2. Click **Run Trigger** → **Run**
3. Vai su **Runs history** → apri l'ultima esecuzione
4. Verifica che:
   - ✅ Trigger parte
   - ✅ Variabili inizializzate
   - ✅ Blobs letti per ogni platform
   - ✅ Email inviata

### 3.2 Check Email

1. Controlla la inbox dell'email configurata
2. Dovresti ricevere il report con:
   - ✅ Subject con status (✅/⚠️/❌)
   - ✅ HTML body con tabella riepilogativa

---

## **TROUBLESHOOTING**

### Problema: "List blobs" fallisce
**Soluzione**: Verifica che il container `social-pipeline-logs` esista e che la connessione Blob Storage sia corretta.

### Problema: Nessun blob trovato
**Soluzione**: Verifica che i Container Jobs abbiano scritto i summary. Controlla i logs dei jobs e la env var `SUMMARY_STORAGE_CONNECTION_STRING`.

### Problema: Email non arriva
**Soluzione**:
1. Verifica che la connessione Office 365 sia autenticata
2. Controlla la cartella Spam
3. Verifica che l'email recipient sia corretta

### Problema: Parse JSON fallisce
**Soluzione**: Verifica che lo schema JSON sia corretto. Usa un summary effettivo dal blob storage per generare lo schema.

---

## **OTTIMIZZAZIONI FUTURE**

### 1. Alert su Teams invece di Email
Sostituisci "Send an email" con "Post message in Teams channel"

### 2. Retention automatica dei blob
Già configurata con Lifecycle Policy (90 giorni)

### 3. Dashboard Power BI
Leggi i blob JSON direttamente in Power BI per visualizzazioni avanzate

---

## **COSTI STIMATI**

- **Logic App Consumption**: ~€0.05/mese (30 esecuzioni × €0.000025/action × 60 actions)
- **Blob Storage transactions**: ~€0.01/mese
- **TOTALE**: **~€0.06/mese**

---

**Fine guida! La Logic App è pronta per aggregare i report notturni.** 🚀
