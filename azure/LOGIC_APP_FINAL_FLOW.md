# Logic App Consumption - Flusso Completo Finale

Guida definitiva step-by-step per creare la Logic App che aggrega i report delle pipeline social.

**IMPORTANTE**: Questa versione prende **SOLO l'ultimo blob** per ogni platform (in caso di re-run multipli nello stesso giorno).

---

## **📋 STRUTTURA FINALE**

```
1. Trigger: Recurrence (ogni giorno 02:45)
   │
2. Initialize variable: AllSummaries (array)
   │
3. Initialize variable: TodayDate (string)
   │
4. For each platform ['facebook', 'google', 'microsoft', 'linkedin']
   │
   ├─→ 5. List blobs (V2)
   │      folder: social-pipeline-logs/{platform}/{date}
   │
   ├─→ 6. Condition: blobs exist?
       │
       ├─→ TRUE Branch:
       │   │
       │   ├─→ 7. Compose: Get_Latest_Blob
       │   │      expression: last(body('List_blobs_(V2)')?['value'])
       │   │
       │   ├─→ 8. Get blob content (V2)
       │   │      blob: Outputs/Id (da Get_Latest_Blob)
       │   │
       │   ├─→ 9. Parse JSON
       │   │      content: File Content
       │   │
       │   └─→ 10. Append to AllSummaries
       │          value: Body (da Parse JSON)
       │
       └─→ FALSE Branch:
           └─→ (vuoto - opzionale: log missing platform)

11. Compose: Build_Email_HTML
    │
12. Send email (V2)
    │
END
```

---

## **STEP-BY-STEP COMPLETO**

---

### **STEP 1: Trigger - Recurrence**

1. Logic App Designer → cerca `Recurrence`
2. Configura:
   - **Interval**: `1`
   - **Frequency**: `Day`
   - Click **Add new parameter** → spunta:
     - ✅ Time zone
     - ✅ At these hours
     - ✅ At these minutes
   - **Time zone**: `(UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna`
   - **At these hours**: `2`
   - **At these minutes**: `45`

---

### **STEP 2: Initialize Variable - AllSummaries**

1. **+ New step** → cerca `Initialize variable`
2. Configura:
   - **Name**: `AllSummaries`
   - **Type**: `Array`
   - **Value**: *lascia vuoto*

---

### **STEP 3: Initialize Variable - TodayDate**

1. **+ New step** → `Initialize variable`
2. Configura:
   - **Name**: `TodayDate`
   - **Type**: `String`
   - **Value**: Click casella → tab **Expression** → copia:
     ```
     formatDateTime(utcNow(), 'yyyy-MM-dd')
     ```
   - Click **OK**

---

### **STEP 4: For Each Platform**

1. **+ New step** → cerca `For each`
2. Rinomina in: `For_each_platform` (click sui 3 puntini → Rename)
3. **Select an output from previous steps**:
   - Click casella → tab **Expression** → copia:
     ```
     createArray('facebook', 'google', 'microsoft', 'linkedin')
     ```
   - Click **OK**

---

### **STEP 5: List Blobs (V2)** *(dentro For each)*

1. Dentro "For_each_platform" → **Add an action**
2. Cerca: `Azure Blob Storage`
3. Scegli: **List blobs (V2)**

4. **Se prima volta** - Crea connessione:
   - **Connection name**: `azureblob-pipeline-logs`
   - **Authentication type**: `Access Key`
   - **Azure Storage Account name**: `stdigitalreportsdc`
   - **Azure Storage Account Access Key**:
     - Portal → Storage `stdigitalreportsdc` → Access keys → **key1** → Show → Copia
   - Click **Create**

5. Configura List blobs:
   - **Storage account name**: `stdigitalreportsdc`
   - **Container name**: `/social-pipeline-logs`
   - **Folder path**: Click casella → tab **Expression** → copia:
     ```
     concat('social-pipeline-logs/', items('For_each_platform'), '/', variables('TodayDate'))
     ```
     - Click **OK**
   - **Paging Marker**: *lascia vuoto*
   - **Flat Listing**: `No`

---

### **STEP 6: Condition - Check if blobs exist** *(dentro For each)*

1. Sotto "List blobs (V2)" → **Add an action**
2. Cerca: `Condition`

3. Configura condition:

   **Prima casella (sinistra)**:
   - Click → tab **Dynamic content**
   - Cerca: `List of Files Id` (oppure `value`)
   - Selezionalo

   **Operatore (centro)**:
   - `is not equal to`

   **Terza casella (destra)**:
   - Click → tab **Expression**
   - Scrivi: `null`
   - Click **OK**

---

### **STEP 7: Compose - Get Latest Blob** *(dentro True branch)*

1. Nel ramo **True** della Condition → **Add an action**
2. Cerca: `Compose`
3. Rinomina in: `Get_Latest_Blob` (3 puntini → Rename)
4. **Inputs**: Click → tab **Expression** → copia:
   ```
   last(body('List_blobs_(V2)')?['value'])
   ```
   - Click **OK**

**Cosa fa**: Prende l'**ultimo blob** della lista (il più recente, in caso di re-run multipli).

---

### **STEP 8: Get Blob Content (V2)** *(dopo Get_Latest_Blob)*

1. **Add an action** → `Azure Blob Storage` → `Get blob content (V2)`
2. Configura:
   - **Storage account name**: `stdigitalreportsdc`
   - **Blob**: Click casella → segui questi sub-step:
     1. Tab **Dynamic content**
     2. Seleziona `Outputs` (da "Get_Latest_Blob")
     3. **IMPORTANTE**: Nella casella, dopo `Outputs`, aggiungi manualmente:
        ```
        ?['Id']
        ```
        Risultato finale: `outputs('Get_Latest_Blob')?['Id']`

     **OPPURE** usa Expression:
     - Tab **Expression** → copia:
       ```
       outputs('Get_Latest_Blob')?['Id']
       ```

---

### **STEP 9: Parse JSON** *(dopo Get blob content)*

1. **Add an action** → `Parse JSON`
2. Configura:
   - **Content**: Tab **Dynamic content** → `File Content` (da "Get blob content (V2)")

   - **Schema**: Click **Use sample payload to generate schema** → incolla:

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

   - Click **Done**

---

### **STEP 10: Append to AllSummaries** *(dopo Parse JSON)*

1. **Add an action** → `Append to array variable`
2. Configura:
   - **Name**: `AllSummaries`
   - **Value**: Tab **Dynamic content** → `Body` (da "Parse JSON")

---

### **STEP 11: Compose - Build Email HTML** *(FUORI dal For each principale)*

1. **+ New step** (assicurati di essere **FUORI** dal "For_each_platform")
2. Cerca: `Compose`
3. Rinomina in: `Build_Email_HTML`
4. **Inputs**: Copia/incolla questo HTML:

```html
<html>
<head>
<style>
body {
  font-family: 'Segoe UI', Arial, sans-serif;
  padding: 20px;
  background-color: #f5f5f5;
}
.container {
  max-width: 800px;
  margin: 0 auto;
  background: white;
  padding: 30px;
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}
h1 {
  color: #0078d4;
  border-bottom: 3px solid #0078d4;
  padding-bottom: 10px;
}
.summary {
  background: #f0f0f0;
  padding: 15px;
  border-radius: 4px;
  margin: 20px 0;
}
.stat {
  display: inline-block;
  margin-right: 20px;
  font-size: 16px;
}
.stat .number {
  font-size: 24px;
  font-weight: bold;
  color: #0078d4;
}
table {
  border-collapse: collapse;
  width: 100%;
  margin-top: 20px;
}
th, td {
  border: 1px solid #ddd;
  padding: 12px;
  text-align: left;
}
th {
  background-color: #0078d4;
  color: white;
  font-weight: 600;
}
tr:hover {
  background-color: #f5f5f5;
}
.badge {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 12px;
  font-size: 12px;
  font-weight: bold;
}
.badge.success { background-color: #28a745; color: white; }
.badge.partial { background-color: #ffc107; color: #333; }
.badge.failed { background-color: #dc3545; color: white; }
</style>
</head>
<body>
<div class="container">
  <h1>📊 Social Pipelines Daily Report</h1>

  <p><strong>Date:</strong> DATEPLACEHOLDER</p>

  <div class="summary">
    <h2>Execution Summary</h2>
    <div class="stat">
      <div class="number">TOTALPLACEHOLDER</div>
      <div>Total Platforms</div>
    </div>
    <div class="stat">
      <div class="number" style="color: #28a745;">SUCCESSPLACEHOLDER</div>
      <div>✅ Success</div>
    </div>
    <div class="stat">
      <div class="number" style="color: #ffc107;">PARTIALPLACEHOLDER</div>
      <div>⚠️ Partial</div>
    </div>
    <div class="stat">
      <div class="number" style="color: #dc3545;">FAILEDPLACEHOLDER</div>
      <div>❌ Failed</div>
    </div>
  </div>

  <h2>Platform Details</h2>
  <table>
    <thead>
      <tr>
        <th>Platform</th>
        <th>Status</th>
        <th>Tables</th>
        <th>Total Rows</th>
        <th>Duration (s)</th>
        <th>Exit Code</th>
      </tr>
    </thead>
    <tbody>
      TABLEROWSPLACEHOLDER
    </tbody>
  </table>

  <p style="margin-top: 30px; color: #666; font-size: 12px;">
    <em>For detailed logs, check Azure Portal → Container App Jobs</em>
  </p>
</div>
</body>
</html>
```

5. Ora **sostituisci i PLACEHOLDER** con Dynamic Content/Expressions:

   **DATEPLACEHOLDER**:
   - Seleziona il testo → tab **Dynamic content** → `TodayDate`

   **TOTALPLACEHOLDER**:
   - Seleziona il testo → tab **Expression** → copia:
     ```
     length(variables('AllSummaries'))
     ```

   **SUCCESSPLACEHOLDER**:
   - Seleziona il testo → tab **Expression** → copia:
     ```
     length(filter(variables('AllSummaries'), item => equals(item['status'], 'success')))
     ```

   **PARTIALPLACEHOLDER**:
   - Seleziona il testo → tab **Expression** → copia:
     ```
     length(filter(variables('AllSummaries'), item => equals(item['status'], 'partial_success')))
     ```

   **FAILEDPLACEHOLDER**:
   - Seleziona il testo → tab **Expression** → copia:
     ```
     length(filter(variables('AllSummaries'), item => equals(item['status'], 'failed')))
     ```

   **TABLEROWSPLACEHOLDER**:
   - Per ora lascia così (oppure metti testo statico "See Azure Portal for details")
   - **NOTA**: Per generare righe dinamiche serve un altro For each (versione avanzata - opzionale)

---

### **STEP 12: Send Email (V2)** *(dopo Compose)*

1. **+ New step** → cerca `Office 365 Outlook`
2. Scegli: **Send an email (V2)**
3. **Sign in** con account aziendale Microsoft (se richiesto)
4. Configura:

   **To**:
   - `tua-email@azienda.com` (sostituisci con email vera)

   **Subject**:
   - Click casella → tab **Expression** → copia:
     ```
     concat('📊 Social Pipelines Report - ', variables('TodayDate'))
     ```

   **Body**:
   - Tab **Dynamic content** → seleziona `Outputs` (da "Build_Email_HTML")

   **Importance**:
   - Lascia `Normal`
   - **OPPURE** usa expression condizionale:
     - Tab **Expression** → copia:
       ```
       if(greater(length(filter(variables('AllSummaries'), item => equals(item['status'], 'failed'))), 0), 'High', 'Normal')
       ```

---

### **STEP 13: Salva**

1. Click **Save** (toolbar in alto)
2. Aspetta conferma "Your logic app has been saved successfully"

---

## **🧪 TEST**

### Test Manuale

1. **Overview** → **Run Trigger** → **Run**
2. Vai su **Run history** → apri l'ultima run
3. Verifica ogni step:
   - ✅ Recurrence trigger
   - ✅ Initialize variables
   - ✅ For each platform (4 iterazioni)
   - ✅ List blobs per ogni platform
   - ✅ Condition (se blob trovato)
   - ✅ Get_Latest_Blob (prende ultimo)
   - ✅ Get blob content
   - ✅ Parse JSON
   - ✅ Append to AllSummaries
   - ✅ Build_Email_HTML
   - ✅ Send email

4. **Check inbox** → verifica email ricevuta

---

## **📝 NOTA IMPORTANTE: Tabella Dinamica (Opzionale)**

L'HTML sopra ha `TABLEROWSPLACEHOLDER` statico. Per popolare la tabella **dinamicamente** con i dati di ogni platform:

**Aggiungi tra STEP 11 e 12**:

1. **+ New step** → `Initialize variable`
   - Name: `TableRows`
   - Type: `String`
   - Value: vuoto

2. **+ New step** → `For each`
   - Select output: `AllSummaries` variable
   - Dentro:
     - **Append to string variable** → `TableRows`
     - Value (Expression):
       ```
       concat(
         '<tr>',
         '<td><strong>', items('For_each_2')['platform'], '</strong></td>',
         '<td><span class="badge ', items('For_each_2')['status'], '">', items('For_each_2')['status'], '</span></td>',
         '<td>', string(items('For_each_2')['tables_count']), '</td>',
         '<td>', string(items('For_each_2')['total_rows']), '</td>',
         '<td>', string(items('For_each_2')['duration_seconds']), '</td>',
         '<td>', string(items('For_each_2')['exit_code']), '</td>',
         '</tr>'
       )
       ```

3. Poi in "Build_Email_HTML", sostituisci `TABLEROWSPLACEHOLDER` con variabile `TableRows`

**Ma per iniziare, la versione base sopra è sufficiente!** ✅

---

## **🐛 TROUBLESHOOTING**

| Problema | Soluzione |
|----------|-----------|
| "List blobs" fallisce | Verifica connection e container `social-pipeline-logs` esiste |
| "Get_Latest_Blob" error | Verifica expression: `last(body('List_blobs_(V2)')?['value'])` |
| "Get blob content" - blob not found | Verifica expression blob: `outputs('Get_Latest_Blob')?['Id']` |
| Parse JSON schema error | Usa blob vero per generare schema |
| Email body vuoto | Verifica "Outputs" da Build_Email_HTML selezionato |
| Expression invalid | Verifica sintassi (virgolette, parentesi) |

---

## **✅ CHECKLIST FINALE**

- [ ] Trigger Recurrence configurato (02:45)
- [ ] Variabili inizializzate (AllSummaries, TodayDate)
- [ ] For each platform corretto (4 platforms)
- [ ] List blobs con path corretto
- [ ] Condition per check blob existence
- [ ] Get_Latest_Blob con `last()` expression
- [ ] Get blob content con Id corretto
- [ ] Parse JSON con schema
- [ ] Append to AllSummaries
- [ ] Build_Email_HTML con placeholder sostituiti
- [ ] Send email configurato
- [ ] Logic App salvata
- [ ] Test eseguito con successo
- [ ] Email ricevuta

---

**La tua Logic App è pronta!** 🎉

Ogni notte alle 02:45 aggregherà automaticamente i summary e invierà l'email di report.
