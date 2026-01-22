# Newsletter ETL - Deployment Guide

Guida per il deployment manuale del modulo Newsletter su Azure Container Apps Job.

---

## 📋 Prerequisiti

### Software richiesto

- ✅ Docker Desktop installato e running
- ✅ Azure CLI installato (`az --version`)
- ✅ Accesso ad Azure subscription (`az login`)
- ✅ Permessi su:
  - Azure Container Registry: `crcentralsdc.azurecr.io`
  - Resource Group: `rg-digitalreport-etl-prod-sdc`
  - Container App Job: `caj-newsletter-prod-sdc`

### Verifica accesso

```bash
# Login ad Azure
az login

# Verifica subscription corretta
az account show --query "{name:name, id:id}"

# Output atteso:
# {
#   "name": "ESP_Global-SPK-DataScientist",
#   "id": "2b22fb10-f6ff-4b3e-95c4-a1cbd17b096d"
# }
```

---

## 🚀 Deployment Step-by-Step

### Step 1: Login al Container Registry

```bash
az acr login --name crcentralsdc
```

✅ **Output atteso:** `Login Succeeded`

---

### Step 2: Build Docker Image

```bash
# Dalla root del progetto
docker build -f newsletter/Dockerfile -t crcentralsdc.azurecr.io/etl-newsletter:latest -t crcentralsdc.azurecr.io/etl-newsletter:v$(date +%Y%m%d-%H%M%S) .
```

**Parametri:**
- `-f newsletter/Dockerfile` → Path del Dockerfile
- `-t` → Tag dell'immagine (2 tag: latest + versioned)
- `.` → Build context (root del progetto)

**Tempo stimato:** 2-4 minuti (prima build), 30-60 sec (successive con cache)

✅ **Output atteso:** `Successfully tagged crcentralsdc.azurecr.io/etl-newsletter:latest`

---

### Step 3: Push Image su Azure Container Registry

```bash
# Push latest tag
docker push crcentralsdc.azurecr.io/etl-newsletter:latest

# Opzionale: Push anche versioned tag (per rollback)
docker push crcentralsdc.azurecr.io/etl-newsletter:v$(date +%Y%m%d-%H%M%S)
```

**Tempo stimato:** 1-3 minuti (dipende dalla connessione)

✅ **Output atteso:**
```
latest: digest: sha256:... size: 856
```

---

### Step 4: Update Container App Job

```bash
az containerapp job update --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --image crcentralsdc.azurecr.io/etl-newsletter:latest
```

**Tempo stimato:** 10-20 secondi

✅ **Output atteso:** JSON con `"provisioningState": "Succeeded"`

---

### Step 5: Verifica Deploy (Opzionale)

```bash
# Verifica immagine attuale
az containerapp job show --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --query "properties.template.containers[0].image" -o tsv

# Output atteso: crcentralsdc.azurecr.io/etl-newsletter:latest

# Verifica configurazione environment variables
az containerapp job show --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --query "properties.template.containers[0].env[].name" -o tsv
```

---

## ⚡ Quick Deploy (One-liner)

Se hai fretta, puoi eseguire tutto in un comando:

```bash
az acr login --name crcentralsdc && \
docker build -f newsletter/Dockerfile -t crcentralsdc.azurecr.io/etl-newsletter:latest . && \
docker push crcentralsdc.azurecr.io/etl-newsletter:latest && \
az containerapp job update \
  --name caj-newsletter-prod-sdc \
  --resource-group rg-digitalreport-etl-prod-sdc \
  --image crcentralsdc.azurecr.io/etl-newsletter:latest
```

⏱️ **Tempo totale:** 3-5 minuti

---

## 🧪 Test del Deploy

### Trigger manuale del job

```bash
# Avvia execution del job
az containerapp job start --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc
```

### Monitora execution

```bash
# Lista executions recenti
az containerapp job execution list --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --query "[].{Name:name, Status:properties.status, StartTime:properties.startTime}" --output table

# Visualizza logs di un'execution specifica
az containerapp job execution logs show --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --execution-name <EXECUTION_NAME>
```

---

## 🔄 Rollback a Versione Precedente

Se qualcosa va storto:

```bash
# Lista tag disponibili
az acr repository show-tags --name crcentralsdc --repository etl-newsletter --output table

# Rollback a versione specifica
az containerapp job update --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --image crcentralsdc.azurecr.io/etl-newsletter:v20260122-103045
```

---

## 📝 Checklist Pre-Deploy

Prima di ogni deploy, verifica:

- [ ] Codice testato in locale (`uv run python -m newsletter.scripts.run_pipeline --test-mode`)
- [ ] Nessun file `.env` o secrets committati per errore
- [ ] Dockerfile funziona correttamente
- [ ] Modifiche committate su Git (per tracciabilità)
- [ ] Tag di versione annotato se necessario (`git tag -a v1.0.1 -m "Release 1.0.1"`)

---

## 🐛 Troubleshooting

### Error: "Login failed"

**Problema:** Non sei loggato ad Azure o sessione scaduta

**Fix:**
```bash
az login
az account set --subscription 2b22fb10-f6ff-4b3e-95c4-a1cbd17b096d
az acr login --name crcentralsdc
```

---

### Error: "denied: access forbidden"

**Problema:** Non hai permessi sul Container Registry

**Fix:** Verifica role assignment:
```bash
az role assignment list --assignee $(az ad signed-in-user show --query id -o tsv) --scope /subscriptions/2b22fb10-f6ff-4b3e-95c4-a1cbd17b096d/resourceGroups/rg-digitalreport-etl-prod-sdc
```

Dovresti avere almeno ruolo `Contributor` o `AcrPush`.

---

### Error: "failed to solve: failed to read dockerfile"

**Problema:** Path del Dockerfile errato o non sei nella root del progetto

**Fix:**
```bash
# Verifica di essere nella root
pwd
# Output atteso: .../digital_report_etl_pipelines

# Verifica che il Dockerfile esista
ls newsletter/Dockerfile
```

---

### Error: "No such container"

**Problema:** Container App Job non trovato

**Fix:** Verifica che esista:
```bash
az containerapp job list --resource-group rg-digitalreport-etl-prod-sdc --query "[].name" -o tsv
```

---

### Build lenta o fallisce per timeout

**Problema:** Cache Docker non ottimale o connessione lenta

**Fix:**
```bash
# Pulisci build cache
docker builder prune -a

# Rebuild senza cache
docker build --no-cache -f newsletter/Dockerfile -t crcentralsdc.azurecr.io/etl-newsletter:latest .
```

---

### Job execution fallisce

**Problema:** Errori runtime nel container

**Fix:** Visualizza logs:
```bash
# Lista executions
az containerapp job execution list --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --output table

# Visualizza logs dell'execution fallita
az containerapp job logs show --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --execution-name <EXECUTION_NAME> --follow
```

**Check comuni:**
- Environment variables configurate correttamente?
- Database Vertica/Oracle raggiungibili?
- Minio storage accessibile?
- Credenziali HCTI API valide?

---

## 📊 Monitoring

### Visualizzare metriche

```bash
# Status generale del job
az containerapp job show --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --query "{Name:name, State:properties.provisioningState, Image:properties.template.containers[0].image}" --output table

# Ultime 10 executions
az containerapp job execution list --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --query "[].{Name:name, Status:properties.status, StartTime:properties.startTime, Duration:properties.template.containers[0].resources}" --output table | head -10
```

### Portale Azure

Visualizza metriche dettagliate:
1. Vai su: https://portal.azure.com
2. Cerca: `caj-newsletter-prod-sdc`
3. Sezioni utili:
   - **Overview** → Stato generale
   - **Logs** → Log stream in tempo reale
   - **Metrics** → CPU, Memory usage
   - **Executions** → Storia esecuzioni

---

## 📚 Comandi Utili

### Gestione immagini

```bash
# Lista tutte le immagini nel registry
az acr repository list --name crcentralsdc --output table

# Lista tag di etl-newsletter
az acr repository show-tags --name crcentralsdc --repository etl-newsletter --orderby time_desc --output table

# Elimina tag vecchio (cleanup)
az acr repository delete --name crcentralsdc --image etl-newsletter:v20250101-120000 --yes
```

### Gestione Container App Job

```bash
# Lista tutti i jobs nel resource group
az containerapp job list --resource-group rg-digitalreport-etl-prod-sdc --output table

# Mostra configurazione completa
az containerapp job show --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --output yaml > job-config.yaml

# Update environment variable
az containerapp job update --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --set-env-vars "S3_FOLDER=correct-images"

# Update secret
az containerapp job secret set --name caj-newsletter-prod-sdc --resource-group rg-digitalreport-etl-prod-sdc --secrets "vertica-password=NewPassword123"
```

---

## 🎯 Best Practices

### Versioning

Usa sempre 2 tag:
- `latest` → Ultima versione stabile
- `vYYYYMMDD-HHMMSS` → Versione specifica per rollback

### Testing

Prima di deployare in produzione:
1. Test locale con `--test-mode`
2. Verifica che tutto funzioni su subset di dati
3. Check dei logs per errori/warning

### Documentation

Dopo ogni deploy significativo:
1. Annota le modifiche (commit message o changelog)
2. Documenta configurazioni cambiate
3. Aggiorna questa guida se necessario

### Security

- ✅ Non committare secrets nel codice
- ✅ Usa Azure Key Vault per secrets sensibili
- ✅ Ruota credenziali periodicamente
- ✅ Limita accesso al registry e resource group

---

## 📞 Support

Per problemi non risolti con questa guida:
- **Team:** Data Science
- **Email:** datascience@esprinet.com
- **Repository:** https://github.com/alexmarinoESP/digital_report_etl_pipelines

---

## 📅 Changelog

### 2026-01-22
- ✅ Creata guida deployment manuale
- ✅ Documentati comandi per build, push, update
- ✅ Aggiunte sezioni troubleshooting e best practices
