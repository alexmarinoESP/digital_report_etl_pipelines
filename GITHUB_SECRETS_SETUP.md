# Setup GitHub Secrets per Deploy Automatico

Questa guida spiega come configurare i GitHub Secrets necessari per i workflow automatici di build e deploy.

## 📋 Problema Risolto

I file di configurazione con credenziali sensibili (Google Ads configs, tokens OAuth, ecc.) sono in `.gitignore` e non vengono tracciati da Git per sicurezza.

Questo causava fallimenti nei build automatici su GitHub Actions perché i file non erano disponibili durante il build.

**Soluzione**: Salvare i contenuti dei file come GitHub Secrets (criptati) e decodificarli prima del build.

---

## 🔧 Setup Passo-Passo

### **Step 1: Generare i Secrets Base64**

Esegui lo script PowerShell per ottenere i valori base64:

```powershell
.\generate-secrets.ps1
```

Lo script mostrerà 4 secrets in formato base64. **Copia i valori** (non condividerli!).

---

### **Step 2: Aggiungere i Secrets su GitHub**

1. Vai su: https://github.com/alexmarinoESP/digital_report_etl_pipelines/settings/secrets/actions

2. Clicca **"New repository secret"** per ogni secret:

#### **Secret 1: `GOOGLE_ADS_CONFIG_9474097201`**
- **Name**: `GOOGLE_ADS_CONFIG_9474097201`
- **Value**: Il valore base64 del file `google-ads-9474097201.yml`

#### **Secret 2: `GOOGLE_ADS_CONFIG_4619434319`**
- **Name**: `GOOGLE_ADS_CONFIG_4619434319`
- **Value**: Il valore base64 del file `google-ads-4619434319.yml`

#### **Secret 3: `MICROSOFT_TOKENS`**
- **Name**: `MICROSOFT_TOKENS`
- **Value**: Il valore base64 del file `tokens.json` (Microsoft)

#### **Secret 4: `CREDENTIALS_YML`**
- **Name**: `CREDENTIALS_YML`
- **Value**: Il valore base64 del file `credentials.yml` (Facebook/LinkedIn/Google)

---

### **Step 3: Verificare i Secrets Esistenti**

Assicurati di avere anche questi secrets (già configurati in precedenza):

- ✅ `AZURE_CREDENTIALS` - Credenziali Service Principal Azure
- ✅ `AZURE_SUBSCRIPTION_ID` - ID Subscription Azure
- ✅ `ACR_LOGIN_SERVER` - Server ACR (`crcentralsdc.azurecr.io`)
- ✅ `ACR_USERNAME` - Username ACR
- ✅ `ACR_PASSWORD` - Password ACR

---

## ✅ Test dei Workflow

Dopo aver aggiunto i secrets:

### **Test Manuale**

1. Vai su: https://github.com/alexmarinoESP/digital_report_etl_pipelines/actions
2. Seleziona un workflow (es. "Deploy Facebook Ads to Azure")
3. Clicca **"Run workflow"** → **"Run workflow"**
4. Verifica che il build completi con successo

### **Test Automatico**

1. Fai una piccola modifica a un file della piattaforma:
   ```bash
   echo "# Test" >> social/platforms/facebook/README.md
   git add social/platforms/facebook/README.md
   git commit -m "test: trigger workflow"
   git push origin main
   ```
2. Il workflow `Deploy Facebook Ads to Azure` dovrebbe partire automaticamente

---

## 📁 File Template

I file template (senza credenziali) sono committati nel repository per riferimento:

- `social/platforms/google/google-ads.template.yml`
- `social/config/credentials.template.yml`
- `social/platforms/microsoft/tokens.template.json`

**NON** modificare i file originali (`google-ads-9474097201.yml`, `credentials.yml`, `tokens.json`), rimangono in `.gitignore`.

---

## 🔄 Come Funziona

Quando un workflow parte:

1. **Checkout** del codice (senza file sensibili)
2. **Decodifica secrets** da base64 → crea file sensibili
3. **Build Docker** (ora i file ci sono!)
4. **Push** su Azure Container Registry
5. **Update** del Container App Job su Azure

---

## 🔒 Sicurezza

✅ I file sensibili **NON** sono nel repository Git
✅ I secrets sono **criptati** da GitHub
✅ I secrets sono **visibili solo ai maintainer** del repo
✅ I file vengono **ricreati ad ogni build** e poi scartati

---

## 🆘 Troubleshooting

### **Errore: "invalid base64 data"**
- Verifica che il valore copiato sia completo (nessuno spazio o a capo)
- Ri-genera il base64 con `generate-secrets.ps1`

### **Errore: "File not found" durante build**
- Verifica che il nome del secret sia **esatto** (case-sensitive)
- Controlla che il secret sia stato salvato correttamente

### **Workflow non parte automaticamente**
- Verifica che i path trigger siano corretti
- Controlla che il push sia sul branch `main`

---

## 📝 Manutenzione

Quando aggiorni le credenziali:

1. Modifica il file locale (es. `credentials.yml`)
2. Ri-genera il base64: `.\generate-secrets.ps1`
3. Aggiorna il secret corrispondente su GitHub
4. Il prossimo build userà le nuove credenziali
