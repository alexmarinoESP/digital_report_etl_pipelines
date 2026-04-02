# Upload Images Service

Servizio FastAPI per il caricamento di immagini delle campagne social su Azure Blob Storage.

Migrato da `reportdigital-api` (che usava MinIO) mantenendo la stessa struttura e API per compatibilità con CRM.

## Struttura Storage

Mantiene la stessa struttura di MinIO:
```
Container: report-digital
  └── {campaign_code}/
      ├── {activity_code}.png
      ├── {activity_code}_1.png
      ├── {activity_code}_2.png
      └── ...
```

## API Endpoints

### POST /uploadfile

Upload di un'immagine per una campagna.

**Parametri:**
- `activity_code` (query): Codice activity in formato "CAMP.ACT" o "CAMP"
- `my_file` (form-data): File immagine (PNG/JPG)

**Response:**
```json
{
  "message": "File uploaded successfully: campaign/activity.png",
  "status_code": 200
}
```

**Esempio:**
```bash
curl -X POST "http://localhost:8000/uploadfile?activity_code=CAMP001.ACT001" \
  -F "my_file=@image.png"
```

### GET /health

Health check del servizio.

## Variabili d'Ambiente

| Variabile | Descrizione | Obbligatorio |
|-----------|-------------|--------------|
| `AZURE_STORAGE_CONNECTION_STRING` | Connection string Azure Storage | Sì |
| `SENTRY_DSN` | Sentry DSN per monitoring | No |
| `ENVIRONMENT` | Ambiente (production/staging/dev) | No |
| `LOG_LEVEL` | Livello di log (INFO/DEBUG/WARNING) | No |

## Sviluppo Locale

### Setup

1. Installa dipendenze:
```bash
pip install -r requirements.txt
```

2. Configura variabili d'ambiente:
```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
```

3. Run server:
```bash
uvicorn social.upload_images.main:app --reload --port 8000
```

### Test

```bash
# Health check
curl http://localhost:8000/health

# Upload test
curl -X POST "http://localhost:8000/uploadfile?activity_code=TEST001.ACT001" \
  -F "my_file=@test_image.png"
```

## Deployment su Azure

### Build Docker Image

```bash
cd social/upload_images
docker build -t upload-images:latest .
```

### Push to Azure Container Registry

```bash
# Login
az acr login --name {ACR_NAME}

# Tag
docker tag upload-images:latest {ACR_NAME}.azurecr.io/upload-images:latest

# Push
docker push {ACR_NAME}.azurecr.io/upload-images:latest
```

### Deploy Container App

```bash
# Create/Update container app
az containerapp create \
  --name upload-images \
  --resource-group {RESOURCE_GROUP} \
  --environment {ENVIRONMENT_NAME} \
  --image {ACR_NAME}.azurecr.io/upload-images:latest \
  --target-port 80 \
  --ingress external \
  --env-vars \
    AZURE_STORAGE_CONNECTION_STRING=secretref:azure-storage-connection-string \
    ENVIRONMENT=production
```

Oppure usando il file YAML:
```bash
az containerapp create --yaml azure-deploy.yml
```

## Differenze con MinIO

| Aspetto | MinIO (vecchio) | Azure Blob (nuovo) |
|---------|----------------|-------------------|
| Storage | MinIO S3-compatible | Azure Blob Storage |
| Client | `minio` Python SDK | `azure-storage-blob` |
| SSL | Custom SSL handling | Managed by Azure |
| Struttura | Identica | Identica |
| API | Identica | Identica |

## Logica Progressive Numbering

Se un'immagine con lo stesso `activity_code` esiste già:
1. Cerca tutte le immagini con quel codice
2. Estrae i numeri progressivi (es: `ACT001_1`, `ACT001_2`)
3. Usa il massimo + 1 come nuovo numero
4. Salva come `{activity_code}_{next_number}.{ext}`

## Monitoring

Il servizio include:
- Health check endpoint (`/health`)
- Logging con Loguru
- Sentry integration (opzionale)
- Metrics via container app insights

## Note

- API mantiene 100% compatibilità con versione MinIO
- CRM non deve modificare nulla nelle chiamate
- Stessa logica di progressive numbering
- Stessa struttura cartelle e nomi file
