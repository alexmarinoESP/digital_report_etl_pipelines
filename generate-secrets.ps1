# Script PowerShell per generare GitHub Secrets in formato base64
# Esegui questo script per ottenere i valori da inserire come secrets su GitHub
# https://github.com/alexmarinoESP/digital_report_etl_pipelines/settings/secrets/actions

Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host "GENERAZIONE GITHUB SECRETS" -ForegroundColor Green
Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host ""

function ConvertTo-Base64 {
    param([string]$FilePath, [string]$SecretName)

    if (-not (Test-Path $FilePath)) {
        Write-Host "⚠️  File not found: $FilePath" -ForegroundColor Yellow
        return
    }

    $content = Get-Content $FilePath -Raw -Encoding UTF8
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($content)
    $base64 = [Convert]::ToBase64String($bytes)

    Write-Host "✅ $SecretName" -ForegroundColor Green
    Write-Host "-" * 80 -ForegroundColor Gray
    Write-Host $base64
    Write-Host ""
}

# Google Ads configs
Write-Host "`n📁 GOOGLE ADS CONFIGURATIONS" -ForegroundColor Cyan
Write-Host "-" * 80 -ForegroundColor Gray
ConvertTo-Base64 "social/platforms/google/google-ads-9474097201.yml" "GOOGLE_ADS_CONFIG_9474097201"
ConvertTo-Base64 "social/platforms/google/google-ads-4619434319.yml" "GOOGLE_ADS_CONFIG_4619434319"

# Microsoft tokens
Write-Host "`n📁 MICROSOFT ADS TOKENS" -ForegroundColor Cyan
Write-Host "-" * 80 -ForegroundColor Gray
ConvertTo-Base64 "social/platforms/microsoft/tokens.json" "MICROSOFT_TOKENS"

# Credentials
Write-Host "`n📁 MULTI-PLATFORM CREDENTIALS" -ForegroundColor Cyan
Write-Host "-" * 80 -ForegroundColor Gray
ConvertTo-Base64 "social/config/credentials.yml" "CREDENTIALS_YML"

Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host "ISTRUZIONI:" -ForegroundColor Yellow
Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Vai su: https://github.com/alexmarinoESP/digital_report_etl_pipelines/settings/secrets/actions" -ForegroundColor White
Write-Host "2. Clicca 'New repository secret'" -ForegroundColor White
Write-Host "3. Per ogni secret sopra:" -ForegroundColor White
Write-Host "   - Name: il nome mostrato (es. GOOGLE_ADS_CONFIG_9474097201)" -ForegroundColor White
Write-Host "   - Value: il valore base64 mostrato sotto il nome" -ForegroundColor White
Write-Host "4. Salva tutti i secrets" -ForegroundColor White
Write-Host ""
Write-Host "Dopo aver aggiunto tutti i secrets, i workflow GitHub Actions funzioneranno!" -ForegroundColor Green
Write-Host ""
