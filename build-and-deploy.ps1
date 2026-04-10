# Script PowerShell per build e deploy delle immagini Docker su Azure
# Questo script builda le immagini LOCALMENTE (includendo file sensibili)
# e le pusha su Azure Container Registry

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("all", "google", "linkedin", "facebook", "microsoft")]
    [string]$Platform = "all",

    [Parameter(Mandatory=$false)]
    [string]$Tag = "latest"
)

# Configurazione
$ACR_NAME = "crcentralsdc"
$ACR_LOGIN_SERVER = "$ACR_NAME.azurecr.io"
$RESOURCE_GROUP = "rg-digitalreport-etl-prod-sdc"

# Login ad Azure Container Registry
Write-Host "🔐 Login ad Azure Container Registry..." -ForegroundColor Cyan
az acr login --name $ACR_NAME

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Login fallito!" -ForegroundColor Red
    exit 1
}

function Build-And-Deploy {
    param(
        [string]$PlatformName,
        [string]$JobName
    )

    Write-Host "`n================================================" -ForegroundColor Yellow
    Write-Host "📦 Building $PlatformName..." -ForegroundColor Green
    Write-Host "================================================`n" -ForegroundColor Yellow

    $imageName = "social-$PlatformName"
    $fullImageName = "$ACR_LOGIN_SERVER/${imageName}:$Tag"

    # Build
    Write-Host "🔨 Building Docker image..." -ForegroundColor Cyan
    docker build -f "social/platforms/$PlatformName/Dockerfile" -t $fullImageName .

    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Build fallito per $PlatformName!" -ForegroundColor Red
        return $false
    }

    # Push
    Write-Host "⬆️  Pushing image to ACR..." -ForegroundColor Cyan
    docker push $fullImageName

    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Push fallito per $PlatformName!" -ForegroundColor Red
        return $false
    }

    # Update Azure Container App Job
    Write-Host "🚀 Updating Azure Container App Job..." -ForegroundColor Cyan
    az containerapp job update `
        --name $JobName `
        --resource-group $RESOURCE_GROUP `
        --image $fullImageName

    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Update fallito per $PlatformName!" -ForegroundColor Red
        return $false
    }

    Write-Host "✅ $PlatformName deployed successfully!" -ForegroundColor Green
    return $true
}

# Piattaforme da deployare
$platforms = @()

if ($Platform -eq "all") {
    $platforms = @(
        @{Name="google"; Job="caj-social-google-prod-sdc"},
        @{Name="linkedin"; Job="caj-social-linkedin-prod-sdc"},
        @{Name="facebook"; Job="caj-social-facebook-prod-sdc"},
        @{Name="microsoft"; Job="caj-social-microsoft-prod-sdc"}
    )
} else {
    $jobName = "caj-social-$Platform-prod-sdc"
    $platforms = @(@{Name=$Platform; Job=$jobName})
}

# Deploy
$successCount = 0
$failCount = 0

foreach ($p in $platforms) {
    if (Build-And-Deploy -PlatformName $p.Name -JobName $p.Job) {
        $successCount++
    } else {
        $failCount++
    }
}

# Summary
Write-Host "`n================================================" -ForegroundColor Yellow
Write-Host "📊 DEPLOYMENT SUMMARY" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Yellow
Write-Host "✅ Successful: $successCount" -ForegroundColor Green
Write-Host "❌ Failed: $failCount" -ForegroundColor Red
Write-Host "================================================`n" -ForegroundColor Yellow

if ($failCount -gt 0) {
    exit 1
}

exit 0
