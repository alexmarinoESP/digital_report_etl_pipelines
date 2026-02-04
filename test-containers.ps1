# Test script for social media containers in TEST MODE
# This script runs all containers with TEST_MODE=true to write to *_TEST tables

param(
    [string]$Platform = "all"  # all, linkedin, google, facebook
)

# Load environment variables from .env file if it exists
if (Test-Path ".env") {
    Get-Content .env | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+?)\s*=\s*(.+?)\s*$') {
            [Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process")
        }
    }
    Write-Host "✓ Loaded environment variables from .env" -ForegroundColor Green
}

# Common environment variables for all containers
$commonEnv = @(
    "TEST_MODE=true",  # ← IMPORTANTE: scrive su tabelle *_TEST
    "LOG_LEVEL=INFO",
    "VERTICA_HOST=$env:VERTICA_HOST",
    "VERTICA_PORT=$env:VERTICA_PORT",
    "VERTICA_DATABASE=$env:VERTICA_DATABASE",
    "VERTICA_USER=$env:VERTICA_USER",
    "VERTICA_PASSWORD=$env:VERTICA_PASSWORD"
)

function Test-Container {
    param(
        [string]$Name,
        [string]$Image,
        [array]$ExtraEnv = @()
    )

    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "Testing $Name Container" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "Image: $Image" -ForegroundColor Yellow
    Write-Host "Mode:  TEST (writes to *_TEST tables)" -ForegroundColor Yellow
    Write-Host ""

    # Combine common and platform-specific env vars
    $allEnv = $commonEnv + $ExtraEnv

    # Build docker run command
    $envArgs = $allEnv | ForEach-Object { "-e", $_ }

    # Run container
    $startTime = Get-Date
    docker run --rm $envArgs $Image
    $exitCode = $LASTEXITCODE
    $duration = (Get-Date) - $startTime

    Write-Host ""
    if ($exitCode -eq 0) {
        Write-Host "✓ $Name completed successfully in $($duration.TotalSeconds)s" -ForegroundColor Green
    } else {
        Write-Host "✗ $Name failed with exit code $exitCode after $($duration.TotalSeconds)s" -ForegroundColor Red
    }

    return $exitCode
}

# LinkedIn
if ($Platform -eq "all" -or $Platform -eq "linkedin") {
    $linkedinEnv = @(
        "LINKEDIN_CLIENT_ID=$env:LINKEDIN_CLIENT_ID",
        "LINKEDIN_CLIENT_SECRET=$env:LINKEDIN_CLIENT_SECRET",
        "LINKEDIN_ACCESS_TOKEN=$env:LINKEDIN_ACCESS_TOKEN"
    )
    $exitLinkedIn = Test-Container -Name "LinkedIn" -Image "social-linkedin:latest" -ExtraEnv $linkedinEnv
}

# Google
if ($Platform -eq "all" -or $Platform -eq "google") {
    $googleEnv = @(
        "GOOGLE_ADS_CONFIG_FILE=/app/social/platforms/google/google-ads.yaml",
        "GOOGLE_MANAGER_CUSTOMER_ID=$env:GOOGLE_MANAGER_CUSTOMER_ID"
    )
    $exitGoogle = Test-Container -Name "Google Ads" -Image "social-google:latest" -ExtraEnv $googleEnv
}

# Facebook
if ($Platform -eq "all" -or $Platform -eq "facebook") {
    $facebookEnv = @(
        "FACEBOOK_ACCESS_TOKEN=$env:FACEBOOK_ACCESS_TOKEN",
        "FACEBOOK_APP_ID=$env:FACEBOOK_APP_ID",
        "FACEBOOK_APP_SECRET=$env:FACEBOOK_APP_SECRET"
    )
    $exitFacebook = Test-Container -Name "Facebook Ads" -Image "social-facebook:latest" -ExtraEnv $facebookEnv
}

# Summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Test Summary" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

if ($Platform -eq "all") {
    Write-Host "LinkedIn:    $(if ($exitLinkedIn -eq 0) { '✓ SUCCESS' } else { '✗ FAILED' })" -ForegroundColor $(if ($exitLinkedIn -eq 0) { 'Green' } else { 'Red' })
    Write-Host "Google Ads:  $(if ($exitGoogle -eq 0) { '✓ SUCCESS' } else { '✗ FAILED' })" -ForegroundColor $(if ($exitGoogle -eq 0) { 'Green' } else { 'Red' })
    Write-Host "Facebook:    $(if ($exitFacebook -eq 0) { '✓ SUCCESS' } else { '✗ FAILED' })" -ForegroundColor $(if ($exitFacebook -eq 0) { 'Green' } else { 'Red' })
} else {
    Write-Host "$Platform test completed"
}

Write-Host ""
Write-Host "NOTE: Data was written to *_TEST tables in Vertica" -ForegroundColor Yellow
