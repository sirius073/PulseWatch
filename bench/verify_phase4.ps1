# Verify Phase 4
$ErrorActionPreference = "Stop"

$server = Start-Process -FilePath "bin\pulsewatch-server.exe" -PassThru
Start-Sleep -Seconds 2

$agent = Start-Process -FilePath "bin\agent.exe" -PassThru
Start-Sleep -Seconds 8

# Query endpoints
$hosts = Invoke-RestMethod -Uri "http://localhost:8080/api/v1/hosts"
$prom = (Invoke-WebRequest -Uri "http://localhost:8080/metrics").Content

$hostname = [System.Net.Dns]::GetHostName()
$mem = Invoke-RestMethod -Uri "http://localhost:8080/api/v1/metrics/latest?host=$hostname&metric=system.memory.used_percent"
$proc = Invoke-RestMethod -Uri "http://localhost:8080/api/v1/metrics/latest?host=$hostname&metric=process.cpu.utilization"

Stop-Process -Id $agent.Id -Force
Stop-Process -Id $server.Id -Force

Write-Host "=== HOST INVENTORY ===" -ForegroundColor Cyan
$hosts | ConvertTo-Json

Write-Host "=== LATEST MEMORY METRIC ===" -ForegroundColor Cyan
$mem | ConvertTo-Json

Write-Host "=== TOP PROCESS METRIC ===" -ForegroundColor Cyan
$proc | ConvertTo-Json

Write-Host "=== PROMETHEUS /metrics (SAMPLE) ===" -ForegroundColor Cyan
$prom -split "`n" | Where-Object { $_ -match "^pulsewatch_server" } | Select-Object -First 6
