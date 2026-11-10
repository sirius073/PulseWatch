# PulseWatch Automated Ingestion & Throughput Benchmark
# Tests server capacity, deferred ack latency, and queue buffering.

param (
    [int]$Agents = 30,
    [int]$PointsPerBatch = 20,
    [int]$DurationSec = 15,
    [string]$ServerAddr = "localhost:50051",
    [string]$HttpAddr = "http://localhost:8080"
)

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "   PulseWatch Automated Ingestion Benchmark Suite        " -ForegroundColor Cyan
Write-Host "   Agents: $Agents | Points/Batch: $PointsPerBatch | Duration: ${DurationSec}s" -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

# Check if server is running
try {
    $health = Invoke-RestMethod -Uri "$HttpAddr/health" -TimeoutSec 3
    Write-Host "[OK] Connected to PulseWatch server ($HttpAddr)" -ForegroundColor Green
    Write-Host "     Initial Queue Depth: $($health.queue_depth)"
} catch {
    Write-Host "[ERROR] PulseWatch server is not reachable at $HttpAddr" -ForegroundColor Red
    Write-Host "Please start bin/pulsewatch-server.exe first."
    exit 1
}

# Run loadgen
Write-Host "`n[START] Launching load generator..." -ForegroundColor Yellow
$loadgenCmd = "bin\loadgen.exe -server $ServerAddr -agents $Agents -points-per-batch $PointsPerBatch -duration ${DurationSec}s"
Invoke-Expression $loadgenCmd

# Inspect server Prometheus telemetry after benchmark
Write-Host "`n[TELEMETRY] Server Telemetry Post-Benchmark:" -ForegroundColor Cyan
try {
    $metrics = (Invoke-WebRequest -Uri "$HttpAddr/metrics" -TimeoutSec 3).Content -split "`n"
    $metrics | Where-Object { $_ -match "pulsewatch_server_" -and $_ -notmatch "^#" } | ForEach-Object {
        Write-Host "  $_" -ForegroundColor Gray
    }
} catch {
    Write-Host "  Could not scrape Prometheus /metrics" -ForegroundColor Red
}

Write-Host "`n[COMPLETE] Benchmark finished successfully." -ForegroundColor Green
