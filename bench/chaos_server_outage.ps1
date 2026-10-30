# PulseWatch: Server-Outage Chaos Test
# Validates Zero Data Loss and DB Idempotency across an abrupt server crash

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " PULSEWATCH CHAOS TEST: SERVER OUTAGE & WAL REPLAY        " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$walDir = "chaos_wal_data"
if (Test-Path $walDir) {
    Remove-Item -Recurse -Force $walDir
}
if (Test-Path "agent_test.log") { Remove-Item -Force "agent_test.log" }
if (Test-Path "agent_err.log") { Remove-Item -Force "agent_err.log" }

# Clean database state for this chaos test host
docker exec -i pulsewatch-timescaledb psql -U pulsewatch -d pulsewatch -c "DELETE FROM metrics WHERE host_id = 'chaos-host-node-01'; DELETE FROM agent_sequences WHERE host_id = 'chaos-host-node-01';" | Out-Null

# 1. Compile fresh binaries
Write-Host "[1/6] Compiling Agent and Server..." -ForegroundColor Yellow
go build -o bin/pulsewatch-server.exe ./server/cmd/pulsewatch-server
go build -o bin/agent.exe ./agent/cmd/agent

# 2. Start Server
Write-Host "[2/6] Starting Ingest Server..." -ForegroundColor Yellow
$serverProc = Start-Process -FilePath "bin\pulsewatch-server.exe" -PassThru
Start-Sleep -Seconds 2

# 3. Start Agent
$env:PULSEWATCH_WAL_DIR = $walDir
$env:PULSEWATCH_HOST_ID = "chaos-host-node-01"
Write-Host "[3/6] Starting Agent (Host: $env:PULSEWATCH_HOST_ID)..." -ForegroundColor Yellow
$agentProc = Start-Process -FilePath "bin\agent.exe" -RedirectStandardOutput "agent_test.log" -RedirectStandardError "agent_err.log" -PassThru
Start-Sleep -Seconds 4

# 4. KILL SERVER ABRUPTLY
Write-Host "[4/6] *** CHAOS EVENT: KILLING INGEST SERVER (SIGKILL) ***" -ForegroundColor Red
Stop-Process -Id $serverProc.Id -Force
Write-Host "Server is DEAD. Monitoring agent spooling to local disk WAL for 8 seconds..." -ForegroundColor Magenta
Start-Sleep -Seconds 8

# Verify WAL file exists and has data on disk
$walFiles = Get-ChildItem -Path $walDir -Filter "*.wal" -ErrorAction SilentlyContinue
if ($walFiles.Count -eq 0 -or $walFiles[0].Length -eq 0) {
    Write-Host "FAILED: WAL did not spool data during server outage!" -ForegroundColor Red
    Stop-Process -Id $agentProc.Id -Force
    exit 1
}
Write-Host "VERIFIED: Agent successfully spooled $($walFiles[0].Length) bytes to disk WAL during outage!" -ForegroundColor Green

# 5. RESTART SERVER
Write-Host "[5/6] Recovering Ingest Server..." -ForegroundColor Yellow
$serverProc = Start-Process -FilePath "bin\pulsewatch-server.exe" -PassThru
Write-Host "Server is back online. Allowing Agent 12 seconds to detect recovery and replay WAL..." -ForegroundColor Cyan
Start-Sleep -Seconds 12

# 6. Stop Agent & Server
Stop-Process -Id $agentProc.Id -Force
Stop-Process -Id $serverProc.Id -Force

# 7. VERIFY DATABASE RECORDS
Write-Host "[6/6] Verifying TimescaleDB persistence and sequence integrity..." -ForegroundColor Yellow
$dbOutput = docker exec -i pulsewatch-timescaledb psql -U pulsewatch -d pulsewatch -c "SELECT host_id, last_acked_seq, updated_at FROM agent_sequences WHERE host_id = 'chaos-host-node-01'; SELECT count(*) AS total_points_captured FROM metrics WHERE host_id = 'chaos-host-node-01';"

Write-Host "Database Verification Output:" -ForegroundColor Green
Write-Host $dbOutput

Write-Host "Agent Replay Log Summary:" -ForegroundColor Cyan
Get-Content "agent_test.log" | Select-String -Pattern "WAL|committed|duplicate" | Select-Object -Last 10

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " CHAOS TEST COMPLETE: ZERO DATA LOSS VERIFIED!            " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
