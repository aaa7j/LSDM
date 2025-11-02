param(
  [int]$TopN = 3,
  [int]$Port = 8080,
  [bool]$Reuse = $true
)

$ErrorActionPreference = "Stop"

Write-Host "[1/5] Avvio cluster Hadoop+Spark (docker compose)..." -ForegroundColor Cyan
powershell -ExecutionPolicy Bypass -File scripts/hadoop_up.ps1

Write-Host "[2/5] Esecuzione Hadoop Streaming (Q1+Q2+Q3)..." -ForegroundColor Cyan
powershell -ExecutionPolicy Bypass -File scripts/run_hadoop_streaming.ps1 -Only all -TopN $TopN

Write-Host "[3/5] Esecuzione PySpark (Q1+Q2+Q3)..." -ForegroundColor Cyan
powershell -ExecutionPolicy Bypass -File scripts/run_spark_cluster.ps1 -Only all -TopN $TopN -Reuse:$Reuse

Write-Host "[4/5] Generazione JSON per l'UI..." -ForegroundColor Cyan
powershell -ExecutionPolicy Bypass -File scripts/generate_web_results.ps1

Write-Host "[5/5] Avvio UI su http://localhost:$Port ..." -ForegroundColor Cyan
$p = Start-Process -FilePath powershell -ArgumentList @('-ExecutionPolicy','Bypass','-File','scripts/serve_ui.ps1','-Port',"$Port") -PassThru
Start-Sleep -Seconds 1
Start-Process "http://localhost:$Port/hadoop_pyspark_results.html"

Write-Host "Tutto pronto. UI in ascolto su http://localhost:$Port (PID server: $($p.Id))" -ForegroundColor Green
