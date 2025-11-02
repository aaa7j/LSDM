param(
  [int]$Port = 8080,
  [switch]$OpenBrowser
)

$ErrorActionPreference = "Stop"

Write-Host "Starting built-in UI server (PowerShell) on port $Port ..." -ForegroundColor Green
powershell -ExecutionPolicy Bypass -File scripts/serve_ui.ps1 -Port $Port

if ($OpenBrowser) { Start-Process "http://localhost:$Port/hadoop_pyspark_results.html" }
