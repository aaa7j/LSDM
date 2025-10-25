Param(
  [string]$TrinoHome = "C:\trino\trino-server-477"
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path $TrinoHome)) { throw "Trino home not found: $TrinoHome" }
$etc = Join-Path $TrinoHome 'etc'
foreach ($f in @('config.properties','node.properties','jvm.config')) {
  if (-not (Test-Path (Join-Path $etc $f))) { throw "Missing file: $etc\$f" }
}

$jvmOpts = Get-Content (Join-Path $etc 'jvm.config') | ForEach-Object { $_.Trim() } | Where-Object { $_ -and (-not $_.StartsWith('#')) }
$cp = Join-Path $TrinoHome 'lib\*'

Write-Host "Starting Trino from $TrinoHome ..." -ForegroundColor Cyan
& java @jvmOpts -cp $cp io.trino.server.TrinoServer (Join-Path $etc 'config.properties')

