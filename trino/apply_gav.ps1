Param(
  [string]$Server = 'http://localhost:8080',
  [string]$CliJar = 'C:\trino\trino-cli-477-executable.jar',
  [string]$ViewsFile = (Join-Path $PSScriptRoot '..\trino\views\gav.sql'),
  [string]$DemoFile = (Join-Path $PSScriptRoot '..\trino\queries\before_after.sql')
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path $CliJar)) { throw "Trino CLI not found: $CliJar" }
if (-not (Test-Path $ViewsFile)) { throw "Views file not found: $ViewsFile" }

Write-Host "Applying GAV views to $Server ..." -ForegroundColor Cyan
& java -jar $CliJar --server $Server --file $ViewsFile

if (Test-Path $DemoFile) {
  Write-Host "Running demo queries (before/after) ..." -ForegroundColor Cyan
  & java -jar $CliJar --server $Server --file $DemoFile
}

