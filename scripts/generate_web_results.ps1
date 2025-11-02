param(
  [int]$MaxRows = 5000
)

$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force -Path web/data | Out-Null

function Write-JsonRows {
  param(
    [string]$InputPath,
    [string]$OutPath
  )
  try {
    $rows = New-Object System.Collections.ArrayList
    if (Test-Path $InputPath) {
      $read = 0
      foreach ($chunk in (Get-Content -Path $InputPath -Encoding UTF8 -ReadCount 1000)) {
        foreach ($line in $chunk) {
          if ([string]::IsNullOrWhiteSpace($line)) { continue }
          [void]$rows.Add(($line.TrimEnd() -split "`t", -1))
          $read++
          if ($read -ge $MaxRows) { break }
        }
        if ($read -ge $MaxRows) { break }
      }
    }
    $obj = [ordered]@{ rows = $rows; count = $rows.Count }
    $json = $obj | ConvertTo-Json -Depth 4
    $json | Out-File -FilePath $OutPath -Encoding utf8
    Write-Host "Wrote $OutPath ($($rows.Count) rows)"
  } catch {
    Write-Warning "Failed to write ${OutPath}: $($_.Exception.Message)"
  }
}

# Hadoop outputs -> web/data/q{1,2,3}.json
if (Test-Path 'outputs/hadoop/q1.tsv') { Write-Host 'Converting outputs/hadoop/q1.tsv -> web/data/q1.json'; Write-JsonRows -InputPath 'outputs/hadoop/q1.tsv' -OutPath 'web/data/q1.json' }
if (Test-Path 'outputs/hadoop/q2.tsv') { Write-Host 'Converting outputs/hadoop/q2.tsv -> web/data/q2.json'; Write-JsonRows -InputPath 'outputs/hadoop/q2.tsv' -OutPath 'web/data/q2.json' }
if (Test-Path 'outputs/hadoop/q3.tsv') { Write-Host 'Converting outputs/hadoop/q3.tsv -> web/data/q3.json'; Write-JsonRows -InputPath 'outputs/hadoop/q3.tsv' -OutPath 'web/data/q3.json' }

# Spark TSV exports -> web/data/spark_q{1,2,3}.json
function FirstFileIn([string]$dir) {
  if (-not (Test-Path $dir)) { return $null }
  $f = Get-ChildItem -Path $dir -File |
    Where-Object { $_.Name -notmatch '^\.' -and $_.Name -notmatch '\.crc$' -and $_.Name -ne '_SUCCESS' } |
    Sort-Object Length -Descending |
    Select-Object -First 1
  if ($null -ne $f) { return $f.FullName } else { return $null }
}

$sq1 = FirstFileIn 'outputs/spark_tsv/q1'
if ($sq1) { Write-Host "Converting $sq1 -> web/data/spark_q1.json"; Write-JsonRows -InputPath $sq1 -OutPath 'web/data/spark_q1.json' }

$sq2 = FirstFileIn 'outputs/spark_tsv/q2'
if ($sq2) { Write-Host "Converting $sq2 -> web/data/spark_q2.json"; Write-JsonRows -InputPath $sq2 -OutPath 'web/data/spark_q2.json' }

$sq3 = FirstFileIn 'outputs/spark_tsv/q3'
if ($sq3) { Write-Host "Converting $sq3 -> web/data/spark_q3.json"; Write-JsonRows -InputPath $sq3 -OutPath 'web/data/spark_q3.json' }

Write-Host 'Done generating web data JSON.' -ForegroundColor Green
