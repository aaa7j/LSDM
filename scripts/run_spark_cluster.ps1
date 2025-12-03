param(
  [int]$TopN = 3,
  [ValidateSet('all','q1','q2','q3','q4')][string]$Only = 'all',
  $Reuse = $true
)

# Normalize Reuse to boolean (accepts True/False, 1/0, 'true'/'false')
try {
  if ($Reuse -is [string]) {
    $v = $Reuse.Trim().ToLower()
    $Reuse = ($v -eq 'true' -or $v -eq '1')
  } elseif ($Reuse -is [int]) {
    $Reuse = ($Reuse -ne 0)
  } else {
    $Reuse = [bool]$Reuse
  }
} catch { $Reuse = $true }

$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force -Path outputs/spark/q1, outputs/spark/q2, outputs/spark/q3, outputs/spark/q4, outputs/spark_tsv/q1, outputs/spark_tsv/q2, outputs/spark_tsv/q3, outputs/spark_tsv/q4 | Out-Null

# Build enriched TSV for Q4 (used by Spark export and Hadoop for fairness)
function Resolve-HostPython {
  if (Test-Path ".venv\Scripts\python.exe") { return (Resolve-Path ".venv\Scripts\python.exe").Path }
  if (Get-Command python -ErrorAction SilentlyContinue) { return 'python' }
  if (Get-Command python3 -ErrorAction SilentlyContinue) { return 'python3' }
  if (Get-Command py -ErrorAction SilentlyContinue) { return 'py -3' }
  return $null
}

$PY = Resolve-HostPython
if ($PY) {
  & $PY bigdata/hadoop/prep/build_q4_multifactor.py --play data/_player_play_by_play_staging.csv --per-game "data/Player Per Game.csv" --advanced data/Advanced.csv --team-totals "data/Team Totals.csv" --out warehouse/bigdata/q4_multifactor.tsv
}

# Use spark-submit inside the Spark master container, targeting the cluster master
# Allow overriding master (e.g., local[*]) via env var to bypass cluster scheduling issues.
$masterUri = $env:SPARK_MASTER_URI
if (-not $masterUri -or [string]::IsNullOrWhiteSpace($masterUri)) {
  # Default to local[*] to avoid “Initial job has not accepted any resources” if the worker is constrained.
  $masterUri = "local[*]"
}

function SparkSubmit([string]$script, [string[]]$scriptArgs) {
  # Use a param name that doesn't shadow PowerShell's automatic $args variable
  $joined = $scriptArgs -join ' '
  $cmd = "/opt/spark/bin/spark-submit --master $masterUri --conf spark.sql.adaptive.enabled=true --conf spark.driver.memory=4g --conf spark.executor.memory=4g --conf spark.executor.cores=4 /workspace/$script $joined"
  docker exec spark-master bash -lc $cmd
}

function ExportSparkTSV([string]$q, [string]$cols) {
  $parquet = "/workspace/outputs/spark/$q"
  $outdir  = "/workspace/outputs/spark_tsv/$q"
  $sql = @"
DROP VIEW IF EXISTS v_$q;
CREATE TEMPORARY VIEW v_$q USING parquet OPTIONS (path '$parquet');
SET spark.sql.shuffle.partitions=4;
INSERT OVERWRITE LOCAL DIRECTORY '$outdir'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT $cols FROM v_$q;
"@
  # Trim to avoid leading newline and write without UTF-8 BOM (spark-sql can choke on BOM)
  $sql = $sql.Trim()
  # Write SQL to a temp file on host and copy into container to avoid quoting issues
  $tmp = Join-Path ([IO.Path]::GetTempPath()) ("export_${q}_" + [Guid]::NewGuid().ToString('N') + ".sql")
  $enc = New-Object System.Text.UTF8Encoding($false)
  $sw = New-Object System.IO.StreamWriter($tmp, $false, $enc)
  $sw.Write($sql)
  $sw.Close()
  $remote = "/tmp/export_${q}.sql"
  docker cp $tmp spark-master:$remote | Out-Null
  Remove-Item -Force $tmp -ErrorAction SilentlyContinue
  docker exec spark-master bash -lc "rm -rf $outdir && /opt/spark/bin/spark-sql --master spark://spark-master:7077 -f $remote"
}

if ($Only -ne 'q2') {
  # Prepare parquet warehouse using Spark (needed for q1/q3)
  docker exec spark-master bash -lc "rm -rf /workspace/warehouse/bigdata/team_game_points /workspace/warehouse/bigdata/team_game_points_tsv" 2>$null
  if ($Reuse -and (Test-Path 'outputs/spark/q1') -and (Test-Path 'outputs/spark/q2') -and (Test-Path 'outputs/spark/q3')) {
    Write-Host "[spark] Reuse ON: skipping prepare (outputs/spark/q{1,2,3} present)" -ForegroundColor DarkYellow
  } else {
    SparkSubmit "bigdata/spark/prepare_team_points.py" @("--games","/workspace/data/json/games.jsonl","--warehouse","/workspace/warehouse")
  }
} else {
  # When running only Q2, ensure the warehouse exists; otherwise prepare it now.
  $wh = 'warehouse/bigdata/team_game_points'
  $hasWh = (Test-Path $wh) -and (Get-ChildItem -Path $wh -Recurse -Force -ErrorAction SilentlyContinue | Where-Object { -not $_.PSIsContainer } | Select-Object -First 1)
  if (-not $hasWh) {
    Write-Host "[spark] Warehouse not found; preparing datasets for Q2 ..." -ForegroundColor Yellow
    SparkSubmit "bigdata/spark/prepare_team_points.py" @("--games","/workspace/data/json/games.jsonl","--warehouse","/workspace/warehouse")
  }
}

# Q1, Q2, Q3
if ($Only -eq 'all' -or $Only -eq 'q1') {
  if ($Reuse -and (Test-Path 'outputs/spark/q1')) {
    Write-Host "[spark] Reuse ON: skipping q1 compute (outputs/spark/q1 present)" -ForegroundColor DarkYellow
  } else {
    SparkSubmit "bigdata/spark/q1_agg_points.py" @("--warehouse","/workspace/warehouse","--out","/workspace/outputs/spark/q1","--fmt","tsv")
  }
  ExportSparkTSV -q 'q1' -cols 'season, team_id, total_points, avg_points, games'
}

if ($Only -eq 'all' -or $Only -eq 'q2') {
  if ($Reuse -and (Test-Path 'outputs/spark/q2')) {
    Write-Host "[spark] Reuse ON: skipping q2 compute (outputs/spark/q2 present)" -ForegroundColor DarkYellow
  } else {
    SparkSubmit "bigdata/spark/q2_join_teamname.py" @("--warehouse","/workspace/warehouse","--out","/workspace/outputs/spark/q2","--fmt","tsv")
  }
  ExportSparkTSV -q 'q2' -cols 'season, team_id, team_name, high_games, total_games, pct_high'
}

if ($Only -eq 'all' -or $Only -eq 'q3') {
  # For q3, results depend on TopN; safest to recompute unless user opts-in reuse
  if ($Reuse -and (Test-Path 'outputs/spark/q3')) {
    Write-Host "[spark] Reuse ON: reusing existing q3 parquet; exporting TSV view" -ForegroundColor DarkYellow
  } else {
    SparkSubmit "bigdata/spark/q3_topn_games.py" @("--warehouse","/workspace/warehouse","--out","/workspace/outputs/spark/q3","--topn","$TopN","--fmt","tsv")
  }
  ExportSparkTSV -q 'q3' -cols 'team_id, season, game_id, points'
}

if ($Only -eq 'all' -or $Only -eq 'q4') {
  if ($Reuse -and (Test-Path 'outputs/spark/q4')) {
    Write-Host "[spark] Reuse ON: skipping q4 compute (outputs/spark/q4 present)" -ForegroundColor DarkYellow
  } else {
    SparkSubmit "bigdata/spark/q4_playbyplay_usage.py" @("--input","/workspace/warehouse/bigdata/q4_multifactor.tsv","--out","/workspace/outputs/spark/q4")
  }
  ExportSparkTSV -q 'q4' -cols 'season, team_abbr, player_name, pos, g, gs, mp, minutes_per_game, usage_pct, ts_percent, pts_per_36, team_minutes_share, team_pts_share, score, rank'
}

Write-Host "Done. Spark outputs in outputs/spark/q{1,2,3,4}" -ForegroundColor Green
