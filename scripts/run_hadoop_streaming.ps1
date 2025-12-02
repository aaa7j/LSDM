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

# --------------------- Helpers ---------------------
function Resolve-HostPython {
  if (Test-Path ".venv\Scripts\python.exe") { return (Resolve-Path ".venv\Scripts\python.exe").Path }
  if (Get-Command python -ErrorAction SilentlyContinue) { return 'python' }
  if (Get-Command python3 -ErrorAction SilentlyContinue) { return 'python3' }
  if (Get-Command py -ErrorAction SilentlyContinue) { return 'py -3' }
  return $null
}

function Ensure-ContainerPython {
  param([string[]]$Containers)
  foreach ($c in $Containers) {
    $check = & docker exec $c bash -c 'command -v python3 >/dev/null 2>&1 && echo ok || echo missing' 2>$null
    if ($check -ne 'ok') {
      Write-Host "Installing python3 in $c ..."
      & docker exec $c bash -c "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y python3 >/dev/null 2>&1 || true" | Out-Null
    }
  }
}

# Helper: resolve streaming jar path inside container
function Get-StreamingJar {
  $lsCmd = 'ls /opt/hadoop*/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -n1'
  try {
    $out = & docker exec hadoop-resourcemanager bash -c $lsCmd 2>&1
  } catch {
    $out = $_.Exception.Message
  }
  return $out.Trim()
}

function Wait-HdfsReady {
  Write-Host 'Waiting for HDFS to leave safemode ...'
  # This blocks until safemode is OFF
  & docker exec hadoop-namenode bash -c "/opt/hadoop-3.2.1/bin/hdfs dfsadmin -safemode wait" | Out-Null
}

$PY = Resolve-HostPython
if (-not $PY) { Write-Warning "Python non trovato sull'host; userò python3 nei container." }

# Make sure python3 exists where mappers/reducers will run (NodeManager)
Ensure-ContainerPython -Containers @('hadoop-resourcemanager','hadoop-nodemanager')

# 1) Prepara TSV di input
if ($PY) {
  & $PY bigdata/hadoop/prep/flatten_games_to_tsv.py --games data/json/games.jsonl --out warehouse/bigdata/team_game_points_tsv
  & $PY bigdata/hadoop/prep/build_teams_dim.py --inp warehouse/bigdata/team_game_points_tsv --out warehouse/bigdata/teams_dim_tsv --team-details data/json/team_details.jsonl
  & $PY bigdata/hadoop/prep/build_q4_multifactor.py --play data/_player_play_by_play_staging.csv --per-game "data/Player Per Game.csv" --advanced data/Advanced.csv --team-totals "data/Team Totals.csv" --out warehouse/bigdata/q4_multifactor.tsv
} else {
  Ensure-ContainerPython -Containers @('hadoop-resourcemanager')
  docker exec hadoop-resourcemanager bash -c "python3 /workspace/bigdata/hadoop/prep/flatten_games_to_tsv.py --games /workspace/data/json/games.jsonl --out /workspace/warehouse/bigdata/team_game_points_tsv"
  docker exec hadoop-resourcemanager bash -c "python3 /workspace/bigdata/hadoop/prep/build_teams_dim.py --inp /workspace/warehouse/bigdata/team_game_points_tsv --out /workspace/warehouse/bigdata/teams_dim_tsv --team-details /workspace/data/json/team_details.jsonl"
  docker exec hadoop-resourcemanager bash -c "python3 /workspace/bigdata/hadoop/prep/build_q4_multifactor.py --play /workspace/data/_player_play_by_play_staging.csv --per-game '/workspace/data/Player Per Game.csv' --advanced /workspace/data/Advanced.csv --team-totals '/workspace/data/Team Totals.csv' --out /workspace/warehouse/bigdata/q4_multifactor.tsv"
}

# Ensure HDFS is writable (not in safemode) before using it
Wait-HdfsReady

# 2) Carica in HDFS (pulizia + singolo file TSV per evitare duplicazioni)
docker exec hadoop-namenode bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -rm -r -f /input/team_game_points_tsv; /opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /input/team_game_points_tsv; /opt/hadoop-3.2.1/bin/hdfs dfs -put -f /workspace/warehouse/bigdata/team_game_points_tsv/part-00000.tsv /input/team_game_points_tsv/"
docker exec hadoop-namenode bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -rm -r -f /input/teams_dim_tsv; /opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /input/teams_dim_tsv; /opt/hadoop-3.2.1/bin/hdfs dfs -put -f /workspace/warehouse/bigdata/teams_dim_tsv/part-00000.tsv /input/teams_dim_tsv/"
# Q4 input: enriched TSV (play-by-play + per-game + advanced + team totals)
docker exec hadoop-namenode bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -rm -f /input/q4_multifactor.tsv; /opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /input; /opt/hadoop-3.2.1/bin/hdfs dfs -put -f /workspace/warehouse/bigdata/q4_multifactor.tsv /input/q4_multifactor.tsv"

$jarPath = Get-StreamingJar
if (-not $jarPath) { throw "Cannot find hadoop-streaming jar in container" }
$jarPath = $jarPath -replace '[^\x20-\x7E]', ''
if ($jarPath -match '/opt/[^\s\"]*/share/hadoop/tools/lib/hadoop-streaming-[^\s\"]*') { $jarPath = $Matches[0] }
Write-Host "DEBUG: resolved jarPath => '" + $jarPath + "' (length=" + $jarPath.Length + ")"
if (-not ($jarPath -match '/opt/')) {
  $jarPath = '/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar'
  Write-Host "WARNING: using fallback jarPath => $jarPath"
}

New-Item -ItemType Directory -Force -Path outputs/hadoop, results | Out-Null

if ($Only -eq 'all' -or $Only -eq 'q1' -or $Only -eq 'q2') {
  # 3) Q1 Aggregation (needed by Q2)
  if ($Reuse -and (Test-Path 'outputs/hadoop/q1.tsv')) {
    Write-Host "[hadoop] Reuse ON: skipping q1 MR (outputs/hadoop/q1.tsv present)" -ForegroundColor DarkYellow
  } else {
    docker exec hadoop-resourcemanager bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -rm -r -f /output/q1; /opt/hadoop-3.2.1/bin/hadoop jar $jarPath -D mapreduce.job.reduces=1 -files /workspace/bigdata/hadoop/streaming/mapper_q1_agg.py,/workspace/bigdata/hadoop/streaming/reducer_q1_agg.py -mapper 'python3 mapper_q1_agg.py' -reducer 'python3 reducer_q1_agg.py' -input /input/team_game_points_tsv -output /output/q1"
    docker exec hadoop-namenode bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -cat /output/q1/part-* > /tmp/q1.tsv"
    docker cp hadoop-namenode:/tmp/q1.tsv outputs/hadoop/q1.tsv
  }
}

if ($Only -eq 'all' -or $Only -eq 'q2') {
  # 4) Q2: High-scoring share (>=120) per team/season directly from team_game_points_tsv
  if ($Reuse -and (Test-Path 'outputs/hadoop/q2.tsv') -and ((Get-Item 'outputs/hadoop/q2.tsv').Length -gt 0)) {
    Write-Host "[hadoop] Reuse ON: skipping q2 MR (outputs/hadoop/q2.tsv present)" -ForegroundColor DarkYellow
  } else {
    docker exec hadoop-resourcemanager bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -rm -r -f /output/q2; /opt/hadoop-3.2.1/bin/hadoop jar $jarPath -D mapreduce.job.reduces=1 -files /workspace/bigdata/hadoop/streaming/mapper_q2_join.py,/workspace/bigdata/hadoop/streaming/reducer_q2_join.py -mapper 'python3 mapper_q2_join.py' -reducer 'python3 reducer_q2_join.py' -input /input/team_game_points_tsv -output /output/q2"
    docker exec hadoop-namenode bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -cat /output/q2/part-* > /tmp/q2.tsv"
    try { docker cp hadoop-namenode:/tmp/q2.tsv outputs/hadoop/q2.tsv } catch { }
  }
}

if ($Only -eq 'all' -or $Only -eq 'q3') {
  # 5) Q3 TopN
  if ($Reuse -and (Test-Path 'outputs/hadoop/q3.tsv')) {
    Write-Host "[hadoop] Reuse ON: skipping q3 MR (outputs/hadoop/q3.tsv present)" -ForegroundColor DarkYellow
  } else {
    docker exec hadoop-resourcemanager bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -rm -r -f /output/q3; export TOPN=$TopN; /opt/hadoop-3.2.1/bin/hadoop jar $jarPath -D mapreduce.job.reduces=1 -files /workspace/bigdata/hadoop/streaming/mapper_q3_topn.py,/workspace/bigdata/hadoop/streaming/reducer_q3_topn.py -mapper 'python3 mapper_q3_topn.py' -reducer 'python3 reducer_q3_topn.py' -input /input/team_game_points_tsv -output /output/q3"
    docker exec hadoop-namenode bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -cat /output/q3/part-* > /tmp/q3.tsv"
    docker cp hadoop-namenode:/tmp/q3.tsv outputs/hadoop/q3.tsv
  }
}

if ($Only -eq 'all' -or $Only -eq 'q4') {
  # Q4: play-by-play usage, directly from Player Play By Play.csv in HDFS
  if ($Reuse -and (Test-Path 'outputs/hadoop/q4.tsv') -and ((Get-Item 'outputs/hadoop/q4.tsv').Length -gt 0)) {
    Write-Host "[hadoop] Reuse ON: skipping q4 MR (outputs/hadoop/q4.tsv present)" -ForegroundColor DarkYellow
  } else {
    docker exec hadoop-resourcemanager bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -rm -r -f /output/q4; /opt/hadoop-3.2.1/bin/hadoop jar $jarPath -D mapreduce.job.reduces=1 -files /workspace/bigdata/hadoop/streaming/mapper_q4_playbyplay_usage.py,/workspace/bigdata/hadoop/streaming/reducer_q4_playbyplay_usage.py -mapper 'python3 mapper_q4_playbyplay_usage.py' -reducer 'python3 reducer_q4_playbyplay_usage.py' -input /input/q4_multifactor.tsv -output /output/q4"
    docker exec hadoop-namenode bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -cat /output/q4/part-* > /tmp/q4.tsv"
    docker cp hadoop-namenode:/tmp/q4.tsv outputs/hadoop/q4.tsv
  }
}

Write-Host "Done. Outputs in outputs/hadoop/*.tsv" -ForegroundColor Green

# 6) Genera JSON per le pagine web (PowerShell)
powershell -ExecutionPolicy Bypass -File scripts/generate_web_results.ps1
