# Starts Hadoop (HDFS+YARN) and Spark (master+worker) via Docker Compose
param()

$compose = "docker/hadoop-spark/docker-compose.yml"
if (-not (Test-Path $compose)) { throw "Compose file not found: $compose" }

docker compose -f $compose up -d

Write-Host "Waiting 10s for Hadoop services to settle..."
Start-Sleep -Seconds 10

# Format HDFS if needed and create base dirs
docker exec hadoop-namenode bash -c "/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /input /output && /opt/hadoop-3.2.1/bin/hdfs dfs -ls / || true"
Write-Host "Hadoop and Spark should be up. UIs: HDFS http://localhost:9870, YARN http://localhost:8088 (if exposed), Spark http://localhost:8090"
