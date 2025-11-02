# Stops Hadoop and Spark containers
param()

$compose = "docker/hadoop-spark/docker-compose.yml"
if (-not (Test-Path $compose)) { throw "Compose file not found: $compose" }

docker compose -f $compose down -v

