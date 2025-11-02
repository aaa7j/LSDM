# Install python3 in Hadoop containers (needed for Streaming mappers/reducers)
param()

$containers = @('hadoop-namenode','hadoop-datanode','hadoop-resourcemanager','hadoop-nodemanager','hadoop-historyserver')

foreach ($c in $containers) {
  Write-Host "Installing python3 in $c ..."
  docker exec $c bash -lc "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y python3 >/dev/null 2>&1 || true"
}

Write-Host "Python install attempted on Hadoop containers." -ForegroundColor Green

