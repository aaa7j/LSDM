#!/bin/bash
set -e
# switch apt sources to archive and disable Valid-Until check for old releases
sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list || true
sed -i 's|http://security.debian.org/debian-security|http://archive.debian.org/debian|g' /etc/apt/sources.list || true
echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check-valid-until || true
apt-get update -o Acquire::Check-Valid-Until=false || true
DEBIAN_FRONTEND=noninteractive apt-get install -y python3 || true
python3 --version || true
