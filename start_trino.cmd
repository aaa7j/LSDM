@echo off
setlocal
set "DIR=%~dp0"
cd /d "%DIR%"

REM Prefer Docker Compose V2 (docker compose). Fallback to docker-compose.
docker compose version >NUL 2>&1
if %ERRORLEVEL%==0 (
  echo Starting Trino with Docker Compose v2...
  docker compose up -d
) else (
  echo Starting Trino with docker-compose...
  docker-compose up -d
)

if %ERRORLEVEL% NEQ 0 (
  echo Failed to start Trino. Ensure Docker Desktop is running.
  exit /b 1
)

echo Trino is starting. Open http://localhost:8080 in your browser.
endlocal

