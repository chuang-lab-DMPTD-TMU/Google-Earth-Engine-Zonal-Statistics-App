@echo off
cd /d "%~dp0"

echo.
echo  Stopping GEE Web App (React + FastAPI)...
echo.

docker compose --profile prod down
if errorlevel 1 (
    echo  Some containers may still be running.
    echo  Run: docker compose --profile prod ps
) else (
    echo  All services stopped.
)

echo.
pause
